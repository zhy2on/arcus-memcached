/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-2020 JaM2in Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "config.h"
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include <sched.h>
#include <inttypes.h>

/* Dummy PERSISTENCE_ACTION Macros */
#define PERSISTENCE_ACTION_BEGIN(a, b)
#define PERSISTENCE_ACTION_END(a)

#include "default_engine.h"
#include "item_clog.h"
#include "hash_tree.h"

static struct default_engine *engine=NULL;
static struct engine_config  *config=NULL; // engine config
static EXTENSION_LOGGER_DESCRIPTOR *logger;


/* Cache Lock */
static inline void LOCK_CACHE(void)
{
    pthread_mutex_lock(&engine->cache_lock);
}

static inline void UNLOCK_CACHE(void)
{
    pthread_mutex_unlock(&engine->cache_lock);
}

/*
 * MAP collection manangement
 */
static ENGINE_ERROR_CODE do_map_item_find(const void *key, const uint32_t nkey,
                                          bool do_update, hash_item **item)
{
    *item = NULL;
    hash_item *it = do_item_get(key, nkey, do_update);
    if (it == NULL) {
        return ENGINE_KEY_ENOENT;
    }
    if (IS_MAP_ITEM(it)) {
        *item = it;
        return ENGINE_SUCCESS;
    } else {
        do_item_release(it);
        return ENGINE_EBADTYPE;
    }
}

static int32_t do_map_real_maxcount(int32_t maxcount)
{
    int32_t real_maxcount = maxcount;

    if (maxcount < 0) {
        /* It has the max_map_size that can be increased in the future */
        real_maxcount = -1;
    } else if (maxcount == 0) {
        real_maxcount = DEFAULT_MAP_SIZE;
    } else if (maxcount > config->max_map_size) {
        real_maxcount = config->max_map_size;
    }
    return real_maxcount;
}

static hash_item *do_map_item_alloc(const void *key, const uint32_t nkey,
                                    item_attr *attrp, const void *cookie)
{
    uint32_t nbytes = 2; /* "\r\n" */
    uint32_t real_nbytes = META_OFFSET_IN_ITEM(nkey,nbytes)
                         + sizeof(map_meta_info) - nkey;

    hash_item *it = do_item_alloc(key, nkey, attrp->flags, attrp->exptime,
                                  real_nbytes, cookie);
    if (it != NULL) {
        it->iflag |= ITEM_IFLAG_MAP;
        it->nbytes = nbytes; /* NOT real_nbytes */
        memcpy(item_get_data(it), "\r\n", nbytes);

        /* initialize map meta information */
        map_meta_info *info = (map_meta_info *)item_get_meta(it);
        info->mcnt = do_map_real_maxcount(attrp->maxcount);
        info->ccnt = 0;
        info->ovflact = OVFL_ERROR;
        info->mflags  = 0;
#ifdef ENABLE_STICKY_ITEM
        if (IS_STICKY_EXPTIME(attrp->exptime)) info->mflags |= COLL_META_FLAG_STICKY;
#endif
        if (attrp->readable == 1)              info->mflags |= COLL_META_FLAG_READABLE;
        info->itdist  = (uint16_t)((size_t*)info-(size_t*)it);
        info->stotal  = 0;
        info->root    = NULL;
        assert((hash_item*)COLL_GET_HASH_ITEM(info) == it);
    }
    return it;
}

static void do_map_node_link(map_meta_info *info,
                             map_hash_node *par_node, const int par_hidx,
                             map_hash_node *node)
{
    htree_node_insert(&info->root, par_node, par_hidx, node);

    size_t stotal = slabs_space_size(sizeof(map_hash_node));
    do_coll_space_incr((coll_meta_info *)info, ITEM_TYPE_MAP, stotal);
}

static void do_map_node_unlink(map_meta_info *info,
                               map_hash_node *par_node, const int par_hidx)
{
    htree_node_remove(&info->root, par_node, par_hidx);

    if (info->stotal > 0) {
        size_t stotal = slabs_space_size(sizeof(map_hash_node));
        do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_MAP, stotal);
    }
}

typedef struct {
    map_meta_info *info;
    bool *replaced;
} map_elem_link_ctx;

static void map_elem_on_insert(htree_elem_item *elem, void *ctx)
{
    map_meta_info *info = ((map_elem_link_ctx *)ctx)->info;
    CLOG_MAP_ELEM_INSERT(info, NULL, (map_elem_item *)elem);
    info->ccnt++;
    size_t stotal = slabs_space_size(htree_elem_ntotal(elem));
    do_coll_space_incr((coll_meta_info *)info, ITEM_TYPE_MAP, stotal);
}

static void map_elem_on_replace(htree_elem_item *old_htree, htree_elem_item *new_htree, void *ctx)
{
    map_elem_link_ctx *lctx = (map_elem_link_ctx *)ctx;
    map_meta_info *info = lctx->info;
    if (lctx->replaced) *lctx->replaced = true;
    map_elem_item *old_elem = (map_elem_item *)old_htree;
    map_elem_item *new_elem = (map_elem_item *)new_htree;

    CLOG_MAP_ELEM_INSERT(info, old_elem, new_elem);

    size_t old_stotal = slabs_space_size(htree_elem_ntotal(old_htree));
    size_t new_stotal = slabs_space_size(htree_elem_ntotal(new_htree));
    if (new_stotal != old_stotal) {
        assert(info->stotal > 0);
        if (new_stotal > old_stotal)
            do_coll_space_incr((coll_meta_info *)info, ITEM_TYPE_MAP, new_stotal - old_stotal);
        else
            do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_MAP, old_stotal - new_stotal);
    }

    if (old_elem->refcount == 0)
        htree_elem_free(old_htree);
}

static ENGINE_ERROR_CODE map_on_pre_replace(htree_elem_item *old_htree,
                                         htree_elem_item *new_htree, void *ctx)
{
#ifdef ENABLE_STICKY_ITEM
    map_meta_info *info = ((map_elem_link_ctx *)ctx)->info;
    if (IS_STICKY_COLLFLG(info)) {
        map_elem_item *old_elem = (map_elem_item *)old_htree;
        map_elem_item *new_elem = (map_elem_item *)new_htree;
        if (old_elem->nbytes < new_elem->nbytes && do_item_sticky_overflowed())
            return ENGINE_ENOMEM;
    }
#endif
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE map_on_pre_insert(htree_elem_item *new_htree, void *ctx)
{
    map_meta_info *info = ((map_elem_link_ctx *)ctx)->info;
#ifdef ENABLE_STICKY_ITEM
    if (IS_STICKY_COLLFLG(info)) {
        if (do_item_sticky_overflowed())
            return ENGINE_ENOMEM;
    }
#endif
    assert(info->ovflact == OVFL_ERROR);
    if (info->ccnt >= (info->mcnt > 0 ? info->mcnt : config->max_map_size))
        return ENGINE_EOVERFLOW;
    return ENGINE_SUCCESS;
}

static void map_node_on_insert(void *ctx)
{
    map_meta_info *info = ((map_elem_link_ctx *)ctx)->info;
    size_t stotal = slabs_space_size(sizeof(map_hash_node));
    do_coll_space_incr((coll_meta_info *)info, ITEM_TYPE_MAP, stotal);
}

static ENGINE_ERROR_CODE do_map_elem_link(map_meta_info *info, map_elem_item *elem,
                                          const bool replace_if_exist, bool *replaced,
                                          const void *cookie)
{
    assert(info->root != NULL);
    map_elem_link_ctx lctx = { info, replaced };
    return htree_elem_insert(&info->root, (htree_elem_item *)elem,
                                elem->data, elem->nfield,
                                replace_if_exist,
                                map_on_pre_replace, map_on_pre_insert,
                                map_elem_on_insert, map_elem_on_replace,
                                map_node_on_insert, &lctx, cookie);
}

static void map_elem_on_delete(htree_elem_item *elem,
                               enum elem_delete_cause cause, void *ctx)
{
    map_meta_info *info = (map_meta_info *)ctx;
    info->ccnt--;

    CLOG_MAP_ELEM_DELETE(info, elem, cause);

    if (info->stotal > 0) {
        size_t stotal = slabs_space_size(htree_elem_ntotal(elem));
        do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_MAP, stotal);
    }

    if (elem->refcount == 0) {
        htree_elem_free(elem);
    }
}

static void map_node_on_remove(void *ctx)
{
    map_meta_info *info = (map_meta_info *)ctx;
    if (info->stotal > 0) {
        size_t stotal = slabs_space_size(sizeof(map_hash_node));
        do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_MAP, stotal);
    }
}

static uint32_t do_map_elem_delete_with_field(map_meta_info *info, const int numfields,
                                              const field_t *flist, enum elem_delete_cause cause)
{
    assert(cause == ELEM_DELETE_NORMAL);
    uint32_t delcnt = 0;

    if (info->root != NULL) {
        CLOG_ELEM_DELETE_BEGIN((coll_meta_info*)info, numfields, cause);
        if (numfields == 0) {
            delcnt = htree_traverse_dfs_bycnt(&info->root, info->root, 0, true, NULL,
                                                 map_elem_on_delete, map_node_on_remove, cause, info);
        } else {
            for (int ii = 0; ii < numfields; ii++) {
                if (htree_traverse_dfs_byfield(&info->root, info->root,
                                                  flist[ii].value, flist[ii].length,
                                                  true, NULL, map_elem_on_delete, map_node_on_remove, info)) {
                    delcnt++;
                }
            }
        }
        if (info->root->tot_elem_cnt == 0) {
            do_map_node_unlink(info, NULL, 0);
        }
        CLOG_ELEM_DELETE_END((coll_meta_info*)info, cause);
    }
    return delcnt;
}

static ENGINE_ERROR_CODE do_map_elem_update(map_meta_info *info,
                                            const field_t *field, const char *value,
                                            const uint32_t nbytes, const void *cookie)
{
    htree_prev_info pinfo;
    map_elem_item *elem;

    elem = (map_elem_item *)htree_elem_find(info->root, field->value, field->length, &pinfo);
    if (elem == NULL) {
        return ENGINE_ELEM_ENOENT;
    }

    if (elem->refcount == 0 && elem->nbytes == nbytes) {
        /* old body size == new body size: do in-place update */
        memcpy(elem->data + elem->nfield, value, nbytes);
        CLOG_MAP_ELEM_INSERT(info, elem, elem);
    } else {
#ifdef ENABLE_STICKY_ITEM
        if (IS_STICKY_COLLFLG(info)) {
            if (elem->nbytes < nbytes && do_item_sticky_overflowed())
                return ENGINE_ENOMEM;
        }
#endif
        map_elem_item *new_elem = htree_elem_alloc((uint8_t)elem->nfield, (uint16_t)nbytes, cookie);
        if (new_elem == NULL) {
            return ENGINE_ENOMEM;
        }

        memcpy(new_elem->data, elem->data, elem->nfield);
        memcpy(new_elem->data + elem->nfield, value, nbytes);
        new_elem->hval = elem->hval;

        new_elem->next = elem->next;
        if (pinfo.prev != NULL)
            ((map_elem_item *)pinfo.prev)->next = new_elem;
        else
            pinfo.node->htab[pinfo.hidx] = new_elem;
        new_elem->status = ELEM_STATUS_LINKED;
        elem->status = ELEM_STATUS_UNLINKED;
        map_elem_link_ctx lctx = { info, NULL };
        map_elem_on_replace((htree_elem_item *)elem, (htree_elem_item *)new_elem, &lctx);
    }

    return ENGINE_SUCCESS;
}

static uint32_t do_map_elem_delete(map_meta_info *info, const uint32_t count,
                                   enum elem_delete_cause cause)
{
    assert(cause == ELEM_DELETE_COLL);
    uint32_t fcnt = 0;
    if (info->root != NULL) {
        fcnt = htree_traverse_dfs_bycnt(&info->root, info->root, count, true, NULL,
                                           map_elem_on_delete, map_node_on_remove, cause, info);
        if (info->root->tot_elem_cnt == 0) {
            do_map_node_unlink(info, NULL, 0);
        }
    }
    return fcnt;
}

uint32_t map_elem_delete_with_count(map_meta_info *info, const uint32_t count)
{
    return do_map_elem_delete(info, count, ELEM_DELETE_COLL);
}

static uint32_t do_map_elem_get(map_meta_info *info,
                                const int numfields, const field_t *flist,
                                const bool delete, map_elem_item **elem_array)
{
    assert(info->root);
    uint32_t fcnt = 0;

    if (delete) {
        CLOG_ELEM_DELETE_BEGIN((coll_meta_info*)info, numfields, ELEM_DELETE_NORMAL);
    }
    if (numfields == 0) {
        fcnt = htree_traverse_dfs_bycnt(&info->root, info->root, 0, delete,
                                           elem_array,
                                           (delete ? map_elem_on_delete : NULL),
                                           (delete ? map_node_on_remove : NULL),
                                           ELEM_DELETE_NORMAL, info);
    } else {
        for (int ii = 0; ii < numfields; ii++) {
            if (htree_traverse_dfs_byfield(&info->root, info->root,
                                              flist[ii].value, flist[ii].length,
                                              delete, &elem_array[fcnt],
                                              (delete ? map_elem_on_delete : NULL),
                                              (delete ? map_node_on_remove : NULL), info)) {
                fcnt++;
            }
        }
    }
    if (delete && info->root->tot_elem_cnt == 0) {
        do_map_node_unlink(info, NULL, 0);
    }
    if (delete) {
        CLOG_ELEM_DELETE_END((coll_meta_info*)info, ELEM_DELETE_NORMAL);
    }
    return fcnt;
}

static ENGINE_ERROR_CODE do_map_elem_insert(hash_item *it, map_elem_item *elem,
                                            const bool replace_if_exist, bool *replaced,
                                            const void *cookie)
{
    map_meta_info *info = (map_meta_info *)item_get_meta(it);
    ENGINE_ERROR_CODE ret;

    /* create the root hash node if it does not exist */
    bool new_root_flag = false;
    if (info->root == NULL) { /* empty map */
        map_hash_node *r_node = htree_node_alloc(0, cookie);
        if (r_node == NULL) {
            return ENGINE_ENOMEM;
        }
        do_map_node_link(info, NULL, 0, r_node);
        new_root_flag = true;
    }

    /* insert the element */
    ret = do_map_elem_link(info, elem, replace_if_exist, replaced, cookie);
    if (ret != ENGINE_SUCCESS) {
        if (new_root_flag) {
            do_map_node_unlink(info, NULL, 0);
        }
        return ret;
    }

    return ENGINE_SUCCESS;
}

/*
 * MAP Interface Functions
 */
ENGINE_ERROR_CODE map_struct_create(const char *key, const uint32_t nkey,
                                    item_attr *attrp, const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    PERSISTENCE_ACTION_BEGIN(cookie, UPD_MAP_CREATE);

    LOCK_CACHE();
    it = do_item_get(key, nkey, DONT_UPDATE);
    if (it != NULL) {
        do_item_release(it);
        ret = ENGINE_KEY_EEXISTS;
    } else {
        it = do_map_item_alloc(key, nkey, attrp, cookie);
        if (it == NULL) {
            ret = ENGINE_ENOMEM;
        } else {
            ret = do_item_link(it);
            do_item_release(it);
        }
    }
    UNLOCK_CACHE();

    PERSISTENCE_ACTION_END(ret);
    return ret;
}

map_elem_item *map_elem_alloc(const int nfield, const uint32_t nbytes, const void *cookie)
{
    map_elem_item *elem;
    LOCK_CACHE();
    elem = htree_elem_alloc((uint8_t)nfield, (uint16_t)nbytes, cookie);
    UNLOCK_CACHE();
    return elem;
}

void map_elem_free(map_elem_item *elem)
{
    LOCK_CACHE();
    assert(elem->status == ELEM_STATUS_UNLINKED);
    htree_elem_free(elem);
    UNLOCK_CACHE();
}

void map_elem_release(map_elem_item **elem_array, const int elem_count)
{
    int cnt = 0;
    LOCK_CACHE();
    while (cnt < elem_count) {
        htree_elem_release(elem_array[cnt++]);
        if ((cnt % 100) == 0 && cnt < elem_count) {
            UNLOCK_CACHE();
            LOCK_CACHE();
        }
    }
    UNLOCK_CACHE();
}

ENGINE_ERROR_CODE map_elem_insert(const char *key, const uint32_t nkey,
                                  map_elem_item *elem, const bool replace_if_exist,
                                  item_attr *attrp, bool *replaced, bool *created,
                                  const void *cookie)
{
    hash_item *it = NULL;
    ENGINE_ERROR_CODE ret;
    PERSISTENCE_ACTION_BEGIN(cookie, UPD_MAP_ELEM_INSERT);

    *created = false;
    *replaced = false;

    LOCK_CACHE();
    ret = do_map_item_find(key, nkey, DONT_UPDATE, &it);
    if (ret == ENGINE_KEY_ENOENT && attrp != NULL) {
        it = do_map_item_alloc(key, nkey, attrp, cookie);
        if (it == NULL) {
            ret = ENGINE_ENOMEM;
        } else {
            ret = do_item_link(it);
            if (ret == ENGINE_SUCCESS) {
                *created = true;
            }
        }
    }
    if (ret == ENGINE_SUCCESS) {
        ret = do_map_elem_insert(it, elem, replace_if_exist, replaced, cookie);
        if (ret != ENGINE_SUCCESS && *created) {
            do_item_unlink(it, ITEM_UNLINK_NORMAL);
        }
    }
    if (it) {
        do_item_release(it);
    }
    UNLOCK_CACHE();

    PERSISTENCE_ACTION_END(ret);
    return ret;
}

ENGINE_ERROR_CODE map_elem_update(const char *key, const uint32_t nkey,
                                  const field_t *field, const char *value,
                                  const uint32_t nbytes, const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    PERSISTENCE_ACTION_BEGIN(cookie, UPD_MAP_ELEM_INSERT);

    LOCK_CACHE();
    ret = do_map_item_find(key, nkey, DONT_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) { /* it != NULL */
        map_meta_info *info = (map_meta_info *)item_get_meta(it);
        ret = do_map_elem_update(info, field, value, nbytes, cookie);
        do_item_release(it);
    }
    UNLOCK_CACHE();

    PERSISTENCE_ACTION_END(ret);
    return ret;
}

ENGINE_ERROR_CODE map_elem_delete(const char *key, const uint32_t nkey,
                                  const int numfields, const field_t *flist,
                                  const bool drop_if_empty,
                                  uint32_t *del_count, bool *dropped,
                                  const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    PERSISTENCE_ACTION_BEGIN(cookie, (drop_if_empty ? UPD_MAP_ELEM_DELETE_DROP
                                                    : UPD_MAP_ELEM_DELETE));

    *dropped = false;

    LOCK_CACHE();
    ret = do_map_item_find(key, nkey, DONT_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) { /* it != NULL */
        map_meta_info *info = (map_meta_info *)item_get_meta(it);
        *del_count = do_map_elem_delete_with_field(info, numfields, flist, ELEM_DELETE_NORMAL);
        if (*del_count > 0) {
            if (info->ccnt == 0 && drop_if_empty) {
                assert(info->root == NULL);
                do_item_unlink(it, ITEM_UNLINK_NORMAL);
                *dropped = true;
            }
        } else {
            ret = ENGINE_ELEM_ENOENT;
        }
        do_item_release(it);
    }
    UNLOCK_CACHE();

    PERSISTENCE_ACTION_END(ret);
    return ret;
}

ENGINE_ERROR_CODE map_elem_get(const char *key, const uint32_t nkey,
                               const int numfields, const field_t *flist,
                               const bool delete, const bool drop_if_empty,
                               struct elems_result *eresult,
                               const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    if (delete) {
        PERSISTENCE_ACTION_BEGIN(cookie, (drop_if_empty ? UPD_MAP_ELEM_DELETE_DROP
                                                        : UPD_MAP_ELEM_DELETE));
    }

    LOCK_CACHE();
    ret = do_map_item_find(key, nkey, DO_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) {
        map_meta_info *info = (map_meta_info *)item_get_meta(it);
        do {
            if ((info->mflags & COLL_META_FLAG_READABLE) == 0) {
                ret = ENGINE_UNREADABLE; break;
            }
            if (info->ccnt <= 0) {
                ret = ENGINE_ELEM_ENOENT; break;
            }
            if (numfields == 0 || info->ccnt < numfields) {
                eresult->elem_array = (eitem **)malloc(info->ccnt * sizeof(eitem*));
            } else {
                eresult->elem_array = (eitem **)malloc(numfields * sizeof(eitem*));
            }
            if (eresult->elem_array == NULL) {
                ret = ENGINE_ENOMEM; break;
            }
            eresult->elem_count = do_map_elem_get(info, numfields, flist, delete,
                                                  (map_elem_item **)eresult->elem_array);
            if (eresult->elem_count > 0) {
                if (info->ccnt == 0 && drop_if_empty) {
                    assert(delete == true);
                    do_item_unlink(it, ITEM_UNLINK_NORMAL);
                    eresult->dropped = true;
                } else {
                    eresult->dropped = false;
                }
                eresult->flags = it->flags;
            } else {
                ret = ENGINE_ELEM_ENOENT;
                free(eresult->elem_array);
                eresult->elem_array = NULL;
            }
        } while (0);
        do_item_release(it);
    }
    UNLOCK_CACHE();

    if (delete) {
        PERSISTENCE_ACTION_END(ret);
    }
    return ret;
}

/* See do_map_elem_traverse_dfs and do_map_elem_link. do_map_elem_traverse_dfs
 * can visit all elements, but only supports get and delete operations.
 * Do something similar and visit all elements.
 */
void map_elem_get_all(map_meta_info *info, elems_result_t *eresult)
{
    assert(eresult->elem_arrsz >= info->ccnt && eresult->elem_count == 0);
    map_hash_node *node;
    map_elem_item *elem;
    int cur_depth, i;
    bool push;

    /* Temporay stack we use to do dfs. Static is ugly but is okay...
     * This function runs with the cache lock acquired.
     */
    static int stack_max = 0;
    static struct _map_hash_posi {
        map_hash_node *node;
        int idx;
    } *stack = NULL;

    node = info->root;
    cur_depth = 0;
    push = true;
    while (node != NULL) {
        if (push) {
            push = false;
            if (stack_max <= cur_depth) {
                struct _map_hash_posi *tmp;
                stack_max += 16;
                tmp = realloc(stack, sizeof(*stack) * stack_max);
                assert(tmp != NULL);
                stack = tmp;
            }
            stack[cur_depth].node = node;
            stack[cur_depth].idx = 0;
        }

        /* Scan the current node */
        for (i = stack[cur_depth].idx; i < HTREE_HASHTAB_SIZE; i++) {
            if (node->hcnt[i] >= 0) {
                /* Hash chain.  Insert all elements on the chain into the
                 * to-be-copied list.
                 */
                for (elem = node->htab[i]; elem != NULL; elem = elem->next) {
                    elem->refcount++;
                    eresult->elem_array[eresult->elem_count++] = elem;
                }
            }
            else if (node->htab[i] != NULL) {
                /* Another hash node.  Go down */
                stack[cur_depth].idx = i+1;
                push = true;
                node = node->htab[i];
                cur_depth++;
                break;
            }
        }

        /* Scannned everything in this node.  Go up. */
        if (i >= HTREE_HASHTAB_SIZE) {
            cur_depth--;
            if (cur_depth < 0)
                node = NULL; /* done */
            else
                node = stack[cur_depth].node;
        }
    }
    assert(eresult->elem_count == info->ccnt);
}

ENGINE_ERROR_CODE map_coll_getattr(hash_item *it, item_attr *attrp,
                                   ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_cnt)
{
    map_meta_info *info = (map_meta_info *)item_get_meta(it);

    /* check attribute validation */
    for (int i = 0; i < attr_cnt; i++) {
        if (attr_ids[i] == ATTR_MAXBKEYRANGE || attr_ids[i] == ATTR_TRIMMED) {
            return ENGINE_EBADATTR;
        }
    }

    /* get collection attributes */
    attrp->count = info->ccnt;
    attrp->maxcount = (info->mcnt > 0) ? info->mcnt : (int32_t)config->max_map_size;
    attrp->ovflaction = info->ovflact;
    attrp->readable = ((info->mflags & COLL_META_FLAG_READABLE) != 0) ? 1 : 0;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE map_coll_setattr(hash_item *it, item_attr *attrp,
                                   ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_cnt)
{
    map_meta_info *info = (map_meta_info *)item_get_meta(it);

    /* check the validity of given attributs */
    for (int i = 0; i < attr_cnt; i++) {
        if (attr_ids[i] == ATTR_MAXCOUNT) {
            attrp->maxcount = do_map_real_maxcount(attrp->maxcount);
            if (attrp->maxcount > 0 && attrp->maxcount < info->ccnt) {
                return ENGINE_EBADVALUE;
            }
        } else if (attr_ids[i] == ATTR_OVFLACTION) {
            if (attrp->ovflaction != OVFL_ERROR) {
                return ENGINE_EBADVALUE;
            }
        } else if (attr_ids[i] == ATTR_READABLE) {
            if (attrp->readable != 1) {
                return ENGINE_EBADVALUE;
            }
        }
    }

    /* set the attributes */
    for (int i = 0; i < attr_cnt; i++) {
        if (attr_ids[i] == ATTR_MAXCOUNT) {
            info->mcnt = attrp->maxcount;
        } else if (attr_ids[i] == ATTR_OVFLACTION) {
            info->ovflact = attrp->ovflaction;
        } else if (attr_ids[i] == ATTR_READABLE) {
            info->mflags |= COLL_META_FLAG_READABLE;
        }
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE map_apply_item_link(void *engine, const char *key, const uint32_t nkey,
                                      item_attr *attrp)
{
    hash_item *old_it;
    hash_item *new_it;
    ENGINE_ERROR_CODE ret;

    logger->log(ITEM_APPLY_LOG_LEVEL, NULL, "map_apply_item_link. key=%.*s nkey=%u\n",
                PRINT_NKEY(nkey), key, nkey);

    LOCK_CACHE();
    old_it = do_item_get(key, nkey, DONT_UPDATE);
    if (old_it) {
        /* Remove the old item first. */
        do_item_unlink(old_it, ITEM_UNLINK_NORMAL);
        do_item_release(old_it);
    }
    new_it = do_map_item_alloc(key, nkey, attrp, NULL); /* cookie is NULL */
    if (new_it) {
        /* Link the new item into the hash table */
        ret = do_item_link(new_it);
        do_item_release(new_it);
    } else {
        ret = ENGINE_ENOMEM;
    }
    UNLOCK_CACHE();

    if (ret == ENGINE_SUCCESS) {
        /* The caller wants to know if the old item has been replaced.
         * This code still indicates success.
         */
        if (old_it != NULL) ret = ENGINE_KEY_EEXISTS;
    } else {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "map_apply_item_link failed. key=%.*s nkey=%u code=%d\n",
                    PRINT_NKEY(nkey), key, nkey, ret);
    }
    return ret;
}

ENGINE_ERROR_CODE map_apply_elem_insert(void *engine, hash_item *it,
                                        const char *field, const uint32_t nfield,
                                        const uint32_t nbytes)
{
    const char *key = item_get_key(it);
    map_elem_item *elem;
    bool replaced;
    ENGINE_ERROR_CODE ret;

    logger->log(ITEM_APPLY_LOG_LEVEL, NULL,
                "map_apply_elem_insert. key=%.*s nkey=%u field=%.*s nfield=%u\n",
                PRINT_NKEY(it->nkey), key, it->nkey, nfield, field, nfield);

    LOCK_CACHE();
    do {
        if (!item_is_valid(it)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "map_apply_elem_insert failed."
                        " invalid item.\n");
            ret = ENGINE_KEY_ENOENT; break;
        }

        elem = htree_elem_alloc((uint8_t)nfield, (uint16_t)nbytes, NULL);
        if (elem == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "map_apply_elem_insert failed."
                        " element alloc failed. nfield=%d nbytes=%d\n", nfield, nbytes);
            ret = ENGINE_ENOMEM; break;
        }
        memcpy(elem->data, field, nfield + nbytes);

        ret = do_map_elem_insert(it, elem, true /* replace_if_exist */, &replaced, NULL);
        if (ret != ENGINE_SUCCESS) {
            htree_elem_free(elem);
            logger->log(EXTENSION_LOG_WARNING, NULL, "map_apply_elem_insert failed."
                        " key=%.*s nkey=%u field=%.*s nfield=%u code=%d\n",
                        PRINT_NKEY(it->nkey), key, it->nkey, nfield, field, nfield, ret);
        }
    } while(0);

    if (ret != ENGINE_SUCCESS) { /* Remove inconsistent has_item */
        do_item_unlink(it, ITEM_UNLINK_NORMAL);
    }
    UNLOCK_CACHE();

    return ret;
}

ENGINE_ERROR_CODE map_apply_elem_delete(void *engine, hash_item *it,
                                        const char *field, const uint32_t nfield,
                                        const bool drop_if_empty)
{
    const char *key = item_get_key(it);
    map_meta_info *info;
    field_t flist;
    uint32_t ndeleted;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    flist.value = (char*)field;
    flist.length = nfield;

    logger->log(ITEM_APPLY_LOG_LEVEL, NULL,
                "map_apply_elem_delete. key=%.*s nkey=%u field=%.*s nfield=%u\n",
                PRINT_NKEY(it->nkey), key, it->nkey, nfield, field, nfield);

    LOCK_CACHE();
    do {
        if (!item_is_valid(it)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "map_apply_elem_delete failed."
                        " invalid item.\n");
            ret = ENGINE_KEY_ENOENT; break;
        }

        info = (map_meta_info *)item_get_meta(it);
        if (info->ccnt == 0) {
            logger->log(EXTENSION_LOG_INFO, NULL, "map_apply_elem_delete failed."
                        " no element.\n");
            ret = ENGINE_ELEM_ENOENT; break;
        }

        ndeleted = do_map_elem_delete_with_field(info, 1, &flist, ELEM_DELETE_NORMAL);
        if (ndeleted == 0) {
            logger->log(EXTENSION_LOG_INFO, NULL, "map_apply_elem_delete failed."
                        " no element deleted. key=%.*s nkey=%u field=%.*s nfield=%u\n",
                        PRINT_NKEY(it->nkey), key, it->nkey, nfield, field, nfield);
            ret = ENGINE_ELEM_ENOENT; break;
        }
    } while(0);

    if (ret == ENGINE_SUCCESS || ret == ENGINE_ELEM_ENOENT) {
        if (drop_if_empty && info->ccnt == 0) {
            do_item_unlink(it, ITEM_UNLINK_NORMAL);
        }
    } else {
        /* Remove inconsistent hash_item */
        do_item_unlink(it, ITEM_UNLINK_NORMAL);
    }
    UNLOCK_CACHE();

    return ret;
}

/*
 * External Functions
 */
ENGINE_ERROR_CODE item_map_coll_init(void *engine_ptr)
{
    /* initialize global variables */
    engine = engine_ptr;
    config = &engine->config;
    logger = engine->server.log->get_logger();

    logger->log(EXTENSION_LOG_INFO, NULL, "ITEM map module initialized.\n");
    return ENGINE_SUCCESS;
}

void item_map_coll_final(void *engine_ptr)
{
    logger->log(EXTENSION_LOG_INFO, NULL, "ITEM map module destroyed.\n");
}
