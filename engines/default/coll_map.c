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
#include <stddef.h>
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

static void do_map_elem_unlink_clog(void *meta, htree_elem_item *elem)
{
    CLOG_MAP_ELEM_DELETE((map_meta_info *)meta, elem, ELEM_DELETE_NORMAL);
}

static ENGINE_ERROR_CODE do_map_elem_delete_with_field(map_meta_info *info,
                                                       const field_t *field)
{
    if (info->root == NULL)
        return ENGINE_ELEM_ENOENT;

    ssize_t space_delta = 0;
    bool found = htree_elem_traverse_dfs_bykey((htree_node **)&info->root, info->root,
                                               field->length,
                                               (const unsigned char *)field->value,
                                               true, NULL,
                                               do_map_elem_unlink_clog, info, &space_delta);
    if (!found)
        return ENGINE_ELEM_ENOENT;

    info->ccnt--;
    if (space_delta != 0)
        do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_MAP, (size_t)-space_delta);
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE do_map_elem_update(map_meta_info *info,
                                            const field_t *field, const char *value,
                                            const uint32_t nbytes, const void *cookie)
{
    uint16_t new_nbytes = (uint16_t)(field->length + nbytes);
    map_elem_item *new_elem = htree_elem_alloc(field->length, new_nbytes, cookie);
    if (new_elem == NULL)
        return ENGINE_ENOMEM;

    memcpy(new_elem->data, field->value, field->length);
    memcpy(new_elem->data + field->length, value, nbytes);

    map_elem_item *old_elem = NULL;
    ssize_t space_delta = 0;
    ENGINE_ERROR_CODE ret = htree_elem_update((htree_node **)&info->root,
                                              new_elem,
                                              IS_STICKY_COLLFLG(info),
                                              &old_elem, &space_delta);
    if (ret != ENGINE_SUCCESS) {
        htree_elem_free(new_elem);
        return ret;
    }

    if (old_elem->status == ELEM_STATUS_LINKED) {
        /* in-place path: data overwritten in place, new_elem not linked */
        CLOG_MAP_ELEM_INSERT(info, old_elem, old_elem);
        htree_elem_free(new_elem);
    } else {
        /* chain-replace path: old elem unlinked, new_elem now linked */
        CLOG_MAP_ELEM_INSERT(info, old_elem, new_elem);
        if (old_elem->refcount == 0)
            htree_elem_free(old_elem);
        if (space_delta > 0)
            do_coll_space_incr((coll_meta_info *)info, ITEM_TYPE_MAP, (size_t)space_delta);
        else if (space_delta < 0)
            do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_MAP, (size_t)-space_delta);
    }

    return ENGINE_SUCCESS;
}

static uint32_t do_map_elem_get(map_meta_info *info,
                                const int numfields, const field_t *flist,
                                const bool delete, map_elem_item **elem_array)
{
    assert(info->root);
    uint32_t fcnt = 0;
    ssize_t space_delta = 0;

    if (delete) {
        CLOG_ELEM_DELETE_BEGIN((coll_meta_info*)info, numfields, ELEM_DELETE_NORMAL);
    }
    if (numfields == 0) {
        htree_elem_unlink_func fn = delete ? do_map_elem_unlink_clog : NULL;
        fcnt = htree_elem_traverse_dfs_bycnt((htree_node **)&info->root, info->root,
                                             0, delete, (htree_elem_item **)elem_array,
                                             fn, info,
                                             fn ? &space_delta : NULL);
    } else {
        for (int ii = 0; ii < numfields; ii++) {
            map_elem_item **elem_out = (elem_array != NULL) ? &elem_array[fcnt] : NULL;
            bool found = htree_elem_traverse_dfs_bykey((htree_node **)&info->root, info->root,
                                                       flist[ii].length,
                                                       (const unsigned char *)flist[ii].value,
                                                       delete, (htree_elem_item **)elem_out,
                                                       delete ? do_map_elem_unlink_clog : NULL,
                                                       info,
                                                       delete ? &space_delta : NULL);
            if (found) fcnt++;
        }
    }
    if (delete) {
        info->ccnt -= fcnt;
        if (space_delta != 0)
            do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_MAP, (size_t)-space_delta);
        CLOG_ELEM_DELETE_END((coll_meta_info*)info, ELEM_DELETE_NORMAL);
    }
    return fcnt;
}

static ENGINE_ERROR_CODE do_map_elem_insert(hash_item *it, map_elem_item *elem,
                                            const bool replace_if_exist, bool *replaced,
                                            const void *cookie)
{
    map_meta_info *info = (map_meta_info *)item_get_meta(it);
    int real_mcnt = (int)(info->mcnt > 0 ? info->mcnt : config->max_map_size);
    bool is_sticky = IS_STICKY_COLLFLG(info);
    map_elem_item *old_elem = NULL;
    ssize_t space_delta = 0;
    ENGINE_ERROR_CODE ret;

    ret = htree_elem_insert((htree_node **)&info->root,
                            elem,
                            replace_if_exist,
                            is_sticky, real_mcnt,
                            &old_elem,
                            &space_delta, cookie);
    if (ret != ENGINE_SUCCESS)
        return ret;

    if (old_elem != NULL) {
        /* replace path */
        CLOG_MAP_ELEM_INSERT(info, old_elem, elem);
        if (old_elem->refcount == 0)
            htree_elem_free(old_elem);
        if (replaced) *replaced = true;
        if (space_delta > 0)
            do_coll_space_incr((coll_meta_info *)info, ITEM_TYPE_MAP, (size_t)space_delta);
        else if (space_delta < 0)
            do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_MAP, (size_t)-space_delta);
    } else {
        /* new insert path */
        CLOG_MAP_ELEM_INSERT(info, NULL, elem);
        info->ccnt++;
        do_coll_space_incr((coll_meta_info *)info, ITEM_TYPE_MAP, (size_t)space_delta);
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
    elem = htree_elem_alloc(nfield, nfield + nbytes, cookie);
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
        htree_elem_release((htree_elem_item *)elem_array[cnt++]);
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
        if (info->root != NULL)
            *del_count = do_map_elem_get(info, numfields, flist, true, NULL);
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

uint32_t map_elem_delete_with_count(map_meta_info *info, const uint32_t count)
{
    uint32_t fcnt = 0;
    if (info->root != NULL) {
        fcnt = htree_elem_traverse_dfs_bycnt((htree_node **)&info->root, info->root,
                                             count, true, NULL, NULL, NULL, NULL);
    }
    return fcnt;
}

/* See do_map_elem_traverse_dfs and do_map_elem_link. do_map_elem_traverse_dfs
 * can visit all elements, but only supports get and delete operations.
 * Do something similar and visit all elements.
 */
void map_elem_get_all(map_meta_info *info, elems_result_t *eresult)
{
    assert(eresult->elem_arrsz >= info->ccnt && eresult->elem_count == 0);
    eresult->elem_count = htree_elem_traverse_dfs_bycnt((htree_node **)&info->root, info->root,
                                                        0, false,
                                                        (htree_elem_item **)eresult->elem_array,
                                                        NULL, NULL, NULL);
    assert(eresult->elem_count == info->ccnt);
}

uint32_t map_elem_ntotal(map_elem_item *elem)
{
    return htree_elem_ntotal((htree_elem_item *)elem);
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

        elem = htree_elem_alloc(nfield, nfield + nbytes, NULL);
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
    uint32_t ndeleted = 0;
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

        if (do_map_elem_delete_with_field(info, &flist) == ENGINE_SUCCESS)
            ndeleted = 1;
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
