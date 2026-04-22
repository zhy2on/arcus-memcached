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
#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "default_engine.h"
#include "hash_tree.h"

extern int genhash_string_hash(const void *p, size_t nkey);

static inline ENGINE_ITEM_TYPE htree_meta_item_type(const coll_meta_info *meta)
{
    hash_item *it = (hash_item *)COLL_GET_HASH_ITEM(meta);
    return (ENGINE_ITEM_TYPE)GET_ITEM_TYPE(it);
}

htree_hash_node *htree_node_alloc(uint8_t hash_depth, const void *cookie)
{
    size_t ntotal = sizeof(htree_hash_node);

    htree_hash_node *node = do_item_mem_alloc(ntotal, LRU_CLSID_FOR_SMALL, cookie);
    if (node != NULL) {
        node->slabs_clsid  = slabs_clsid(ntotal);
        assert(node->slabs_clsid > 0);

        node->refcount     = 0;
        node->hdepth       = hash_depth;
        node->tot_elem_cnt = 0;
        memset(node->hcnt, 0, HTREE_HASHTAB_SIZE * sizeof(uint16_t));
        memset(node->htab, 0, HTREE_HASHTAB_SIZE * sizeof(void *));
    }
    return node;
}

static void do_htree_node_free(htree_hash_node *node)
{
    do_item_mem_free(node, sizeof(htree_hash_node));
}

htree_elem_item *htree_elem_alloc(uint8_t nfield, uint16_t nbytes, const void *cookie)
{
    size_t ntotal = sizeof(htree_elem_item) + nfield + nbytes;

    htree_elem_item *elem = do_item_mem_alloc(ntotal, LRU_CLSID_FOR_SMALL, cookie);
    if (elem != NULL) {
        elem->slabs_clsid = slabs_clsid(ntotal);
        assert(elem->slabs_clsid > 0);

        elem->refcount = 0;
        elem->nfield   = nfield;
        elem->nbytes   = nbytes;
        elem->status   = ELEM_STATUS_UNLINKED;
    }
    return elem;
}

uint32_t htree_elem_ntotal(htree_elem_item *elem)
{
    return sizeof(htree_elem_item) + elem->nfield + elem->nbytes;
}

void htree_elem_free(htree_elem_item *elem)
{
    assert(elem->refcount == 0);
    assert(elem->slabs_clsid != 0);
    do_item_mem_free(elem, htree_elem_ntotal(elem));
}

void htree_elem_release(htree_elem_item *elem)
{
    if (elem->refcount != 0) {
        elem->refcount--;
    }
    if (elem->refcount == 0 && elem->status == ELEM_STATUS_UNLINKED) {
        htree_elem_free(elem);
    }
}

static bool htree_node_is_leaf(const htree_hash_node *node)
{
    for (int hidx = 0; hidx < HTREE_HASHTAB_SIZE; hidx++) {
        if (node->hcnt[hidx] == -1)
            return false;
    }
    return true;
}

/* Split: redistribute par_node->htab[par_hidx] elem chain into child node.
 * If par_node is NULL, sets *root to node (root initialization).
 * Caller must have already allocated the node via htree_node_alloc. */
void htree_node_link(htree_hash_node **root,
                     htree_hash_node *par_node, int par_hidx,
                     htree_hash_node *node, coll_meta_info *meta)
{
    if (par_node == NULL) {
        *root = node;
        return;
    }

    int num_elems = par_node->hcnt[par_hidx];
    int num_found = 0;

    htree_elem_item *elem;
    while (par_node->htab[par_hidx] != NULL) {
        elem = (htree_elem_item *)par_node->htab[par_hidx];
        par_node->htab[par_hidx] = elem->next;

        int hidx = HTREE_GET_HASHIDX(elem->hval, node->hdepth);
        elem->next = node->htab[hidx];
        node->htab[hidx] = elem;
        node->hcnt[hidx] += 1;
        num_found++;
    }
    assert(num_found == num_elems);
    node->tot_elem_cnt = num_found;

    par_node->htab[par_hidx] = node;
    par_node->hcnt[par_hidx] = -1; /* child hash node */

    do_coll_space_incr(meta, htree_meta_item_type(meta),
                       slabs_space_size(sizeof(htree_hash_node)));
}

/* Merge: pull all elems from child node back up to par_node->htab[par_hidx].
 * If par_node is NULL, clears *root and frees the root node. */
void htree_node_unlink(htree_hash_node **root,
                       htree_hash_node *par_node, int par_hidx,
                       coll_meta_info *meta)
{
    htree_hash_node *node;

    if (par_node == NULL) {
        node = *root;
        *root = NULL;
        assert(node->tot_elem_cnt == 0);
        do_htree_node_free(node);
        return;
    }

    assert(par_node->hcnt[par_hidx] == -1); /* child hash node */
    node = (htree_hash_node *)par_node->htab[par_hidx];
    assert(node->tot_elem_cnt == 0 || htree_node_is_leaf(node));

    htree_elem_item *head = NULL;
    htree_elem_item *elem;
    int hidx, fcnt = 0;

    for (hidx = 0; hidx < HTREE_HASHTAB_SIZE; hidx++) {
        assert(node->hcnt[hidx] >= 0);
        if (node->hcnt[hidx] > 0) {
            fcnt += node->hcnt[hidx];
            while (node->htab[hidx] != NULL) {
                elem = (htree_elem_item *)node->htab[hidx];
                node->htab[hidx] = elem->next;
                node->hcnt[hidx] -= 1;

                elem->next = head;
                head = elem;
            }
            assert(node->hcnt[hidx] == 0);
        }
    }
    assert(fcnt == node->tot_elem_cnt);
    node->tot_elem_cnt = 0;

    par_node->htab[par_hidx] = head;
    par_node->hcnt[par_hidx] = fcnt;

    do_htree_node_free(node);

    if (meta->stotal > 0)
        do_coll_space_decr(meta, htree_meta_item_type(meta),
                           slabs_space_size(sizeof(htree_hash_node)));
}

static inline bool htree_elem_match(const htree_elem_item *elem,
                                    const void *key, size_t klen)
{
    if (elem->nfield > 0)
        return (elem->nfield == klen && memcmp(elem->data, key, klen) == 0);
    return (elem->nbytes == klen && memcmp(elem->data, key, klen) == 0);
}

ENGINE_ERROR_CODE htree_elem_insert(htree_hash_node **root,
                                    htree_elem_item *elem,
                                    const void *key, size_t klen,
                                    bool replace_if_exist,
                                    htree_elem_item **replaced_out,
                                    htree_pre_replace_cb on_pre_replace,
                                    htree_pre_insert_cb on_pre_insert,
                                    coll_meta_info *meta, const void *cookie)
{
    if (replaced_out) *replaced_out = NULL;

    elem->hval = (uint32_t)genhash_string_hash(key, klen);
    htree_hash_node *node = *root;
    htree_elem_item *prev = NULL;
    htree_elem_item *find = NULL;
    int hidx = -1;

    while (node != NULL) {
        hidx = HTREE_GET_HASHIDX(elem->hval, node->hdepth);
        if (node->hcnt[hidx] >= 0)
            break;
        node = (htree_hash_node *)node->htab[hidx];
    }
    assert(node != NULL);

    for (find = (htree_elem_item *)node->htab[hidx]; find != NULL; find = find->next) {
        if (find->hval == elem->hval && htree_elem_match(find, key, klen))
            break;
        prev = find;
    }

    if (find != NULL) {
        if (!replace_if_exist)
            return ENGINE_ELEM_EEXISTS;
        if (on_pre_replace) {
            ENGINE_ERROR_CODE ret = on_pre_replace(find, elem, meta);
            if (ret != ENGINE_SUCCESS) return ret;
        }
        /* replace: swap find out, elem in */
        elem->next = find->next;
        if (prev != NULL) prev->next = elem;
        else              node->htab[hidx] = elem;
        elem->status = ELEM_STATUS_LINKED;
        find->status = ELEM_STATUS_UNLINKED;

        /* space accounting for size difference */
        size_t old_stotal = slabs_space_size(htree_elem_ntotal(find));
        size_t new_stotal = slabs_space_size(htree_elem_ntotal(elem));
        if (new_stotal != old_stotal) {
            assert(meta->stotal > 0);
            ENGINE_ITEM_TYPE itype = htree_meta_item_type(meta);
            if (new_stotal > old_stotal)
                do_coll_space_incr(meta, itype, new_stotal - old_stotal);
            else
                do_coll_space_decr(meta, itype, old_stotal - new_stotal);
        }
        if (replaced_out)
            *replaced_out = find;
        else if (find->refcount == 0)
            htree_elem_free(find);
        return ENGINE_SUCCESS;
    }

    if (on_pre_insert) {
        ENGINE_ERROR_CODE ret = on_pre_insert(elem, meta);
        if (ret != ENGINE_SUCCESS) return ret;
    }

    if (node->hcnt[hidx] >= HTREE_MAX_HASHCHAIN_SIZE) {
        htree_hash_node *n_node = htree_node_alloc(node->hdepth + 1, cookie);
        if (n_node == NULL)
            return ENGINE_ENOMEM;
        htree_node_link(root, node, hidx, n_node, meta);
        node = n_node;
        hidx = HTREE_GET_HASHIDX(elem->hval, node->hdepth);
    }

    elem->next = (htree_elem_item *)node->htab[hidx];
    node->htab[hidx] = elem;
    node->hcnt[hidx] += 1;
    node->tot_elem_cnt += 1;
    elem->status = ELEM_STATUS_LINKED;

    htree_hash_node *par_node = *root;
    while (par_node != node) {
        par_node->tot_elem_cnt += 1;
        hidx = HTREE_GET_HASHIDX(elem->hval, par_node->hdepth);
        assert(par_node->hcnt[hidx] == -1);
        par_node = (htree_hash_node *)par_node->htab[hidx];
    }

    meta->ccnt++;
    do_coll_space_incr(meta, htree_meta_item_type(meta),
                       slabs_space_size(htree_elem_ntotal(elem)));
    return ENGINE_SUCCESS;
}

int htree_traverse_sampling(htree_hash_node *node,
                               uint32_t remain, const uint32_t count,
                               htree_elem_item **elem_array)
{
    int hidx;
    int fcnt = 0;

    for (hidx = 0; hidx < HTREE_HASHTAB_SIZE; hidx++) {
        if (node->hcnt[hidx] == -1) {
            htree_hash_node *child_node = (htree_hash_node *)node->htab[hidx];
            fcnt += htree_traverse_sampling(child_node, remain,
                                               count - fcnt, &elem_array[fcnt]);
            remain -= child_node->tot_elem_cnt;
        } else if (node->hcnt[hidx] > 0) {
            htree_elem_item *elem = node->htab[hidx];
            while (elem != NULL) {
                if ((rand() % remain) < (count - fcnt)) {
                    elem->refcount++;
                    elem_array[fcnt] = elem;
                    fcnt++;
                    if (fcnt >= count) break;
                }
                remain -= 1;
                elem = elem->next;
            }
        }
        if (fcnt >= count) break;
    }
    return fcnt;
}

htree_elem_item *htree_elem_at_offset(htree_hash_node **root,
                                         htree_hash_node *node,
                                         uint32_t offset, const bool delete,
                                         htree_elem_delete_cb on_elem_delete,
                                         coll_meta_info *meta)
{
    int hidx;
    for (hidx = 0; hidx < HTREE_HASHTAB_SIZE; hidx++) {
        if (node->hcnt[hidx] == -1) {
            htree_hash_node *child_node = (htree_hash_node *)node->htab[hidx];
            if (offset >= child_node->tot_elem_cnt) {
                offset -= child_node->tot_elem_cnt;
                continue;
            }
            htree_elem_item *found = htree_elem_at_offset(root, child_node, offset,
                                                          delete, on_elem_delete, meta);
            if (delete) {
                if (child_node->tot_elem_cnt < (HTREE_MAX_HASHCHAIN_SIZE/2)
                    && htree_node_is_leaf(child_node)) {
                    htree_node_unlink(root, node, hidx, meta);
                }
                node->tot_elem_cnt -= 1;
            }
            return found;
        } else if (node->hcnt[hidx] > 0) {
            if (offset >= (uint32_t)node->hcnt[hidx]) {
                offset -= node->hcnt[hidx];
                continue;
            }
            htree_elem_item *prev = NULL;
            htree_elem_item *elem = node->htab[hidx];
            while (offset > 0) {
                prev = elem;
                elem = elem->next;
                offset -= 1;
            }
            elem->refcount++;
            if (delete) {
                do_htree_elem_delete(node, hidx, prev, elem);
                meta->ccnt--;
                if (meta->stotal > 0)
                    do_coll_space_decr(meta, htree_meta_item_type(meta),
                                       slabs_space_size(htree_elem_ntotal(elem)));
                if (on_elem_delete) on_elem_delete(elem, ELEM_DELETE_NORMAL, meta);
                if (elem->refcount == 0) htree_elem_free(elem);
            }
            return elem;
        }
    }
    return NULL;
}

int htree_traverse_dfs_bycnt(htree_hash_node **root,
                                htree_hash_node *node,
                                const uint32_t count, const bool delete,
                                htree_elem_item **elem_array,
                                htree_elem_delete_cb on_elem_delete,
                                enum elem_delete_cause cause,
                                coll_meta_info *meta)
{
    int hidx;
    int fcnt = 0;

    for (hidx = 0; hidx < HTREE_HASHTAB_SIZE; hidx++) {
        if (node->hcnt[hidx] == -1) {
            htree_hash_node *child_node = (htree_hash_node *)node->htab[hidx];
            int rcnt = (count > 0 ? (count - fcnt) : 0);
            int child_fcnt = htree_traverse_dfs_bycnt(root, child_node, rcnt, delete,
                                                      (elem_array==NULL ? NULL : &elem_array[fcnt]),
                                                      on_elem_delete, cause, meta);
            fcnt += child_fcnt;
            if (delete && child_fcnt > 0) {
                if (child_node->tot_elem_cnt < (HTREE_MAX_HASHCHAIN_SIZE/2) &&
                    htree_node_is_leaf(child_node)) {
                    htree_node_unlink(root, node, hidx, meta);
                }
                node->tot_elem_cnt -= child_fcnt;
            }
        } else if (node->hcnt[hidx] > 0) {
            htree_elem_item *elem = node->htab[hidx];
            while (elem != NULL) {
                if (elem_array) {
                    elem->refcount++;
                    elem_array[fcnt] = elem;
                }
                fcnt++;
                if (delete) {
                    do_htree_elem_delete(node, hidx, NULL, elem);
                    meta->ccnt--;
                    if (meta->stotal > 0)
                        do_coll_space_decr(meta, htree_meta_item_type(meta),
                                           slabs_space_size(htree_elem_ntotal(elem)));
                    if (on_elem_delete) on_elem_delete(elem, cause, meta);
                    if (elem->refcount == 0) htree_elem_free(elem);
                }
                if (count > 0 && fcnt >= count) break;
                elem = (delete ? node->htab[hidx] : elem->next);
            }
        }
        if (count > 0 && fcnt >= count) break;
    }
    return fcnt;
}

static bool do_htree_traverse_dfs_byfield(htree_hash_node **root,
                                          htree_hash_node *node,
                                          const uint32_t hval,
                                          const void *key, const size_t klen,
                                          const bool delete,
                                          htree_elem_item **elem_array,
                                          htree_elem_delete_cb on_elem_delete,
                                          coll_meta_info *meta)
{
    bool ret;
    int hidx = HTREE_GET_HASHIDX(hval, node->hdepth);

    if (node->hcnt[hidx] == -1) {
        htree_hash_node *child_node = node->htab[hidx];
        ret = do_htree_traverse_dfs_byfield(root, child_node, hval, key, klen,
                                            delete, elem_array, on_elem_delete, meta);
        if (ret && delete) {
            if (child_node->tot_elem_cnt < (HTREE_MAX_HASHCHAIN_SIZE/2) &&
                htree_node_is_leaf(child_node)) {
                htree_node_unlink(root, node, hidx, meta);
            }
            node->tot_elem_cnt -= 1;
        }
    } else {
        ret = false;
        if (node->hcnt[hidx] > 0) {
            htree_elem_item *prev = NULL;
            htree_elem_item *elem = node->htab[hidx];
            while (elem != NULL) {
                if (elem->hval == hval && htree_elem_match(elem, key, klen)) {
                    if (elem_array) {
                        elem->refcount++;
                        elem_array[0] = elem;
                    }
                    if (delete) {
                        do_htree_elem_delete(node, hidx, prev, elem);
                        meta->ccnt--;
                        if (meta->stotal > 0)
                            do_coll_space_decr(meta, htree_meta_item_type(meta),
                                               slabs_space_size(htree_elem_ntotal(elem)));
                        if (on_elem_delete) on_elem_delete(elem, ELEM_DELETE_NORMAL, meta);
                        if (elem->refcount == 0) htree_elem_free(elem);
                    }
                    ret = true;
                    break;
                }
                prev = elem;
                elem = elem->next;
            }
        }
    }
    return ret;
}

bool htree_traverse_dfs_byfield(htree_hash_node **root,
                                   htree_hash_node *node,
                                   const void *key, const size_t klen,
                                   const bool delete,
                                   htree_elem_item **elem_array,
                                   htree_elem_delete_cb on_elem_delete,
                                   coll_meta_info *meta)
{
    uint32_t hval = (uint32_t)genhash_string_hash(key, klen);
    return do_htree_traverse_dfs_byfield(root, node, hval, key, klen,
                                         delete, elem_array, on_elem_delete, meta);
}

static void do_htree_elem_delete(htree_hash_node *node, const int hidx,
                          htree_elem_item *prev, htree_elem_item *elem)
{
    if (prev != NULL) prev->next = elem->next;
    else              node->htab[hidx] = elem->next;
    elem->status = ELEM_STATUS_UNLINKED;
    node->hcnt[hidx] -= 1;
    node->tot_elem_cnt -= 1;
}

htree_elem_item *htree_elem_find(htree_hash_node *root,
                                    const void *key, size_t klen)
{
    if (root == NULL) return NULL;

    uint32_t hval = (uint32_t)genhash_string_hash(key, klen);
    htree_hash_node *node = root;
    int hidx = -1;

    while (node != NULL) {
        hidx = HTREE_GET_HASHIDX(hval, node->hdepth);
        if (node->hcnt[hidx] >= 0)
            break;
        node = (htree_hash_node *)node->htab[hidx];
    }
    assert(node != NULL);

    for (htree_elem_item *elem = (htree_elem_item *)node->htab[hidx];
         elem != NULL; elem = elem->next) {
        if (elem->hval == hval && htree_elem_match(elem, key, klen))
            return elem;
    }
    return NULL;
}
