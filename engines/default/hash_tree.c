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

htree_hash_node *do_htree_node_alloc(uint8_t hash_depth, const void *cookie)
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

void do_htree_node_free(htree_hash_node *node)
{
    do_item_mem_free(node, sizeof(htree_hash_node));
}

htree_elem_item *do_htree_elem_alloc(uint8_t nfield, uint16_t nbytes, const void *cookie)
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

uint32_t do_htree_elem_ntotal(htree_elem_item *elem)
{
    return sizeof(htree_elem_item) + elem->nfield + elem->nbytes;
}

void do_htree_elem_free(htree_elem_item *elem)
{
    assert(elem->refcount == 0);
    assert(elem->slabs_clsid != 0);
    do_item_mem_free(elem, do_htree_elem_ntotal(elem));
}

void do_htree_elem_release(htree_elem_item *elem)
{
    if (elem->refcount != 0) {
        elem->refcount--;
    }
    if (elem->refcount == 0 && elem->status == ELEM_STATUS_UNLINKED) {
        do_htree_elem_free(elem);
    }
}

bool do_htree_node_is_leaf(const htree_hash_node *node)
{
    for (int hidx = 0; hidx < HTREE_HASHTAB_SIZE; hidx++) {
        if (node->hcnt[hidx] == -1)
            return false;
    }
    return true;
}

/* Split: redistribute par_node->htab[par_hidx] elem chain into child node.
 * If par_node is NULL, sets *root to node (root initialization). */
void do_htree_node_insert(htree_hash_node **root,
                        htree_hash_node *par_node, int par_hidx,
                        htree_hash_node *node)
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
}

/* Merge: pull all elems from child node back up to par_node->htab[par_hidx].
 * If par_node is NULL, clears *root and frees the root node. */
void do_htree_node_remove(htree_hash_node **root,
                          htree_hash_node *par_node, int par_hidx)
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
    assert(node->tot_elem_cnt == 0 || do_htree_node_is_leaf(node));

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
}

static inline bool htree_elem_match(const htree_elem_item *elem,
                                    const void *key, size_t klen)
{
    if (elem->nfield > 0)
        return (elem->nfield == klen && memcmp(elem->data, key, klen) == 0);
    return (elem->nbytes == klen && memcmp(elem->data, key, klen) == 0);
}

ENGINE_ERROR_CODE do_htree_elem_insert(htree_hash_node **root,
                                     htree_elem_item *elem,
                                     const void *key, size_t klen,
                                     bool replace_if_exist,
                                     htree_elem_item **old_elem,
                                     bool *node_split,
                                     const void *cookie)
{
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
        /* replace: swap find out, elem in */
        elem->next = find->next;
        if (prev != NULL) prev->next = elem;
        else              node->htab[hidx] = elem;
        elem->status = ELEM_STATUS_LINKED;
        find->status = ELEM_STATUS_UNLINKED;
        if (old_elem) *old_elem = find;
        return ENGINE_SUCCESS;
    }

    if (node->hcnt[hidx] >= HTREE_MAX_HASHCHAIN_SIZE) {
        htree_hash_node *n_node = do_htree_node_alloc(node->hdepth + 1, cookie);
        if (n_node == NULL)
            return ENGINE_ENOMEM;
        do_htree_node_insert(root, node, hidx, n_node);
        if (node_split) *node_split = true;
        node = n_node;
        hidx = HTREE_GET_HASHIDX(elem->hval, node->hdepth);
    } else {
        if (node_split) *node_split = false;
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

    if (old_elem) *old_elem = NULL;
    return ENGINE_SUCCESS;
}

int do_htree_traverse_sampling(htree_hash_node *node,
                               uint32_t remain, const uint32_t count,
                               htree_elem_item **elem_array)
{
    int hidx;
    int fcnt = 0;

    for (hidx = 0; hidx < HTREE_HASHTAB_SIZE; hidx++) {
        if (node->hcnt[hidx] == -1) {
            htree_hash_node *child_node = (htree_hash_node *)node->htab[hidx];
            fcnt += do_htree_traverse_sampling(child_node, remain,
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

htree_elem_item *do_htree_elem_at_offset(htree_hash_node **root,
                                         htree_hash_node *node,
                                         uint32_t offset, const bool delete,
                                         htree_elem_delete_cb on_delete,
                                         htree_node_remove_cb on_node_remove, void *ctx)
{
    int hidx;
    for (hidx = 0; hidx < HTREE_HASHTAB_SIZE; hidx++) {
        if (node->hcnt[hidx] == -1) {
            htree_hash_node *child_node = (htree_hash_node *)node->htab[hidx];
            if (offset >= child_node->tot_elem_cnt) {
                offset -= child_node->tot_elem_cnt;
                continue;
            }
            htree_elem_item *found = do_htree_elem_at_offset(root, child_node, offset,
                                                             delete, on_delete, on_node_remove, ctx);
            if (delete) {
                if (child_node->tot_elem_cnt < (HTREE_MAX_HASHCHAIN_SIZE/2)
                    && do_htree_node_is_leaf(child_node)) {
                    do_htree_node_remove(root, node, hidx);
                    if (on_node_remove) on_node_remove(ctx);
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
                do_htree_elem_remove(node, hidx, prev, elem);
                if (on_delete) on_delete(elem, ELEM_DELETE_NORMAL, ctx);
            }
            return elem;
        }
    }
    return NULL;
}

int do_htree_traverse_dfs_bycnt(htree_hash_node **root,
                                htree_hash_node *node,
                                const uint32_t count, const bool delete,
                                htree_elem_item **elem_array,
                                htree_elem_delete_cb on_delete,
                                htree_node_remove_cb on_node_remove,
                                enum elem_delete_cause cause, void *ctx)
{
    int hidx;
    int fcnt = 0;

    for (hidx = 0; hidx < HTREE_HASHTAB_SIZE; hidx++) {
        if (node->hcnt[hidx] == -1) {
            htree_hash_node *child_node = (htree_hash_node *)node->htab[hidx];
            int rcnt = (count > 0 ? (count - fcnt) : 0);
            int child_fcnt = do_htree_traverse_dfs_bycnt(root, child_node, rcnt, delete,
                                                         (elem_array==NULL ? NULL : &elem_array[fcnt]),
                                                         on_delete, on_node_remove, cause, ctx);
            fcnt += child_fcnt;
            if (delete && child_fcnt > 0) {
                if (child_node->tot_elem_cnt < (HTREE_MAX_HASHCHAIN_SIZE/2) &&
                    do_htree_node_is_leaf(child_node)) {
                    do_htree_node_remove(root, node, hidx);
                    if (on_node_remove) on_node_remove(ctx);
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
                    do_htree_elem_remove(node, hidx, NULL, elem);
                    if (on_delete) on_delete(elem, cause, ctx);
                }
                if (count > 0 && fcnt >= count) break;
                elem = (delete ? node->htab[hidx] : elem->next);
            }
        }
        if (count > 0 && fcnt >= count) break;
    }
    return fcnt;
}

bool do_htree_traverse_dfs_byfield(htree_hash_node **root,
                                   htree_hash_node *node,
                                   const uint32_t hval,
                                   const void *key, const size_t klen,
                                   const bool delete,
                                   htree_elem_item **elem_array,
                                   htree_elem_delete_cb on_delete,
                                   htree_node_remove_cb on_node_remove,
                                   void *ctx)
{
    bool ret;
    int hidx = HTREE_GET_HASHIDX(hval, node->hdepth);

    if (node->hcnt[hidx] == -1) {
        htree_hash_node *child_node = node->htab[hidx];
        ret = do_htree_traverse_dfs_byfield(root, child_node, hval, key, klen,
                                            delete, elem_array, on_delete, on_node_remove, ctx);
        if (ret && delete) {
            if (child_node->tot_elem_cnt < (HTREE_MAX_HASHCHAIN_SIZE/2) &&
                do_htree_node_is_leaf(child_node)) {
                do_htree_node_remove(root, node, hidx);
                if (on_node_remove) on_node_remove(ctx);
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
                        do_htree_elem_remove(node, hidx, prev, elem);
                        if (on_delete) on_delete(elem, ELEM_DELETE_NORMAL, ctx);
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

void do_htree_elem_remove(htree_hash_node *node, const int hidx,
                          htree_elem_item *prev, htree_elem_item *elem)
{
    if (prev != NULL) prev->next = elem->next;
    else              node->htab[hidx] = elem->next;
    elem->status = ELEM_STATUS_UNLINKED;
    node->hcnt[hidx] -= 1;
    node->tot_elem_cnt -= 1;
}

htree_elem_item *do_htree_elem_find(htree_hash_node *root,
                                    const void *key, size_t klen,
                                    htree_prev_info *pinfo)
{
    if (root == NULL) return NULL;

    uint32_t hval = (uint32_t)genhash_string_hash(key, klen);
    htree_hash_node *node = root;
    htree_elem_item *elem = NULL;
    htree_elem_item *prev = NULL;
    int hidx = -1;

    while (node != NULL) {
        hidx = HTREE_GET_HASHIDX(hval, node->hdepth);
        if (node->hcnt[hidx] >= 0)
            break;
        node = (htree_hash_node *)node->htab[hidx];
    }
    assert(node != NULL);

    for (elem = (htree_elem_item *)node->htab[hidx]; elem != NULL; elem = elem->next) {
        if (elem->hval == hval && htree_elem_match(elem, key, klen)) {
            if (pinfo != NULL) {
                pinfo->node = node;
                pinfo->prev = prev;
                pinfo->hidx = hidx;
            }
            break;
        }
        prev = elem;
    }
    return elem;
}
