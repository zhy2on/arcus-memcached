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
#include <stddef.h>
#include <string.h>

#include "default_engine.h"
#include "hash_tree.h"

extern int genhash_string_hash(const void *p, size_t nkey);

static htree_node *do_htree_node_alloc(const uint8_t depth, const void *cookie)
{
    size_t ntotal = sizeof(htree_node);
    htree_node *node = do_item_mem_alloc(ntotal, LRU_CLSID_FOR_SMALL, cookie);
    if (node != NULL) {
        node->slabs_clsid  = slabs_clsid(ntotal);
        node->refcount     = 0;
        node->hdepth       = depth;
        node->tot_elem_cnt = 0;
        memset(node->hcnt, 0, HTREE_HASHTAB_SIZE * sizeof(int16_t));
        memset(node->htab, 0, HTREE_HASHTAB_SIZE * sizeof(void *));
    }
    return node;
}

static void do_htree_node_free(htree_node *node)
{
    do_item_mem_free(node, sizeof(htree_node));
}

/* Redistribute elems from par_node->htab[par_hidx] into new child node,
 * then install child into par_node's slot. */
static void do_htree_node_link(htree_node *par_node, const int par_hidx,
                               htree_node *node)
{
    htree_elem_item *elem;
    while (par_node->htab[par_hidx] != NULL) {
        elem = (htree_elem_item *)par_node->htab[par_hidx];
        par_node->htab[par_hidx] = elem->next;

        int hidx = HTREE_GET_HASHIDX(elem->hval, node->hdepth);
        elem->next = node->htab[hidx];
        node->htab[hidx] = elem;
        node->hcnt[hidx] += 1;
        node->tot_elem_cnt += 1;
    }
    assert(node->tot_elem_cnt == par_node->hcnt[par_hidx]);
    par_node->htab[par_hidx] = node;
    par_node->hcnt[par_hidx] = -1; /* child hash node */
}

/* Traverse from root to the leaf node that owns hval, and search for a
 * matching elem (hval + nkey + data).  On return, *node_out and *hidx_out
 * identify the leaf slot; *prev_out is the predecessor (NULL = chain head).
 * Returns the matching elem, or NULL if not found. */
static htree_elem_item *do_htree_find_leaf(htree_node *root,
                                           uint32_t hval, uint16_t nkey,
                                           const unsigned char *data,
                                           htree_node **node_out, int *hidx_out,
                                           htree_elem_item **prev_out)
{
    htree_node      *node = root;
    htree_elem_item *prev = NULL;
    htree_elem_item *find;
    int hidx = -1;

    while (node != NULL) {
        hidx = HTREE_GET_HASHIDX(hval, node->hdepth);
        if (node->hcnt[hidx] >= 0)
            break;
        node = (htree_node *)node->htab[hidx];
    }
    assert(node != NULL && hidx != -1);

    for (find = (htree_elem_item *)node->htab[hidx];
         find != NULL;
         find = (htree_elem_item *)find->next) {
        if (find->hval == hval && find->nkey == nkey &&
            memcmp(find->data, data, nkey) == 0)
            break;
        prev = find;
    }

    *node_out = node;
    *hidx_out = hidx;
    *prev_out = prev;
    return find;
}

/* sticky check + replace find with elem. No overflow check (ccnt unchanged). */
static ENGINE_ERROR_CODE do_htree_elem_replace(htree_node *node, const int hidx,
                                               htree_elem_item *prev,
                                               htree_elem_item *find,
                                               htree_elem_item *elem,
                                               bool is_sticky,
                                               htree_elem_item **old_elem_out,
                                               ssize_t *space_delta_out)
{
#ifdef ENABLE_STICKY_ITEM
    if (is_sticky && find->nbytes < elem->nbytes) {
        if (do_item_sticky_overflowed())
            return ENGINE_ENOMEM;
    }
#endif
    elem->next = find->next;
    if (prev != NULL)
        prev->next = elem;
    else
        node->htab[hidx] = elem;
    elem->status  = ELEM_STATUS_LINKED;
    find->status  = ELEM_STATUS_UNLINKED;

    if (old_elem_out)
        *old_elem_out = find;
    if (space_delta_out) {
        size_t new_ntotal = offsetof(htree_elem_item, data) + elem->nbytes;
        size_t old_ntotal = offsetof(htree_elem_item, data) + find->nbytes;
        *space_delta_out = (ssize_t)slabs_space_size(new_ntotal)
                         - (ssize_t)slabs_space_size(old_ntotal);
    }
    return ENGINE_SUCCESS;
}

/* Link elem as a new entry at node->htab[hidx], splitting the chain into a
 * child node if needed.  Caller must run sticky/overflow checks beforehand. */
static ENGINE_ERROR_CODE do_htree_elem_insert(htree_node **root_pptr,
                                              htree_node *node, int hidx,
                                              htree_elem_item *elem,
                                              bool new_root,
                                              htree_elem_item **old_elem_out,
                                              ssize_t *space_delta_out,
                                              const void *cookie)
{
    bool node_created = new_root;
    if (node->hcnt[hidx] >= HTREE_MAX_HASHCHAIN_SIZE) {
        htree_node *n_node = do_htree_node_alloc(node->hdepth + 1, cookie);
        if (n_node == NULL) {
            if (new_root) {
                do_htree_node_free(*root_pptr);
                *root_pptr = NULL;
            }
            return ENGINE_ENOMEM;
        }
        do_htree_node_link(node, hidx, n_node);
        node  = n_node;
        hidx  = HTREE_GET_HASHIDX(elem->hval, node->hdepth);
        node_created = true;
    }

    elem->next       = node->htab[hidx];
    node->htab[hidx] = elem;
    node->hcnt[hidx] += 1;
    elem->status      = ELEM_STATUS_LINKED;

    htree_node *cur = *root_pptr;
    while (cur != node) {
        cur->tot_elem_cnt += 1;
        int cidx = HTREE_GET_HASHIDX(elem->hval, cur->hdepth);
        assert(cur->hcnt[cidx] == -1);
        cur = (htree_node *)cur->htab[cidx];
    }
    node->tot_elem_cnt += 1;

    if (old_elem_out)
        *old_elem_out = NULL;
    if (space_delta_out) {
        size_t new_ntotal = offsetof(htree_elem_item, data) + elem->nbytes;
        *space_delta_out = (ssize_t)slabs_space_size(new_ntotal);
        if (node_created)
            *space_delta_out += (ssize_t)slabs_space_size(sizeof(htree_node));
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE htree_elem_update(htree_node      **root_pptr,
                                    htree_elem_item  *elem,
                                    bool              is_sticky,
                                    htree_elem_item **old_elem_out,
                                    ssize_t          *space_delta_out,
                                    const void       *cookie)
{
    if (*root_pptr == NULL)
        return ENGINE_ELEM_ENOENT;

    htree_node      *node;
    htree_elem_item *prev;
    int hidx;

    htree_elem_item *find = do_htree_find_leaf(*root_pptr, elem->hval,
                                               elem->nkey, elem->data,
                                               &node, &hidx, &prev);
    if (find == NULL)
        return ENGINE_ELEM_ENOENT;

    if (find->refcount == 0 && find->nbytes == elem->nbytes) {
        /* in-place update: same size, overwrite data[] without realloc */
        memcpy(find->data, elem->data, elem->nbytes);
        if (old_elem_out)    *old_elem_out   = find;
        if (space_delta_out) *space_delta_out = 0;
        return ENGINE_SUCCESS;
    }

    return do_htree_elem_replace(node, hidx, prev, find, elem,
                                 is_sticky, old_elem_out, space_delta_out);
}

ENGINE_ERROR_CODE htree_elem_insert(htree_node      **root_pptr,
                                    htree_elem_item  *elem,
                                    bool              replace_if_exist,
                                    bool              is_sticky,
                                    int               max_count,
                                    htree_elem_item **old_elem_out,
                                    ssize_t          *space_delta_out,
                                    const void       *cookie)
{
    bool new_root = false;

    elem->hval = genhash_string_hash(elem->data, elem->nkey);

    if (*root_pptr == NULL) {
        htree_node *root = do_htree_node_alloc(0, cookie);
        if (root == NULL)
            return ENGINE_ENOMEM;
        *root_pptr = root;
        new_root = true;
    }

    htree_node      *node;
    htree_elem_item *prev;
    int hidx;

    /* insert-only: check before find so overflow takes priority over EEXISTS */
    if (!replace_if_exist) {
#ifdef ENABLE_STICKY_ITEM
        if (is_sticky && do_item_sticky_overflowed()) {
            if (new_root) { do_htree_node_free(*root_pptr); *root_pptr = NULL; }
            return ENGINE_ENOMEM;
        }
#endif
        if (max_count > 0 && (int)(*root_pptr)->tot_elem_cnt >= max_count) {
            if (new_root) { do_htree_node_free(*root_pptr); *root_pptr = NULL; }
            return ENGINE_EOVERFLOW;
        }
    }

    htree_elem_item *find = do_htree_find_leaf(*root_pptr, elem->hval,
                                               elem->nkey, elem->data,
                                               &node, &hidx, &prev);

    if (find != NULL) {
        if (!replace_if_exist)
            return ENGINE_ELEM_EEXISTS;
        return do_htree_elem_replace(node, hidx, prev, find, elem,
                                     is_sticky, old_elem_out, space_delta_out);
    }

    /* upsert new insert path: check after find (no duplicate found) */
    if (replace_if_exist) {
#ifdef ENABLE_STICKY_ITEM
        if (is_sticky && do_item_sticky_overflowed()) {
            if (new_root) { do_htree_node_free(*root_pptr); *root_pptr = NULL; }
            return ENGINE_ENOMEM;
        }
#endif
        if (max_count > 0 && (int)(*root_pptr)->tot_elem_cnt >= max_count) {
            if (new_root) { do_htree_node_free(*root_pptr); *root_pptr = NULL; }
            return ENGINE_EOVERFLOW;
        }
    }

    return do_htree_elem_insert(root_pptr, node, hidx, elem, new_root,
                                old_elem_out, space_delta_out, cookie);
}
