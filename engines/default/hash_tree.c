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
#include <stdlib.h>
#include <string.h>

#include "default_engine.h"
#include "hash_tree.h"

#define HTREE_HASHIDX_MASK       0x0000000F
#define HTREE_MAX_HASHCHAIN_SIZE 64

#define HTREE_GET_HASHIDX(hval, hdepth) \
    (((hval) & (HTREE_HASHIDX_MASK << ((hdepth)*4))) >> ((hdepth)*4))

extern int genhash_string_hash(const void *p, size_t nkey);

static inline uint32_t htree_hash(htree_ops *ops, const htree_elem_item *elem)
{
    uint16_t nkey;
    const void *key = ops->get_key(elem, &nkey);
    return (uint32_t)genhash_string_hash(key, nkey);
}

static inline bool htree_key_eq(htree_ops *ops,
                                const htree_elem_item *a, const htree_elem_item *b)
{
    uint16_t na, nb;
    const void *ka = ops->get_key(a, &na);
    const void *kb = ops->get_key(b, &nb);
    return na == nb && memcmp(ka, kb, na) == 0;
}

static inline bool htree_key_eq_raw(htree_ops *ops, const htree_elem_item *elem,
                                    const void *key, uint16_t nkey)
{
    uint16_t lnkey;
    const void *lkey = ops->get_key(elem, &lnkey);
    return lnkey == nkey && memcmp(lkey, key, nkey) == 0;
}

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

static inline void space_delta_add(ssize_t *delta, ssize_t size)
{
    if (delta) *delta += size;
}

static void do_htree_node_link(htree_node *par_node, const int par_hidx,
                               htree_node *node)
{
    /* redistribute elems from par_node's chain into the new child node (split) */
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
    par_node->hcnt[par_hidx] = -1;
}

static void do_htree_elem_link(htree_node **root_pptr,
                               htree_node *node, const int hidx,
                               htree_elem_item *elem)
{
    /* prepend elem to the hash chain */
    elem->next       = node->htab[hidx];
    node->htab[hidx] = elem;
    node->hcnt[hidx] += 1;

    /* increment tot_elem_cnt on every node from root down to the target node */
    htree_node *cur = *root_pptr;
    while (cur != node) {
        cur->tot_elem_cnt += 1;
        int cidx = HTREE_GET_HASHIDX(elem->hval, cur->hdepth);
        assert(cur->hcnt[cidx] == -1);
        cur = (htree_node *)cur->htab[cidx];
    }
    node->tot_elem_cnt += 1;
}

htree_elem_item *htree_elem_find(htree_node *root,
                                 const void *key, uint16_t nkey,
                                 htree_ops *ops,
                                 htree_elem_pos *pos)
{
    if (root == NULL)
        return NULL;

    uint32_t hval = (uint32_t)genhash_string_hash(key, nkey);
    htree_node *node = root;
    htree_elem_item *prev = NULL;
    int hidx = -1;

    while (node != NULL) {
        hidx = HTREE_GET_HASHIDX(hval, node->hdepth);
        if (node->hcnt[hidx] >= 0)
            break;
        node = (htree_node *)node->htab[hidx];
    }
    assert(node != NULL && hidx != -1);

    htree_elem_item *find;
    for (find = (htree_elem_item *)node->htab[hidx];
         find != NULL;
         find = find->next) {
        if (find->hval == hval && htree_key_eq_raw(ops, find, key, nkey))
            break;
        prev = find;
    }

    if (find != NULL && pos != NULL) {
        pos->node = node;
        pos->hidx = hidx;
        pos->prev = prev;
    }
    return find;
}

void htree_elem_replace_at(htree_elem_pos *pos,
                           htree_elem_item *old_elem,
                           htree_elem_item *new_elem)
{
    new_elem->hval = old_elem->hval;
    new_elem->next = old_elem->next;
    if (pos->prev != NULL)
        pos->prev->next = new_elem;
    else
        pos->node->htab[pos->hidx] = new_elem;
}

ENGINE_ERROR_CODE htree_elem_link(htree_node **root_pptr,
                                  htree_elem_item *elem,
                                  htree_ops *ops,
                                  ssize_t *htree_space_delta,
                                  const void *cookie)
{
    if (htree_space_delta) *htree_space_delta = 0;

    elem->hval = htree_hash(ops, elem);

    /* allocate root node if the tree is empty */
    bool new_root = false;
    if (*root_pptr == NULL) {
        htree_node *root = do_htree_node_alloc(0, cookie);
        if (root == NULL)
            return ENGINE_ENOMEM;
        *root_pptr = root;
        new_root = true;
    }

    /* traverse to the leaf node that should contain this element */
    htree_node *node = *root_pptr;
    int hidx;
    while (true) {
        hidx = HTREE_GET_HASHIDX(elem->hval, node->hdepth);
        if (node->hcnt[hidx] >= 0)
            break;
        node = (htree_node *)node->htab[hidx];
    }

    /* check for duplicate key in the hash chain */
    for (htree_elem_item *find = (htree_elem_item *)node->htab[hidx];
         find != NULL; find = find->next) {
        if (find->hval == elem->hval && htree_key_eq(ops, find, elem))
            return ENGINE_ELEM_EEXISTS;
    }

    /* split the hash chain into a new child node if it is full */
    if (node->hcnt[hidx] >= HTREE_MAX_HASHCHAIN_SIZE) {
        htree_node *n_node = do_htree_node_alloc(node->hdepth + 1, cookie);
        if (n_node == NULL)
            return ENGINE_ENOMEM;
        do_htree_node_link(node, hidx, n_node);
        space_delta_add(htree_space_delta, (ssize_t)slabs_space_size(sizeof(htree_node)));
        node = n_node;
        hidx = HTREE_GET_HASHIDX(elem->hval, node->hdepth);
    }

    do_htree_elem_link(root_pptr, node, hidx, elem);
    if (new_root)
        space_delta_add(htree_space_delta, (ssize_t)slabs_space_size(sizeof(htree_node)));
    return ENGINE_SUCCESS;
}
