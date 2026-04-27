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
#include <string.h>

#include "default_engine.h"
#include "hash_tree.h"

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

ENGINE_ERROR_CODE htree_elem_insert(htree_node           **root_pptr,
                                    htree_elem_item       *elem,
                                    htree_elem_match_func  match_fn,
                                    bool                   replace_if_exist,
                                    htree_elem_item      **old_elem_out,
                                    const void            *cookie)
{
    bool new_root = false;

    if (*root_pptr == NULL) {
        htree_node *root = do_htree_node_alloc(0, cookie);
        if (root == NULL)
            return ENGINE_ENOMEM;
        *root_pptr = root;
        new_root = true;
    }

    htree_node      *node = *root_pptr;
    htree_elem_item *prev = NULL;
    htree_elem_item *find;
    int hidx = -1;

    while (node != NULL) {
        hidx = HTREE_GET_HASHIDX(elem->hval, node->hdepth);
        if (node->hcnt[hidx] >= 0) /* elem chain */
            break;
        node = (htree_node *)node->htab[hidx];
    }
    assert(node != NULL && hidx != -1);

    prev = NULL;
    for (find = (htree_elem_item *)node->htab[hidx];
         find != NULL;
         find = (htree_elem_item *)find->next) {
        if (match_fn(find, elem))
            break;
        prev = find;
    }

    if (find != NULL) {
        if (replace_if_exist) {
            elem->next = find->next;
            if (prev != NULL)
                prev->next = elem;
            else
                node->htab[hidx] = elem;
            elem->status = ELEM_STATUS_LINKED;
            find->status = ELEM_STATUS_UNLINKED;
            if (old_elem_out)
                *old_elem_out = find;
            return ENGINE_SUCCESS;
        } else {
            return ENGINE_ELEM_EEXISTS;
        }
    }

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
        node = n_node;
        hidx = HTREE_GET_HASHIDX(elem->hval, node->hdepth);
    }

    elem->next       = node->htab[hidx];
    node->htab[hidx] = elem;
    node->hcnt[hidx] += 1;
    elem->status      = ELEM_STATUS_LINKED;

    /* update tot_elem_cnt from root down to node */
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
    return ENGINE_SUCCESS;
}
