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
void do_htree_node_link(htree_hash_node **root,
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
void do_htree_node_unlink(htree_hash_node **root,
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

htree_elem_item *do_htree_elem_find(htree_hash_node *root,
                                    const void *key, size_t klen,
                                    htree_elem_match_func match_func,
                                    htree_prev_info *pinfo)
{
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
        if (elem->hval == hval && match_func(elem, key, klen)) {
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
