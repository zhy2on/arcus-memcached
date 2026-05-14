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

static void do_htree_node_free(htree_node *node)
{
    do_item_mem_free(node, sizeof(htree_node));
}

static inline void space_delta_add(ssize_t *delta, ssize_t size)
{
    if (delta) *delta += size;
}

static inline bool is_leaf_node(const htree_node *node)
{
    for (int hidx = 0; hidx < HTREE_HASHTAB_SIZE; hidx++) {
        if (node->hcnt[hidx] == -1)
            return false;
    }
    return true;
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

static void do_htree_node_unlink(htree_node **root_pptr,
                                 htree_node *par_node, const int par_hidx)
{
    htree_node *node;

    if (par_node == NULL) {
        /* removing the root: must already be empty */
        node = *root_pptr;
        *root_pptr = NULL;
        assert(node->tot_elem_cnt == 0);
    } else {
        /* merge child node's elements back into par_node's hash chain */
        assert(par_node->hcnt[par_hidx] == -1); /* par_hidx must point to a child node */

        node = (htree_node *)par_node->htab[par_hidx];

        htree_elem_item *head = NULL;
        int fcnt = 0;

        for (int hidx = 0; hidx < HTREE_HASHTAB_SIZE; hidx++) {
            assert(node->hcnt[hidx] >= 0); /* child must be a leaf (no grandchildren) */
            if (node->hcnt[hidx] == 0)
                continue;

            /* prepend this slot's chain onto head */
            fcnt += node->hcnt[hidx];
            while (node->htab[hidx] != NULL) {
                htree_elem_item *elem = (htree_elem_item *)node->htab[hidx];
                node->htab[hidx] = elem->next;
                node->hcnt[hidx] -= 1;
                elem->next = head;
                head = elem;
            }
        }
        assert(fcnt == (int)node->tot_elem_cnt);

        par_node->htab[par_hidx] = head;
        par_node->hcnt[par_hidx] = fcnt;
    }

    do_htree_node_free(node);
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

static void do_htree_elem_unlink(htree_node **root_pptr,
                                 htree_node *node, int hidx,
                                 htree_elem_item *prev, htree_elem_item *elem)
{
    /* remove link from the hash chain */
    if (prev != NULL) prev->next = elem->next;
    else              node->htab[hidx] = elem->next;

    node->hcnt[hidx] -= 1;
    elem->next = NULL;

    /* decrement tot_elem_cnt on every node from root down to the target node */
    htree_node *cur = *root_pptr;
    while (cur != node) {
        cur->tot_elem_cnt -= 1;
        int cidx = HTREE_GET_HASHIDX(elem->hval, cur->hdepth);
        assert(cur->hcnt[cidx] == -1);
        cur = (htree_node *)cur->htab[cidx];
    }
    node->tot_elem_cnt -= 1;
}

static uint32_t do_htree_elem_get_by_cnt(htree_node *node, uint32_t count,
                                         htree_elem_item **elem_array, uint32_t fcnt)
{
    for (int hidx = 0; hidx < HTREE_HASHTAB_SIZE; hidx++) {
        if (node->hcnt[hidx] == -1) {
            htree_node *child_node = (htree_node *)node->htab[hidx];
            fcnt = do_htree_elem_get_by_cnt(child_node, count, elem_array, fcnt);
        } else if (node->hcnt[hidx] > 0) {
            htree_elem_item *elem = (htree_elem_item *)node->htab[hidx];
            while (elem != NULL) {
                elem_array[fcnt++] = elem;
                elem = elem->next;
                if (count > 0 && fcnt >= count) return fcnt;
            }
        }
        if (count > 0 && fcnt >= count) break;
    }
    return fcnt;
}

/* Unlinks and returns the element at logical position `offset` (0-based DFS order).
 *
 * Traversal: slots are visited in order (hidx 0..15).
 *   - hcnt == -1  : child node  — skip if offset falls outside its subtree,
 *                   otherwise recurse.
 *   - hcnt >  0   : hash chain  — skip if offset falls outside this chain,
 *                   otherwise walk to the exact position and unlink.
 *
 * tot_elem_cnt propagation: each level decrements its own node->tot_elem_cnt
 * on the way back up the call stack.  This avoids the need for a root-to-leaf
 * pass (contrast with do_htree_elem_unlink which walks from root every time). */
static htree_elem_item *do_htree_elem_unlink_at_offset(htree_node **root_pptr,
                                                       htree_node *node,
                                                       uint32_t offset,
                                                       ssize_t *htree_space_delta)
{
    for (int hidx = 0; hidx < HTREE_HASHTAB_SIZE; hidx++) {
        if (node->hcnt[hidx] == -1) {
            htree_node *child_node = (htree_node *)node->htab[hidx];
            if (offset >= child_node->tot_elem_cnt) {
                /* target is not in this subtree; skip past it */
                offset -= child_node->tot_elem_cnt;
                continue;
            }
            htree_elem_item *elem = do_htree_elem_unlink_at_offset(root_pptr, child_node,
                                                                   offset, htree_space_delta);
            /* merge child back into this node if it fell below the threshold */
            if (is_leaf_node(child_node) &&
                child_node->tot_elem_cnt < (HTREE_MAX_HASHCHAIN_SIZE / 2)) {
                do_htree_node_unlink(root_pptr, node, hidx);
                space_delta_add(htree_space_delta, -(ssize_t)slabs_space_size(sizeof(htree_node)));
            }
            node->tot_elem_cnt -= 1;
            return elem;
        } else if (node->hcnt[hidx] > 0) {
            if (offset >= (uint32_t)node->hcnt[hidx]) {
                /* target is not in this chain; skip past it */
                offset -= node->hcnt[hidx];
                continue;
            }
            /* walk to the target position within the chain */
            htree_elem_item *prev = NULL;
            htree_elem_item *elem = (htree_elem_item *)node->htab[hidx];
            while (offset-- > 0) {
                prev = elem;
                elem = elem->next;
            }
            if (prev != NULL) prev->next = elem->next;
            else              node->htab[hidx] = elem->next;
            node->hcnt[hidx] -= 1;
            elem->next = NULL;
            node->tot_elem_cnt -= 1;
            return elem;
        }
    }
    return NULL;
}

static htree_elem_item *do_htree_elem_unlink_by_cnt(htree_node **root_pptr,
                                                    htree_node *node,
                                                    uint32_t count,
                                                    htree_elem_item *cur,
                                                    uint32_t *fcnt,
                                                    ssize_t *htree_space_delta)
{
    for (int hidx = 0; hidx < HTREE_HASHTAB_SIZE; hidx++) {
        if (node->hcnt[hidx] == -1) {
            htree_node *child_node = (htree_node *)node->htab[hidx];
            uint32_t rcnt = (count > 0 ? (count - *fcnt) : 0);
            uint32_t before = *fcnt;
            cur = do_htree_elem_unlink_by_cnt(root_pptr, child_node, rcnt,
                                              cur, fcnt, htree_space_delta);
            node->tot_elem_cnt -= (*fcnt - before);
            if (is_leaf_node(child_node) &&
                child_node->tot_elem_cnt < (HTREE_MAX_HASHCHAIN_SIZE / 2)) {
                do_htree_node_unlink(root_pptr, node, hidx);
                space_delta_add(htree_space_delta, -(ssize_t)slabs_space_size(sizeof(htree_node)));
            }
        } else if (node->hcnt[hidx] > 0) {
            htree_elem_item *elem = (htree_elem_item *)node->htab[hidx];
            while (elem != NULL) {
                htree_elem_item *next = elem->next;
                node->htab[hidx] = next;
                node->hcnt[hidx] -= 1;
                node->tot_elem_cnt -= 1;
                elem->next = NULL;
                cur->next = elem;
                cur = elem;
                (*fcnt)++;
                elem = next;
                if (count > 0 && *fcnt >= count) break;
            }
        }
        if (count > 0 && *fcnt >= count) break;
    }
    return cur;
}

static int do_htree_elem_traverse_sampling(htree_node *node,
                                           uint32_t remain, uint32_t count,
                                           htree_elem_item **elem_array)
{
    int fcnt = 0;
    for (int hidx = 0; hidx < HTREE_HASHTAB_SIZE; hidx++) {
        if (node->hcnt[hidx] == -1) {
            htree_node *child_node = (htree_node *)node->htab[hidx];
            fcnt += do_htree_elem_traverse_sampling(child_node, remain,
                                                    count - fcnt, &elem_array[fcnt]);
            remain -= child_node->tot_elem_cnt;
        } else if (node->hcnt[hidx] > 0) {
            htree_elem_item *elem = (htree_elem_item *)node->htab[hidx];
            while (elem != NULL) {
                if ((rand() % remain) < (count - (uint32_t)fcnt)) {
                    elem_array[fcnt++] = elem;
                    if ((uint32_t)fcnt >= count) break;
                }
                remain -= 1;
                elem = elem->next;
            }
        }
        if ((uint32_t)fcnt >= count) break;
    }
    return fcnt;
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

htree_elem_item *htree_elem_unlink(htree_node **root_pptr,
                                   const void *key, uint16_t nkey,
                                   htree_ops *ops,
                                   ssize_t *htree_space_delta)
{
    if (*root_pptr == NULL) return NULL;
    if (htree_space_delta) *htree_space_delta = 0;

    uint32_t hval = (uint32_t)genhash_string_hash(key, nkey);

    /* traverse to the leaf node, tracking parent for potential merge */
    htree_node *par_node = NULL;
    int par_hidx = 0;
    htree_node *node = *root_pptr;
    int hidx;
    while (true) {
        hidx = HTREE_GET_HASHIDX(hval, node->hdepth);
        if (node->hcnt[hidx] >= 0) break;
        par_node = node;
        par_hidx = hidx;
        node = (htree_node *)node->htab[hidx];
    }

    /* find the element in the hash chain */
    htree_elem_item *prev = NULL;
    htree_elem_item *elem = (htree_elem_item *)node->htab[hidx];
    while (elem != NULL) {
        if (elem->hval == hval && htree_key_eq_raw(ops, elem, key, nkey))
            break;
        prev = elem;
        elem = elem->next;
    }
    if (elem == NULL) return NULL;

    do_htree_elem_unlink(root_pptr, node, hidx, prev, elem);

    /* merge child node into parent if empty or below merge threshold */
    if (node->tot_elem_cnt == 0 ||
        (par_node != NULL && is_leaf_node(node) &&
         node->tot_elem_cnt < (HTREE_MAX_HASHCHAIN_SIZE / 2))) {
        do_htree_node_unlink(root_pptr, par_node, par_hidx);
        space_delta_add(htree_space_delta, -(ssize_t)slabs_space_size(sizeof(htree_node)));
    }
    return elem;
}

htree_elem_item *htree_elem_unlink_at_offset(htree_node **root_pptr,
                                             uint32_t offset,
                                             ssize_t *htree_space_delta)
{
    if (*root_pptr == NULL) return NULL;
    if (htree_space_delta) *htree_space_delta = 0;

    htree_elem_item *elem = do_htree_elem_unlink_at_offset(root_pptr, *root_pptr,
                                                           offset, htree_space_delta);
    if (elem == NULL) return NULL;

    if ((*root_pptr) != NULL && (*root_pptr)->tot_elem_cnt == 0) {
        do_htree_node_unlink(root_pptr, NULL, 0);
        space_delta_add(htree_space_delta, -(ssize_t)slabs_space_size(sizeof(htree_node)));
    }
    return elem;
}

htree_elem_item *htree_elem_unlink_by_cnt(htree_node **root_pptr,
                                          uint32_t count,
                                          ssize_t *htree_space_delta)
{
    if (*root_pptr == NULL) return NULL;
    if (htree_space_delta) *htree_space_delta = 0;

    htree_elem_item dummy = {0};
    uint32_t fcnt = 0;
    do_htree_elem_unlink_by_cnt(root_pptr, *root_pptr, count,
                                &dummy, &fcnt, htree_space_delta);

    if (*root_pptr != NULL && (*root_pptr)->tot_elem_cnt == 0) {
        do_htree_node_unlink(root_pptr, NULL, 0);
        space_delta_add(htree_space_delta, -(ssize_t)slabs_space_size(sizeof(htree_node)));
    }
    return dummy.next;
}

htree_elem_item *htree_elem_get_at_offset(htree_node *node, uint32_t offset)
{
    for (int hidx = 0; hidx < HTREE_HASHTAB_SIZE; hidx++) {
        if (node->hcnt[hidx] == -1) {
            htree_node *child_node = (htree_node *)node->htab[hidx];
            if (offset >= child_node->tot_elem_cnt) {
                offset -= child_node->tot_elem_cnt;
                continue;
            }
            return htree_elem_get_at_offset(child_node, offset);
        } else if (node->hcnt[hidx] > 0) {
            if (offset >= (uint32_t)node->hcnt[hidx]) {
                offset -= node->hcnt[hidx];
                continue;
            }
            htree_elem_item *elem = (htree_elem_item *)node->htab[hidx];
            while (offset > 0) { elem = elem->next; offset -= 1; }
            return elem;
        }
    }
    return NULL;
}

uint32_t htree_elem_get_by_cnt(htree_node **root_pptr,
                               uint32_t count,
                               htree_elem_item **elem_array,
                               bool unlink,
                               ssize_t *htree_space_delta)
{
    assert(elem_array != NULL);
    if (*root_pptr == NULL) return 0;

    if (!unlink) return do_htree_elem_get_by_cnt(*root_pptr, count, elem_array, 0);

    htree_elem_item *head = htree_elem_unlink_by_cnt(root_pptr, count, htree_space_delta);
    uint32_t i = 0;
    for (htree_elem_item *f = head; f != NULL; f = f->next)
        elem_array[i++] = f;
    return i;
}

int htree_elem_get_rand(htree_node *node,
                        uint32_t total_count, uint32_t count,
                        htree_elem_item **elem_array)
{
    int fcnt = 0;

    if (count <= total_count / 10) {
        /* sparse: offset dedup hash table */
        int capacity = (int)(1.3 * (double)count);
        typedef struct { int cnt; int keys[3]; } _bucket;
        _bucket *buckets = calloc(capacity, sizeof(_bucket));
        if (buckets == NULL) return 0;
        while ((uint32_t)fcnt < count) {
            int rand_offset = rand() % (int)total_count;
            int idx = rand_offset % capacity;
            _bucket *b = &buckets[idx];
            bool dup = false;
            for (int i = 0; i < b->cnt; i++) {
                if (b->keys[i] == rand_offset) { dup = true; break; }
            }
            if (!dup && b->cnt < 3) {
                b->keys[b->cnt++] = rand_offset;
                htree_elem_item *found = htree_elem_get_at_offset(node, rand_offset);
                assert(found != NULL);
                elem_array[fcnt++] = found;
            }
        }
        free(buckets);
    } else {
        /* dense: reservoir sampling + Fisher-Yates shuffle */
        fcnt = do_htree_elem_traverse_sampling(node, total_count, count, elem_array);
        for (int i = fcnt - 1; i > 0; i--) {
            int rand_idx = rand() % (i + 1);
            if (rand_idx != i) {
                htree_elem_item *tmp = elem_array[i];
                elem_array[i] = elem_array[rand_idx];
                elem_array[rand_idx] = tmp;
            }
        }
    }
    return fcnt;
}
