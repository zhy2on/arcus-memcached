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

static void do_htree_elem_unlink(htree_node *node, int hidx,
                                 htree_elem_item *prev, htree_elem_item *elem,
                                 ssize_t *space_delta_out)
{
    if (prev != NULL) prev->next = elem->next;
    else              node->htab[hidx] = elem->next;
    elem->status      = ELEM_STATUS_UNLINKED;
    node->hcnt[hidx] -= 1;
    node->tot_elem_cnt -= 1;

    if (space_delta_out)
        *space_delta_out = -(ssize_t)slabs_space_size(
                               offsetof(htree_elem_item, data) + elem->nbytes);
}

static void do_htree_node_unlink(htree_node **root_pptr,
                                 htree_node *par_node, const int par_hidx,
                                 ssize_t *space_delta_out)
{
    htree_node *node;

    if (par_node == NULL) {
        node = *root_pptr;
        *root_pptr = NULL;
        assert(node->tot_elem_cnt == 0);
    } else {
        assert(par_node->hcnt[par_hidx] == -1); /* child hash node */
        htree_elem_item *head = NULL;
        htree_elem_item *elem;
        int hidx, fcnt = 0;

        node = (htree_node *)par_node->htab[par_hidx];

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
        assert(fcnt == (int)node->tot_elem_cnt);
        node->tot_elem_cnt = 0;

        par_node->htab[par_hidx] = head;
        par_node->hcnt[par_hidx] = fcnt;
    }

    if (space_delta_out)
        *space_delta_out = -(ssize_t)slabs_space_size(sizeof(htree_node));

    do_htree_node_free(node);
}

htree_elem_item *htree_elem_alloc(uint16_t nkey, uint16_t nbytes, const void *cookie)
{
    size_t ntotal = offsetof(htree_elem_item, data) + nbytes;
    htree_elem_item *elem = do_item_mem_alloc(ntotal, LRU_CLSID_FOR_SMALL, cookie);
    if (elem != NULL) {
        elem->slabs_clsid = slabs_clsid(ntotal);
        elem->refcount    = 0;
        elem->status      = ELEM_STATUS_UNLINKED;
        elem->nkey        = nkey;
        elem->nbytes      = nbytes;
    }
    return elem;
}

void htree_elem_free(htree_elem_item *elem)
{
    do_item_mem_free(elem, offsetof(htree_elem_item, data) + elem->nbytes);
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

uint32_t htree_elem_ntotal(htree_elem_item *elem)
{
    return offsetof(htree_elem_item, data) + elem->nbytes;
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

bool htree_elem_find(htree_node *root,
                     uint16_t nkey, const unsigned char *data,
                     htree_find_result *result_out)
{
    if (root == NULL)
        return false;

    uint32_t hval = genhash_string_hash(data, nkey);
    htree_node      *node = root;
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
        if (find->hval == hval && find->nkey == nkey &&
            memcmp(find->data, data, nkey) == 0)
            break;
        prev = find;
    }

    if (find == NULL)
        return false;

    if (result_out != NULL) {
        result_out->node = node;
        result_out->hidx = hidx;
        result_out->prev = prev;
        result_out->elem = find;
    }
    return true;
}

void htree_elem_replace_at(htree_node **root_pptr,
                           htree_find_result *result,
                           htree_elem_item *elem,
                           ssize_t *space_delta_out)
{
    htree_elem_item *find = result->elem;

    elem->hval   = find->hval;
    elem->next   = find->next;
    if (result->prev != NULL)
        result->prev->next = elem;
    else
        result->node->htab[result->hidx] = elem;
    elem->status  = ELEM_STATUS_LINKED;
    find->status  = ELEM_STATUS_UNLINKED;

    if (space_delta_out) {
        size_t new_ntotal = offsetof(htree_elem_item, data) + elem->nbytes;
        size_t old_ntotal = offsetof(htree_elem_item, data) + find->nbytes;
        *space_delta_out = (ssize_t)slabs_space_size(new_ntotal)
                         - (ssize_t)slabs_space_size(old_ntotal);
    }
}

static void do_htree_elem_link(htree_node **root_pptr,
                               htree_node *node, const int hidx,
                               htree_elem_item *elem)
{
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
}

ENGINE_ERROR_CODE htree_elem_insert(htree_node **root_pptr,
                                    htree_elem_item *elem,
                                    ssize_t *space_delta_out,
                                    const void *cookie)
{
    if (space_delta_out) *space_delta_out = 0;
    bool new_root = false;

    elem->hval = genhash_string_hash(elem->data, elem->nkey);

    /* allocate root node if the tree is empty */
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
        if (find->hval == elem->hval && find->nkey == elem->nkey &&
            memcmp(find->data, elem->data, elem->nkey) == 0) {
            if (new_root) {
                do_htree_node_free(*root_pptr);
                *root_pptr = NULL;
            }
            return ENGINE_ELEM_EEXISTS;
        }
    }

    /* split the hash chain into a new child node if it is full */
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

    do_htree_elem_link(root_pptr, node, hidx, elem);

    if (space_delta_out) {
        size_t new_ntotal = offsetof(htree_elem_item, data) + elem->nbytes;
        *space_delta_out = (ssize_t)slabs_space_size(new_ntotal);
        if (node_created)
            *space_delta_out += (ssize_t)slabs_space_size(sizeof(htree_node));
    }
    return ENGINE_SUCCESS;
}

static htree_elem_item *do_htree_elem_at_offset(htree_node **root_pptr,
                                                htree_node *node,
                                                uint32_t offset,
                                                bool delete,
                                                htree_elem_unlink_func unlink_fn,
                                                void *meta,
                                                ssize_t *space_delta_out)
{
    for (int hidx = 0; hidx < HTREE_HASHTAB_SIZE; hidx++) {
        if (node->hcnt[hidx] == -1) {
            htree_node *child_node = (htree_node *)node->htab[hidx];
            if (offset >= child_node->tot_elem_cnt) {
                offset -= child_node->tot_elem_cnt;
                continue;
            }
            htree_elem_item *found = do_htree_elem_at_offset(root_pptr, child_node,
                                                             offset, delete,
                                                             unlink_fn, meta,
                                                             space_delta_out);
            if (delete) {
                if (is_leaf_node(child_node) &&
                    child_node->tot_elem_cnt < (HTREE_MAX_HASHCHAIN_SIZE / 2)) {
                    ssize_t node_delta = 0;
                    do_htree_node_unlink(root_pptr, node, hidx, &node_delta);
                    if (space_delta_out) *space_delta_out += node_delta;
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
            htree_elem_item *elem = (htree_elem_item *)node->htab[hidx];
            while (offset > 0) {
                prev = elem;
                elem = elem->next;
                offset -= 1;
            }
            elem->refcount++;
            if (delete) {
                ssize_t delta = 0;
                do_htree_elem_unlink(node, hidx, prev, elem, &delta);
                if (space_delta_out) *space_delta_out += delta;
                if (unlink_fn != NULL)
                    unlink_fn(meta, elem);
                if (elem->refcount == 0)
                    htree_elem_free(elem);
            }
            return elem;
        }
    }
    return NULL;
}

static int do_htree_elem_traverse_sampling(htree_node *node,
                                           uint32_t remain,
                                           uint32_t count,
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
                    elem->refcount++;
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

int htree_elem_traverse_rand(htree_node **root_pptr,
                             htree_node *node,
                             uint32_t total_count,
                             uint32_t count,
                             bool delete,
                             htree_elem_item **elem_array,
                             htree_elem_unlink_func unlink_fn,
                             void *meta,
                             ssize_t *space_delta_out)
{
    if (space_delta_out) *space_delta_out = 0;
    int fcnt = 0;

    if (delete) {
        while ((uint32_t)fcnt < count) {
            uint32_t rand_offset = (uint32_t)(rand() % (int)total_count);
            htree_elem_item *found = do_htree_elem_at_offset(root_pptr, node,
                                                             rand_offset, delete,
                                                             unlink_fn, meta,
                                                             space_delta_out);
            assert(found != NULL);
            elem_array[fcnt++] = found;
            total_count--;
        }
        if (*root_pptr != NULL && (*root_pptr)->tot_elem_cnt == 0) {
            ssize_t node_delta = 0;
            do_htree_node_unlink(root_pptr, NULL, 0, &node_delta);
            if (space_delta_out) *space_delta_out += node_delta;
        }
    } else if (count <= total_count / 10) {
        /* sparse: use offset dedup hash table */
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
                htree_elem_item *found = do_htree_elem_at_offset(root_pptr, node,
                                                                 rand_offset, false,
                                                                 NULL, NULL, NULL);
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

static bool do_htree_elem_traverse_bykey(htree_node **root_pptr,
                                         htree_node *node,
                                         uint32_t hval,
                                         uint16_t nkey,
                                         const unsigned char *data,
                                         bool delete,
                                         htree_elem_item **elem_out,
                                         htree_elem_unlink_func unlink_fn,
                                         void *meta,
                                         ssize_t *space_delta_out)
{
    int hidx = HTREE_GET_HASHIDX(hval, node->hdepth);

    if (node->hcnt[hidx] == -1) {
        htree_node *child_node = (htree_node *)node->htab[hidx];
        bool found = do_htree_elem_traverse_bykey(root_pptr, child_node,
                                                  hval, nkey, data, delete,
                                                  elem_out, unlink_fn, meta,
                                                  space_delta_out);
        if (found && delete) {
            if (is_leaf_node(child_node) &&
                child_node->tot_elem_cnt < (HTREE_MAX_HASHCHAIN_SIZE / 2)) {
                ssize_t node_delta = 0;
                do_htree_node_unlink(root_pptr, node, hidx, &node_delta);
                if (space_delta_out) *space_delta_out += node_delta;
            }
            node->tot_elem_cnt -= 1;
        }
        return found;
    }

    if (node->hcnt[hidx] > 0) {
        htree_elem_item *prev = NULL;
        htree_elem_item *elem = (htree_elem_item *)node->htab[hidx];
        while (elem != NULL) {
            if (elem->hval == hval && elem->nkey == nkey &&
                memcmp(elem->data, data, nkey) == 0) {
                if (elem_out) {
                    elem->refcount++;
                    *elem_out = elem;
                }
                if (delete) {
                    ssize_t delta = 0;
                    do_htree_elem_unlink(node, hidx, prev, elem, &delta);
                    if (space_delta_out) *space_delta_out += delta;
                    if (unlink_fn != NULL)
                        unlink_fn(meta, elem);
                    if (elem->refcount == 0)
                        htree_elem_free(elem);
                }
                return true;
            }
            prev = elem;
            elem = elem->next;
        }
    }
    return false;
}

bool htree_elem_delete(htree_node **root_pptr,
                       uint16_t nkey, const unsigned char *data,
                       htree_elem_item **elem_out,
                       ssize_t *space_delta_out)
{
    if (*root_pptr == NULL) return false;
    if (space_delta_out) *space_delta_out = 0;

    uint32_t hval = genhash_string_hash(data, nkey);

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

    htree_elem_item *prev = NULL;
    htree_elem_item *elem = (htree_elem_item *)node->htab[hidx];
    while (elem != NULL) {
        if (elem->hval == hval && elem->nkey == nkey &&
            memcmp(elem->data, data, nkey) == 0)
            break;
        prev = elem;
        elem = elem->next;
    }
    if (elem == NULL) return false;

    if (elem_out) {
        elem->refcount++;
        *elem_out = elem;
    }
    ssize_t delta = 0;
    do_htree_elem_unlink(node, hidx, prev, elem, &delta);
    if (space_delta_out) *space_delta_out += delta;
    if (elem->refcount == 0)
        htree_elem_free(elem);

    if (node->tot_elem_cnt == 0) {
        ssize_t node_delta = 0;
        do_htree_node_unlink(root_pptr, par_node, par_node ? par_hidx : 0, &node_delta);
        if (space_delta_out) *space_delta_out += node_delta;
    } else if (par_node != NULL &&
               is_leaf_node(node) &&
               node->tot_elem_cnt < (HTREE_MAX_HASHCHAIN_SIZE / 2)) {
        ssize_t node_delta = 0;
        do_htree_node_unlink(root_pptr, par_node, par_hidx, &node_delta);
        if (space_delta_out) *space_delta_out += node_delta;
    }
    return true;
}

bool htree_elem_traverse_dfs_bykey(htree_node **root_pptr,
                                   htree_node *node,
                                   uint16_t nkey,
                                   const unsigned char *data,
                                   bool delete,
                                   htree_elem_item **elem_out,
                                   htree_elem_unlink_func unlink_fn,
                                   void *meta,
                                   ssize_t *space_delta_out)
{
    if (space_delta_out) *space_delta_out = 0;
    uint32_t hval = genhash_string_hash(data, nkey);
    bool found = do_htree_elem_traverse_bykey(root_pptr, node, hval, nkey, data,
                                              delete, elem_out, unlink_fn, meta,
                                              space_delta_out);
    if (found && delete && *root_pptr != NULL && (*root_pptr)->tot_elem_cnt == 0) {
        ssize_t node_delta = 0;
        do_htree_node_unlink(root_pptr, NULL, 0, &node_delta);
        if (space_delta_out) *space_delta_out += node_delta;
    }
    return found;
}

int htree_elem_traverse_dfs_bycnt(htree_node **root_pptr,
                                  htree_node *node,
                                  uint32_t count,
                                  bool delete,
                                  htree_elem_item **elem_array,
                                  htree_elem_unlink_func unlink_fn,
                                  void *meta,
                                  ssize_t *space_delta_out)
{
    if (space_delta_out) *space_delta_out = 0;
    int hidx;
    int fcnt = 0;

    for (hidx = 0; hidx < HTREE_HASHTAB_SIZE; hidx++) {
        if (node->hcnt[hidx] == -1) {
            htree_node *child_node = (htree_node *)node->htab[hidx];
            uint32_t rcnt = (count > 0 ? (count - (uint32_t)fcnt) : 0);
            int ecnt = htree_elem_traverse_dfs_bycnt(root_pptr, child_node, rcnt, delete,
                                                     (elem_array == NULL ? NULL : &elem_array[fcnt]),
                                                     unlink_fn, meta, space_delta_out);
            fcnt += ecnt;
            if (delete) {
                if (is_leaf_node(child_node) &&
                    child_node->tot_elem_cnt < (HTREE_MAX_HASHCHAIN_SIZE / 2)) {
                    ssize_t node_delta = 0;
                    do_htree_node_unlink(root_pptr, node, hidx, &node_delta);
                    if (space_delta_out) *space_delta_out += node_delta;
                }
                node->tot_elem_cnt -= ecnt;
            }
        } else if (node->hcnt[hidx] > 0) {
            htree_elem_item *elem = (htree_elem_item *)node->htab[hidx];
            while (elem != NULL) {
                if (elem_array) {
                    elem->refcount++;
                    elem_array[fcnt] = elem;
                }
                fcnt++;
                if (delete) {
                    htree_elem_item *next = elem->next;
                    ssize_t delta = 0;
                    do_htree_elem_unlink(node, hidx, NULL, elem, &delta);
                    if (space_delta_out) *space_delta_out += delta;
                    if (unlink_fn != NULL) {
                        unlink_fn(meta, elem);
                    }
                    if (elem->refcount == 0)
                        htree_elem_free(elem);
                    elem = next;
                } else {
                    elem = elem->next;
                }
                if (count > 0 && fcnt >= (int)count) break;
            }
        }
        if (count > 0 && fcnt >= (int)count) break;
    }
    if (delete && node == *root_pptr && node->tot_elem_cnt == 0) {
        ssize_t node_delta = 0;
        do_htree_node_unlink(root_pptr, NULL, 0, &node_delta);
        if (space_delta_out) *space_delta_out += node_delta;
    }
    return fcnt;
}

