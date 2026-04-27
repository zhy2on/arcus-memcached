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
#ifndef HASH_TREE_H
#define HASH_TREE_H

#include <stdint.h>
#include <stdbool.h>
#include <sys/types.h>
#include <memcached/engine.h>

#define HTREE_HASHTAB_SIZE       16
#define HTREE_HASHIDX_MASK       0x0000000F
#define HTREE_MAX_HASHCHAIN_SIZE 64

#define HTREE_GET_HASHIDX(hval, hdepth) \
    (((hval) & (HTREE_HASHIDX_MASK << ((hdepth)*4))) >> ((hdepth)*4))

/* Base type for hash_tree to cast elem items.
 * Each collection defines its own elem struct whose leading fields match
 * this layout exactly.  data[] holds <key, value> where the first nkey
 * bytes are the lookup key (used for hval computation and match).
 * For set: nkey == nbytes (the whole value is the key).
 * For map: nkey == field length, nbytes == field + value length. */
typedef struct {
    uint16_t refcount;
    uint8_t  slabs_clsid;
    uint8_t  status;
    uint32_t hval;
    void    *next;
    uint16_t nkey;          /* bytes of data[] used as lookup key */
    uint16_t nbytes;        /* total bytes in data[] */
    unsigned char data[];   /* offsetof = 20 */
} htree_elem_item;

/* Fixed internal node type — managed entirely by hash_tree.
 * Collections reference this via htree_node *root in their meta_info. */
typedef struct _htree_node {
    uint16_t refcount;
    uint8_t  slabs_clsid;
    uint8_t  hdepth;
    uint32_t tot_elem_cnt;
    int16_t  hcnt[HTREE_HASHTAB_SIZE];
    void    *htab[HTREE_HASHTAB_SIZE];
} htree_node;

/* Insert elem into the hash tree rooted at *root_pptr.
 *
 * Precondition: elem->nkey and elem->nbytes must be set by the caller.
 * hval is computed internally from data[:nkey].
 *
 * If replace_if_exist is true and a matching elem is found, the old elem is
 * unlinked and returned via *old_elem_out (if non-NULL); the caller is
 * responsible for CLOG, ccnt update, and freeing the old elem.
 *
 * If replace_if_exist is false and a duplicate is found, ENGINE_ELEM_EEXISTS
 * is returned.
 *
 * The caller is responsible for sticky/overflow checks and CLOG on success.
 * ccnt must be incremented by the caller only when old_elem_out is NULL
 * (i.e., a new elem was inserted, not a replacement).
 *
 * On ENGINE_SUCCESS, *space_delta_out (if non-NULL) is set to the net slab
 * space change: slabs_space_size(new_elem) - slabs_space_size(old_elem if any)
 * + slabs_space_size(new_node if any).  Positive means space grew, negative
 * means space shrank (replace with smaller elem).  On any error the value is
 * undefined.
 *
 * Returns ENGINE_SUCCESS, ENGINE_ENOMEM, or ENGINE_ELEM_EEXISTS. */
ENGINE_ERROR_CODE htree_elem_insert(htree_node      **root_pptr,
                                    htree_elem_item  *elem,
                                    bool              replace_if_exist,
                                    htree_elem_item **old_elem_out,
                                    ssize_t          *space_delta_out,
                                    const void       *cookie);

#endif
