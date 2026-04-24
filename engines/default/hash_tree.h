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

typedef struct _htree_elem_item {
    uint16_t refcount;
    uint8_t  slabs_clsid;
    uint8_t  status;
    uint32_t hval;
    struct _htree_elem_item *next;
    uint16_t nkey;          /* bytes of data[] used as lookup key */
    uint16_t nbytes;        /* total bytes in data[] */
    unsigned char data[];   /* offsetof = 20 */
} htree_elem_item;

typedef struct _htree_node {
    uint16_t refcount;
    uint8_t  slabs_clsid;
    uint8_t  hdepth;
    uint32_t tot_elem_cnt;
    int16_t  hcnt[HTREE_HASHTAB_SIZE];
    void    *htab[HTREE_HASHTAB_SIZE];
} htree_node;

typedef void (*htree_elem_unlink_func)(void *meta, htree_elem_item *elem);

htree_elem_item *htree_elem_alloc(uint16_t nkey, uint16_t nbytes, const void *cookie);

void htree_elem_free(htree_elem_item *elem);

void htree_elem_release(htree_elem_item *elem);

uint32_t htree_elem_ntotal(htree_elem_item *elem);

ENGINE_ERROR_CODE htree_elem_update(htree_node      **root_pptr,
                                    htree_elem_item  *elem,
                                    bool              is_sticky,
                                    htree_elem_item **old_elem_out,
                                    ssize_t          *space_delta_out);

ENGINE_ERROR_CODE htree_elem_insert(htree_node      **root_pptr,
                                    htree_elem_item  *elem,
                                    bool              replace_if_exist,
                                    bool              is_sticky,
                                    int               max_count,
                                    htree_elem_item **old_elem_out,
                                    ssize_t          *space_delta_out,
                                    const void       *cookie);

int htree_elem_traverse_rand(htree_node            **root_pptr,
                              htree_node             *node,
                              uint32_t                total_count,
                              uint32_t                count,
                              bool                    delete,
                              htree_elem_item       **elem_array,
                              htree_elem_unlink_func  unlink_fn,
                              void                   *meta,
                              ssize_t                *space_delta_out);

bool htree_elem_traverse_dfs_bykey(htree_node            **root_pptr,
                                    htree_node             *node,
                                    uint16_t                nkey,
                                    const unsigned char    *data,
                                    bool                    delete,
                                    htree_elem_item       **elem_out,
                                    htree_elem_unlink_func  unlink_fn,
                                    void                   *meta,
                                    ssize_t                *space_delta_out);

int htree_elem_traverse_dfs_bycnt(htree_node            **root_pptr,
                                   htree_node             *node,
                                   uint32_t                count,
                                   bool                    delete,
                                   htree_elem_item       **elem_array,
                                   htree_elem_unlink_func  unlink_fn,
                                   void                   *meta,
                                   ssize_t                *space_delta_out);

#endif
