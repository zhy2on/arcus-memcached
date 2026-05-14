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

#define HTREE_HASHTAB_SIZE 16

/* Layout contract for hash-tree elements.
 * Any elem_item used with hash_tree functions must begin with these
 * fields in this exact order. Fields beyond this point may differ. */
typedef struct _htree_elem_item {
    uint8_t reserved[8];           /* available for use by the caller's own header fields */
    struct _htree_elem_item *next; /* hash chain next */
    uint32_t hval;                 /* hash value */
} htree_elem_item;

typedef struct _htree_node {
    uint16_t refcount;
    uint8_t  slabs_clsid;
    uint8_t  hdepth;
    uint32_t tot_elem_cnt;
    int16_t  hcnt[HTREE_HASHTAB_SIZE];
    void    *htab[HTREE_HASHTAB_SIZE];
} htree_node;

/* ops: collection-specific key extraction */
typedef struct {
    const void *(*get_key)(const htree_elem_item *elem, uint16_t *nkey);
} htree_ops;

#endif
