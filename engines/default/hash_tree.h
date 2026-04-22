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
#ifndef HASH_TREE_H
#define HASH_TREE_H

#include "item_base.h"

typedef struct {
    htree_hash_node *node;
    htree_elem_item *prev;
    uint16_t         hidx;
} htree_prev_info;

htree_hash_node  *do_htree_node_alloc(uint8_t hash_depth, const void *cookie);
void              do_htree_node_free(htree_hash_node *node);
bool              do_htree_node_is_leaf(const htree_hash_node *node);

htree_elem_item  *do_htree_elem_alloc(uint8_t nfield, uint16_t nbytes, const void *cookie);
uint32_t          do_htree_elem_ntotal(htree_elem_item *elem);
void              do_htree_elem_free(htree_elem_item *elem);
void              do_htree_elem_release(htree_elem_item *elem);

void              do_htree_node_link(htree_hash_node **root,
                                     htree_hash_node *par_node, int par_hidx,
                                     htree_hash_node *node);
void              do_htree_node_unlink(htree_hash_node **root,
                                       htree_hash_node *par_node, int par_hidx);

htree_elem_item  *do_htree_elem_find(htree_hash_node *root,
                                     const void *key, size_t klen,
                                     htree_prev_info *pinfo);
ENGINE_ERROR_CODE do_htree_elem_link(htree_hash_node **root,
                                     htree_elem_item *elem,
                                     const void *key, size_t klen,
                                     bool replace_if_exist,
                                     htree_elem_item **old_elem,
                                     bool *node_split,
                                     const void *cookie);

#endif /* HASH_TREE_H */
