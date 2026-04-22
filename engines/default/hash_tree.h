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

htree_hash_node  *htree_node_alloc(uint8_t hash_depth, const void *cookie);

void              htree_node_free(htree_hash_node *node);

bool              htree_node_is_leaf(const htree_hash_node *node);

htree_elem_item  *htree_elem_alloc(uint8_t nfield, uint16_t nbytes, const void *cookie);

uint32_t          htree_elem_ntotal(htree_elem_item *elem);

void              htree_elem_free(htree_elem_item *elem);

void              htree_elem_release(htree_elem_item *elem);

void              htree_node_insert(htree_hash_node **root,
                                    htree_hash_node *par_node, int par_hidx,
                                    htree_hash_node *node);

void              htree_node_remove(htree_hash_node **root,
                                    htree_hash_node *par_node, int par_hidx);

/* Called during delete traversals: CLOG only, space accounting is handled internally */
typedef void (*htree_elem_delete_cb)(htree_elem_item *elem,
                                     enum elem_delete_cause cause,
                                     coll_meta_info *meta);

typedef ENGINE_ERROR_CODE (*htree_pre_replace_cb)(htree_elem_item *old_elem,
                                                  htree_elem_item *new_elem,
                                                  coll_meta_info *meta);

typedef ENGINE_ERROR_CODE (*htree_pre_insert_cb)(htree_elem_item *new_elem,
                                                 coll_meta_info *meta);

int               htree_traverse_sampling(htree_hash_node *node,
                                          uint32_t remain, const uint32_t count,
                                          htree_elem_item **elem_array);

htree_elem_item  *htree_elem_at_offset(htree_hash_node **root,
                                       htree_hash_node *node,
                                       uint32_t offset, const bool delete,
                                       htree_elem_delete_cb on_elem_delete,
                                       coll_meta_info *meta);

int               htree_traverse_dfs_bycnt(htree_hash_node **root,
                                           htree_hash_node *node,
                                           const uint32_t count, const bool delete,
                                           htree_elem_item **elem_array,
                                           htree_elem_delete_cb on_elem_delete,
                                           enum elem_delete_cause cause,
                                           coll_meta_info *meta);

bool              htree_traverse_dfs_byfield(htree_hash_node **root,
                                             htree_hash_node *node,
                                             const void *key, const size_t klen,
                                             const bool delete,
                                             htree_elem_item **elem_array,
                                             htree_elem_delete_cb on_elem_delete,
                                             coll_meta_info *meta);

void              htree_elem_delete(htree_hash_node *node, const int hidx,
                                    htree_elem_item *prev, htree_elem_item *elem);

htree_elem_item  *htree_elem_find(htree_hash_node *root,
                                  const void *key, size_t klen,
                                  htree_prev_info *pinfo);

/* replaced_out: if non-NULL and a replace occurred, *replaced_out is set to the old
 * element (unlinked, refcount unchanged). Caller is responsible for CLOG and free. */
ENGINE_ERROR_CODE htree_elem_insert(htree_hash_node **root,
                                    htree_elem_item *elem,
                                    const void *key, size_t klen,
                                    bool replace_if_exist,
                                    htree_elem_item **replaced_out,
                                    htree_pre_replace_cb on_pre_replace,
                                    htree_pre_insert_cb on_pre_insert,
                                    coll_meta_info *meta, const void *cookie);

#endif /* HASH_TREE_H */
