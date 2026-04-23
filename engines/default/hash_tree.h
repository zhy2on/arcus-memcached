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

htree_hash_node  *htree_node_alloc(uint8_t hash_depth, const void *cookie);

htree_elem_item  *htree_elem_alloc(uint8_t nfield, uint16_t nbytes, const void *cookie);

uint32_t          htree_elem_ntotal(htree_elem_item *elem);

void              htree_elem_free(htree_elem_item *elem);

void              htree_elem_release(htree_elem_item *elem);

/* Returns bytes added for the new node; 0 if par_node is NULL (root init). */
size_t            htree_node_link(htree_hash_node **root,
                                  htree_hash_node *par_node, int par_hidx,
                                  htree_hash_node *node);

/* Returns bytes freed for the removed node; 0 if par_node is NULL (root clear). */
size_t            htree_node_unlink(htree_hash_node **root,
                                    htree_hash_node *par_node, int par_hidx);

/* Called during delete traversals for CLOG. Space accounting is caller's responsibility. */
typedef void (*htree_elem_delete_cb)(htree_elem_item *elem,
                                     enum elem_delete_cause cause);

int               htree_traverse_sampling(htree_hash_node *node,
                                          uint32_t remain, const uint32_t count,
                                          htree_elem_item **elem_array);

htree_elem_item  *htree_elem_at_offset(htree_hash_node **root,
                                       htree_hash_node *node,
                                       uint32_t offset, const bool delete,
                                       htree_elem_delete_cb on_elem_delete,
                                       ssize_t *space_delta_out);

int               htree_traverse_dfs_bycnt(htree_hash_node **root,
                                           htree_hash_node *node,
                                           const uint32_t count, const bool delete,
                                           htree_elem_item **elem_array,
                                           htree_elem_delete_cb on_elem_delete,
                                           enum elem_delete_cause cause,
                                           ssize_t *space_delta_out);

bool              htree_traverse_dfs_byfield(htree_hash_node **root,
                                             htree_hash_node *node,
                                             const void *key, const size_t klen,
                                             const bool delete,
                                             htree_elem_item **elem_array,
                                             htree_elem_delete_cb on_elem_delete,
                                             ssize_t *space_delta_out);

htree_elem_item  *htree_elem_find(htree_hash_node *root,
                                  const void *key, size_t klen);

/* space_delta_out: bytes added (insert) or net diff (replace). NULL if caller doesn't need it.
 * replaced_out: if non-NULL and a replace occurred, set to old elem (caller handles CLOG+free). */
ENGINE_ERROR_CODE htree_elem_insert(htree_hash_node **root,
                                    htree_elem_item *elem,
                                    const void *key, size_t klen,
                                    bool replace_if_exist,
                                    htree_elem_item **replaced_out,
                                    ssize_t *space_delta_out,
                                    const void *cookie);

#endif /* HASH_TREE_H */
