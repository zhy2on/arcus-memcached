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

htree_hash_node *do_htree_node_alloc(uint8_t hash_depth, const void *cookie);
void             do_htree_node_free(htree_hash_node *node);
bool             do_htree_node_is_leaf(const htree_hash_node *node);
void             do_htree_node_link(htree_hash_node **root,
                                    htree_hash_node *par_node, int par_hidx,
                                    htree_hash_node *node);
void             do_htree_node_unlink(htree_hash_node **root,
                                      htree_hash_node *par_node, int par_hidx);

#endif /* HASH_TREE_H */
