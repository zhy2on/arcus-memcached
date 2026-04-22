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
