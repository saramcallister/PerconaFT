/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: ft=cpp:expandtab:ts=8:sw=4:softtabstop=4:
#ident "$Id$"
/*======
This file is part of PerconaFT.


Copyright (c) 2006, 2015, Percona and/or its affiliates. All rights reserved.

    PerconaFT is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License, version 2,
    as published by the Free Software Foundation.

    PerconaFT is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with PerconaFT.  If not, see <http://www.gnu.org/licenses/>.

----------------------------------------

    PerconaFT is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License, version 3,
    as published by the Free Software Foundation.

    PerconaFT is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with PerconaFT.  If not, see <http://www.gnu.org/licenses/>.
======= */

#ident "Copyright (c) 2006, 2015, Percona and/or its affiliates. All rights reserved."

#include <algorithm>

#include <string.h>

#include "portability/memory.h"
#include "portability/toku_assert.h"
#include "portability/toku_stdint.h"
#include "portability/toku_stdlib.h"

#include "ft/serialize/block_allocator.h"
#include "ft/serialize/block_allocator_strategy.h"
#include "ft/serialize/tree_block_allocator.h"

#if TOKU_DEBUG_PARANOID
#define VALIDATE() validate()
#else
#define VALIDATE()
#endif

extern FILE *ba_trace_file;

void tree_block_allocator::_create_internal(uint64_t reserve_at_beginning, uint64_t alignment) {
    // the alignment must be at least 512 and aligned with 512 to work with direct I/O
    assert(alignment >= 512 && (alignment % 512) == 0);

    _reserve_at_beginning = reserve_at_beginning;
    _alignment = alignment;
    _n_blocks = 0;
    _n_bytes_in_use = reserve_at_beginning;
    _tree = new rbtree_mhs(alignment);
    memset(&_trace_lock, 0, sizeof(toku_mutex_t));
    toku_mutex_init(&_trace_lock, nullptr);

    VALIDATE();
}

void tree_block_allocator::create(uint64_t reserve_at_beginning, uint64_t
                                  alignment) {
    _create_internal(reserve_at_beginning, alignment);
    _tree->insert({reserve_at_beginning, MAX_BYTE}); 
    _trace_create();
}

void tree_block_allocator::destroy() {
    delete _tree;
    _trace_destroy();
    toku_mutex_destroy(&_trace_lock);
}

void tree_block_allocator::create_from_blockpairs(uint64_t reserve_at_beginning, uint64_t alignment,
                                             struct blockpair * translation_pairs, uint64_t
                                             n_blocks) {
    _create_internal(reserve_at_beginning, alignment);
    _n_blocks = n_blocks;

    struct blockpair * XMALLOC_N(n_blocks, pairs);
    memcpy(pairs, translation_pairs, n_blocks * sizeof(struct blockpair));
    std::sort(pairs, pairs + n_blocks); 
    
    for (uint64_t i = 0; i < _n_blocks; i++) {
        // Allocator does not support size 0 blocks. See block_allocator_free_block.
        invariant(pairs[i].size > 0);
        invariant(pairs[i].offset >= _reserve_at_beginning);
        invariant(pairs[i].offset % _alignment == 0);

        _n_bytes_in_use += pairs[i].size;
    
        uint64_t free_offset = 0;
        uint64_t free_size = MAX_BYTE;
    
        free_offset = pairs[i].offset+pairs[i].size;
        if(i < n_blocks -1 ){
            assert(pairs[i+1].offset >= (pairs[i].offset+pairs[i].size));
            free_size = pairs[i+1].offset - (pairs[i].offset + pairs[i].size);
            if(!free_size) 
                continue;
        }
        _tree->insert({free_offset, free_size});
      
    }
    toku_free(pairs);
    VALIDATE();

    _trace_create_from_blockpairs();
}

// Effect: align a value by rounding up.
static inline uint64_t align(uint64_t value, uint64_t ba_alignment) {
    return ((value + ba_alignment - 1) / ba_alignment) * ba_alignment;
}


// Effect: Allocate a block. The resulting block must be aligned on the ba->alignment (which to make direct_io happy must be a positive multiple of 512).
void tree_block_allocator::alloc_block(uint64_t size, uint64_t heat, uint64_t *offset) {

    // Allocator does not support size 0 blocks. See block_allocator_free_block.
    invariant(size > 0);

    _n_bytes_in_use += size;
    *offset = _tree->remove(size);

    _n_blocks++;
    VALIDATE();

    _trace_alloc(size, heat, *offset);
}

// To support 0-sized blocks, we need to include size as an input to this function.
// All 0-sized blocks at the same offset can be considered identical, but
// a 0-sized block can share offset with a non-zero sized block.
// The non-zero sized block is not exchangable with a zero sized block (or vice versa),
// so inserting 0-sized blocks can cause corruption here.
void tree_block_allocator::free_block(uint64_t offset, uint64_t size) {
    VALIDATE();
    _n_bytes_in_use -= size;
    _tree->insert({offset, size});
    _n_blocks--;
    VALIDATE();
    
    _trace_free(offset);
}

uint64_t tree_block_allocator::allocated_limit() const {
    rbtnode_mhs * max_node = _tree->max_node();
    return rbn_offset(max_node);
}

// Effect: Consider the blocks in sorted order.  The reserved block at the beginning is number 0.  The next one is number 1 and so forth.
// Return the offset and size of the block with that number.
// Return 0 if there is a block that big, return nonzero if b is too big.
int tree_block_allocator::get_nth_block_in_layout_order(uint64_t b, uint64_t *offset, uint64_t *size) {
    rbtnode_mhs * x, *y;
    if(b == 0) {
        *offset = 0;
        *size = _reserve_at_beginning;
        return 0;
    } else if(b> _n_blocks){
        return -1;
    } else {
        x = _tree->min_node();  
        for(uint64_t i=1; i<= b; i++) {
            y = x;
            x = _tree->successor(x);
        }
        *size = rbn_offset(x) - (rbn_offset(y) + rbn_size(y));
        *offset = rbn_offset(y) + rbn_size(y); 
        return 0;
    }

}

uint64_t tree_block_allocator:: get_alignment() {
    return _alignment;
}

static void vis_unused_collector(void * extra, rbtnode_mhs *node, uint64_t
                                 UU(depth)) {
  
    TOKU_DB_FRAGMENTATION report = (TOKU_DB_FRAGMENTATION) extra;
    uint64_t offset = rbn_offset(node);
    uint64_t size = rbn_size(node);
    uint64_t answer_offset = align(offset, tree_block_allocator::get_alignment());
    uint64_t free_space = offset + size - answer_offset;
    if(free_space > 0) {
        report->unused_bytes += free_space;
        report->unused_blocks ++;
        if (free_space > report->largest_unused_block) {
                    report->largest_unused_block = free_space;
        }
         
    }
}
// Requires: report->file_size_bytes is filled in
// Requires: report->data_bytes is filled in
// Requires: report->checkpoint_bytes_additional is filled in
void tree_block_allocator::get_unused_statistics(TOKU_DB_FRAGMENTATION report) {
    assert(_n_bytes_in_use == report->data_bytes + report->checkpoint_bytes_additional);

    report->unused_bytes = 0;
    report->unused_blocks = 0;
    report->largest_unused_block = 0;
    _tree->in_order_visitor(vis_unused_collector, report);
}

void tree_block_allocator::get_statistics(TOKU_DB_FRAGMENTATION report) {
    report->data_bytes = _n_bytes_in_use; 
    report->data_blocks = _n_blocks; 
    report->file_size_bytes = 0;
    report->checkpoint_bytes_additional = 0;
    get_unused_statistics(report);
}

struct validate_extra {
    uint64_t n_bytes;
    rbtnode_mhs * pre_node ;
};
static void vis_used_blocks_in_order(void *extra, rbtnode_mhs * cur_node, uint64_t
                                     UU(depth)) {
    struct validate_extra * v_e = (struct validate_extra *) extra;
    rbtnode_mhs * pre_node = v_e -> pre_node;
    //verify no overlaps
    if(pre_node) {
        assert(rbn_size(pre_node) > 0);
        assert(rbn_offset(cur_node) > rbn_offset(pre_node) +
              rbn_size(pre_node));
        uint64_t used_space = rbn_offset(cur_node) -
            (rbn_offset(pre_node)+rbn_size(pre_node));       
        v_e->n_bytes += used_space;
    } 
    v_e->pre_node = cur_node;
}

void tree_block_allocator::validate() const {
    _tree->validate_balance();
    struct validate_extra * XMALLOC(extra);
    extra->n_bytes = 0;
    extra->pre_node = NULL;
    _tree->in_order_visitor(vis_used_blocks_in_order, extra);
    assert(extra->n_bytes == _n_bytes_in_use);
    toku_free(extra);
}

// Tracing

void tree_block_allocator::_trace_create(void) {
    if (ba_trace_file != nullptr) {
        toku_mutex_lock(&_trace_lock);
        fprintf(ba_trace_file, "ba_trace_create %p %" PRIu64 " %" PRIu64 "\n",
                this, _reserve_at_beginning, _alignment);
        toku_mutex_unlock(&_trace_lock);

        fflush(ba_trace_file);
    }
}

static void vis_print_blocks(void * extra, rbtnode_mhs * cur_node, uint64_t
                             UU(depth)) {
   
    rbtnode_mhs ** p_pre_node = (rbtnode_mhs **) extra;
    if(*p_pre_node) {
        uint64_t blk_offset = rbn_offset(*p_pre_node) + rbn_offset(*p_pre_node);
        uint64_t blk_size = rbn_offset(cur_node) - blk_offset; 
        fprintf(ba_trace_file, "[%" PRIu64 " %" PRIu64 "] ", blk_offset, blk_size);
    }
    *p_pre_node = cur_node;
}
void tree_block_allocator::_trace_create_from_blockpairs(void) {
    if (ba_trace_file != nullptr) {
        toku_mutex_lock(&_trace_lock);
        fprintf(ba_trace_file, "ba_trace_create_from_blockpairs %p %" PRIu64 " %" PRIu64 " ",
                this, _reserve_at_beginning, _alignment);

        rbtnode_mhs * pre_node =NULL;
        _tree->in_order_visitor(vis_print_blocks, &pre_node); 
        fprintf(ba_trace_file, "\n");
        
        toku_mutex_unlock(&_trace_lock);
        fflush(ba_trace_file);
    }
}

void tree_block_allocator::_trace_destroy(void) {
    if (ba_trace_file != nullptr) {
        toku_mutex_lock(&_trace_lock);
        fprintf(ba_trace_file, "ba_trace_destroy %p\n", this);
        toku_mutex_unlock(&_trace_lock);

        fflush(ba_trace_file);
    }
}

void tree_block_allocator::_trace_alloc(uint64_t size, uint64_t heat, uint64_t offset) {
    if (ba_trace_file != nullptr) {
        toku_mutex_lock(&_trace_lock);
        fprintf(ba_trace_file, "ba_trace_alloc %p %" PRIu64 " %" PRIu64 " %" PRIu64 "\n",
                this, size, heat, offset);
        toku_mutex_unlock(&_trace_lock);

        fflush(ba_trace_file);
    }
}

void tree_block_allocator::_trace_free(uint64_t offset) {
    if (ba_trace_file != nullptr) {
        toku_mutex_lock(&_trace_lock);
        fprintf(ba_trace_file, "ba_trace_free %p %" PRIu64 "\n", this, offset);
        toku_mutex_unlock(&_trace_lock);

        fflush(ba_trace_file);
    }
}
