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

#pragma once


//RBTree(Red-black tree) with max hole sizes for subtrees.

//This is a tentative data struct to improve the block allocation time
//complexity from the linear time to the log time. Please be noted this DS only
//supports first-fit for now. It is actually easier to do it with best-fit.(just
//sort by size).

//RBTree is a classic data struct with O(log(n)) for insertion, deletion and
//search. Many years have seen its efficiency.

//a *hole* is the representation of an available blockpair for allocation. 
//defined as (start_address,size) or (offset, size) interchangably.

//each node has a *label* to indicate a pair of the max hole sizes for its subtree.

//We are implementing a RBTree with max hole sizes for subtree. It is a red
//black tree that is sorted by the start_address but also labeld with the max
//hole sizes of the subtrees. 

//        [(6,3)]  -> [(offset, size)], the hole
//        [{2,5}]  -> [{mhs_of_left, mhs_of_right}], the label
/*        /     \           */
// [(0, 1)]    [(10,  5)]
// [{0, 2}]    [{0,   0}]
/*        \                 */
//       [(3,  2)]
//       [{0,  0}]
//request of allocation size=2 goes from root to [(3,2)].

//above example shows a simplified RBTree_max_holes. 
//it is easier to tell the search time is O(log(n)) as we can make a decision on
//each descent until we get to the target.

//the only question is if we can keep the maintenance cost low -- and i think it
//is not a problem becoz an insertion/deletion is only going to update the
//max_hole_sizes of the nodes along the path from the root to the node to be
//deleted/inserted. The path can be cached and search is anyway O(log(n)).

//unlike the typical rbtree, rbtree_mhs has to handle the inserts and deletes
//with more care: an allocation that triggers the delete might leave some unused
//space which we can simply update the start_addr and size without worrying
//overlapping. An free might not only mean the insertion but also *merging* with
//the adjacent holes.

#ifndef _RED_BLACK_TREE_MHS_
#define _RED_BLACK_TREE_MHS_
#include "ft/ft.h"
#define offset_t uint64_t
enum rbtcolor {RED, BLACK};
enum direction {LEFT=1,RIGHT};


class rbtnode_mhs {
public:
struct blockpair {
    uint64_t offset;
    uint64_t size;
    blockpair() :
         offset(0), size(0) {
    }
    blockpair(uint64_t o, uint64_t s) :
         offset(o), size(s) {
    }
    int operator<(const struct blockpair &rhs) const {
        return offset < rhs.offset;
    }
    int operator<(const uint64_t &o) const {
        return offset < o;
    }
};

struct mhspair {
    uint64_t left_mhs;
    uint64_t right_mhs;
    mhspair(uint64_t l, uint64_t r) :
        left_mhs(l), right_mhs(r) {
    }
};


    rbtcolor   color;
    struct blockpair hole;
    struct mhspair   label;
    rbtnode_mhs * left;
    rbtnode_mhs * right;
    rbtnode_mhs * parent;
    
    rbtnode_mhs(rbtcolor c, rbtnode_mhs::blockpair h, struct mhspair lb, rbtnode_mhs * l,
                rbtnode_mhs * r, rbtnode_mhs * p)
        :color(c), hole(h),label(lb),left(l),right(r),parent(p) {}
    
};

class rbtree_mhs {
private:
    rbtnode_mhs * m_root;
public:
    rbtree_mhs();
    ~rbtree_mhs();
        
    void pre_order();
    void in_order();
    void post_order();
    //immutable operations   
    rbtnode_mhs * search_by_offset(uint64_t addr);
    rbtnode_mhs * search_first_fit_by_size(uint64_t size);
        
    rbtnode_mhs * min_node();
    rbtnode_mhs * max_node();

    rbtnode_mhs * successor(rbtnode_mhs *);
    rbtnode_mhs * predecessor(rbtnode_mhs *);

    //mapped from tree_allocator::free_block
    int insert(rbtnode_mhs::blockpair pair);
    //mapped from tree_allocator::alloc_block
    uint64_t remove(size_t size);
    //mapped from tree_allocator::alloc_block_after

    void raw_remove(uint64_t offset);
    void destroy();
    //print the tree
    void dump();
//validation
    //balance
    void validate_balance();
    void validate_inorder(rbtnode_mhs::blockpair *);
    void in_order_visitor(void( *f)(void *, rbtnode_mhs *, uint64_t), void *) ;

private:
    void pre_order(rbtnode_mhs * node) const;
    void in_order(rbtnode_mhs * node) const;
    void post_order(rbtnode_mhs * node) const;
    rbtnode_mhs * search_by_offset(rbtnode_mhs * node, offset_t addr) const;
    rbtnode_mhs * search_first_fit_by_size(rbtnode_mhs *node, size_t size) const;
        
    rbtnode_mhs * min_node(rbtnode_mhs * node);
    rbtnode_mhs * max_node(rbtnode_mhs * node);

    //rotations to fix up. we will have to update the labels too.
    void left_rotate(rbtnode_mhs * & root, rbtnode_mhs * x);
    void right_rotate(rbtnode_mhs * & root, rbtnode_mhs * y);
    
    int insert(rbtnode_mhs * & root, rbtnode_mhs::blockpair pair);
    int insert_fixup(rbtnode_mhs * & root, rbtnode_mhs * node);
    
    void raw_remove(rbtnode_mhs * &root, rbtnode_mhs * node);
    uint64_t remove(rbtnode_mhs * & root, rbtnode_mhs * node, size_t size);
    void raw_remove_fixup(rbtnode_mhs * & root, rbtnode_mhs * node, rbtnode_mhs *
                     parent);

    void destroy(rbtnode_mhs * & tree);
    void dump(rbtnode_mhs * tree, rbtnode_mhs::blockpair pair, direction dir);
    void recalculate_mhs(rbtnode_mhs * node);
    void is_new_node_mergable(rbtnode_mhs *, rbtnode_mhs *, rbtnode_mhs::blockpair,
                              bool *, bool *);
    void absorb_new_node(rbtnode_mhs *, rbtnode_mhs *, rbtnode_mhs::blockpair, bool, bool,bool);    
    rbtnode_mhs * search_first_fit_by_size_helper(rbtnode_mhs* x, uint64_t size) ;

    rbtnode_mhs * successor_helper(rbtnode_mhs *y, rbtnode_mhs *x) ;

    rbtnode_mhs * predecessor_helper(rbtnode_mhs *y, rbtnode_mhs * x);

    void in_order_visitor(rbtnode_mhs*, void (*f)(void *, rbtnode_mhs *,
                                                  uint64_t), void *, uint64_t) ;
    //mixed with some macros.....
#define rbn_parent(r)   ((r)->parent)
#define rbn_color(r) ((r)->color)
#define rbn_is_red(r)   ((r)->color==RED)
#define rbn_is_black(r)  ((r)->color==BLACK)
#define rbn_set_black(r)  do { (r)->color = BLACK; } while (0)
#define rbn_set_red(r)  do { (r)->color = RED; } while (0)
#define rbn_set_parent(r,p)  do { (r)->parent = (p); } while (0)
#define rbn_set_color(r,c)  do { (r)->color = (c); } while (0)
#define rbn_set_offset(r)  do { (r)->hole.offset = (c); } while (0)
#define rbn_set_size(r,c)  do { (r)->hole.size = (c); } while (0)
#define rbn_set_left_mhs(r,c)  do { (r)->label.left_mhs = (c); } while (0)
#define rbn_set_right_mhs(r,c)  do { (r)->label.right_mhs = (c); } while (0)
#define rbn_size(r)   ((r)->hole.size) 
#define rbn_offset(r)   ((r)->hole.offset)
#define rbn_key(r)   ((r)->hole.offset)
#define rbn_left_mhs(r) ((r)->label.left_mhs)
#define rbn_right_mhs(r) ((r)->label.right_mhs)
#define mhs_of_subtree(y) (std::max( std::max(rbn_left_mhs(y),rbn_right_mhs(y)),rbn_size(y)))

};

#endif
