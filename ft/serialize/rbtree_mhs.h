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
//       /       \
// [(0, 1)]    [(10,  5)]
// [{-1,2}]    [{-1, -1}]
//       \
//       [(3,  2)]
//       [{-1,-1}]
//request of allocation size=2 goes from root to [(3,2)].

//above example shows a simplified RBTree_max_holes. 
//it is easier to tell the search time is O(log(n)) as we can make a decision on
//each descent until we get to the target.

//the only question is if we can keep the maintenance cost low -- and i think it
//is not a problem becoz an insertion/deletion is only going to update the
//max_hole_sizes of the nodes along the path from the root to the node to be
//deleted/inserted. The path can be cached and search is anyway O(log(n)).

//unlike the typical rbtree, rbtree_mhs has to handles the inserts and deletes
//with more care: an allocation that triggers the delete might leave some unused
//space which we can simply update the start_addr and size without worrying
//overlapping. An free might not only mean the insertion but also *merging* with
//the adjacent holes.

#ifndef _RED_BLACK_TREE_MHS_
#define _RED_BLACK_TREE_MHS_
enum rbtcolor {RED; BLACK};
enum direction {LEFT;RIGHT};
class rbtnode_mhs {
public:

    struct blockpair {
        uint64_t offset;
        uint64_t size;
        blockpair(uint64_t o, uint64_t s) :
            offset(o), size(s) {
        }
        blockpair(struct blockpair p) :
            offset(p.offset), size(p.size) {
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
        mhspair(struct mhspair p) :
            left_mhs(p.left_mhs), right_mhs(p.right_mhs) {
        }
    };

    rbtcolor   color;
    struct blockpair hole;
    struct mhspair   label;
    rbtnode_mhs * left;
    rbtnode_mhs * right;
    rbtnode_mhs * parent;
    rbtnode_mhs(rbtcolor c, struct blockpair h, struct mhspair lb, rbtnode_mhs * l,
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
    rbtnode_mhs * search_by_offset(offset_t addr);
    rbtnode_mhs * search_first_fit_by_size(size_t size);
        
    rbtnode_mhs * min_node();
    rbtnode_mhs * max_node();

    rbtnode_mhs * successor(rbtnode_mhs *);
    rbtnode_mhs * predecessor(rbtnode_mhs *);

    //mapped from tree_allocator::free_block
    int insert(struct blockpair pair);
    //mapped from tree_allocator::alloc_block
    int remove(size_t size);
    //mapped from tree_allocator::alloc_block_after
    int remove_after(offset_t start, size_t size);

    int destroy();
    //print the tree
    void dump();
private:
    void pre_order(rbtnode_mhs * node) const;
    void in_order(rbtnode_mhs * node) const;
    void post_order(rbtnode_mhs * node) const;
    rbtnode_mhs * search_by_offset(rbtnode_mhs * node, offset_t addr) const;
    rbtnode_mhs * search_first_fit_by_size(rbtnode_mhs *node, size_t size) const;
        
    rbtnode_mhs * min_node(rbtnode_mhs * node);
    rbtnode_mhs * max_node(rbtnode_mhs * node);

    //rotations to fix up. we will have to update the labels too.
    void leftRotate(rbtnode * & root, rbtnode * x);
    void rightRotate(rbtnode * & root, rbtnode* y);
    
    int insert(rbtnode_mhs * & root, struct blockpair pair);
    int insert_fixup(rbtnode_mhs * & root, rbtnode_mhs * node);
    
    int remove(rbtnode_mhs * & root, rbtnode_mhs * node, size_t size);
    int remove_fixup(rbtnode_mhs * & root, rbtnode_mhs * node, rbtnode_mhs *
                     parent);

    int destroy(rbtnode * & tree);
    void dump(rbtnode * tree, uint64_t addr, direction dir);

//simple macros.
#define rb_parent(r)   ((r)->parent)
#define rb_color(r) ((r)->color)
#define rb_is_red(r)   ((r)->color==RED)
#define rb_is_black(r)  ((r)->color==BLACK)
#define rb_set_black(r)  do { (r)->color = BLACK; } while (0)
#define rb_set_red(r)  do { (r)->color = RED; } while (0)
#define rb_set_parent(r,p)  do { (r)->parent = (p); } while (0)
#define rb_set_color(r,c)  do { (r)->color = (c); } while (0)

}

#endif
