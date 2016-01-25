/*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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
    MERCHANTABILIT or FITNESS FOR A PARTICULAR PURPOSE.  See the
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

#include "ft/serialize/rbtree_mhs.h"
#include "portability/toku_assert.h"
#include <iostream>
#include <iomanip>
rbtree_mhs::rbtree_mhs():m_root(NULL)
{ 
   m_align = 1; //we do not align
}

rbtree_mhs::rbtree_mhs(uint64_t align):m_root(NULL)
{
	  m_align = align;
}

rbtree_mhs::~rbtree_mhs() 
{
	  destroy();
}

void rbtree_mhs::pre_order(rbtnode_mhs* tree) const
{
	  if(tree != NULL)
	  {
      std::cout<< rbn_offset(tree).to_int() << " " ;
		    pre_order(tree->left);
		    pre_order(tree->right);
	  }
}

void rbtree_mhs::pre_order() 
{
	  pre_order(m_root);
}

void rbtree_mhs::in_order(rbtnode_mhs* tree) const
{
	  if(tree != NULL)
	  {
		    in_order(tree->left);
        std::cout<< rbn_offset(tree).to_int() << " " ;
		    in_order(tree->right);
	  }
}

//yeah, i only care about in order visitor. -Jun
void rbtree_mhs::in_order_visitor(rbtnode_mhs * tree, void(* f)(void*,
                                                                rbtnode_mhs *,
                                                                uint64_t),
                                  void * extra, uint64_t depth) {
   if(tree != NULL) {
       in_order_visitor(tree->left, f, extra, depth+1);
       f(extra, tree, depth);
       in_order_visitor(tree->right, f, extra, depth+1);
   }
}

void rbtree_mhs::in_order_visitor(void(* f)(void *, rbtnode_mhs *, uint64_t
                                            ), void *  extra) {
    in_order_visitor(m_root, f, extra, 0);
}

void rbtree_mhs::in_order() 
{
	  in_order(m_root);
}

void rbtree_mhs::post_order(rbtnode_mhs* tree) const
{
	  if(tree != NULL)
	  {
		    post_order(tree->left);
		    post_order(tree->right);
        std::cout<< rbn_offset(tree).to_int() << " " ;
  	}
}

void rbtree_mhs::post_order() 
{
	  post_order(m_root);
}

rbtnode_mhs * rbtree_mhs::search_by_offset(uint64_t offset) {
    rbtnode_mhs * x = m_root; 
    while ((x!=NULL) && (rbn_offset(x).to_int()!=offset))
	  {
		    if (offset < rbn_offset(x).to_int())
			      x = x->left;
		    else
			      x = x->right;
	  }

	  return x;
}

//mostly for testing
rbtnode_mhs * rbtree_mhs::search_first_fit_by_size(uint64_t size) {
    if(get_effective_size(m_root) < size && rbn_left_mhs(m_root) < size &&
       rbn_right_mhs(m_root) < size) {
        return nullptr;
    }     
    else {
        return search_first_fit_by_size_helper(m_root, size);
    }
}


rbtnode_mhs * rbtree_mhs::search_first_fit_by_size_helper(rbtnode_mhs* x, uint64_t size) {

    if(get_effective_size(x) >= size) {
      //only possible to go left
      if(rbn_left_mhs(x) >= size) 
          return search_first_fit_by_size_helper(x->left, size);
      else 
          return x;
    } else {
          if(rbn_left_mhs(x) >= size) 
              return search_first_fit_by_size_helper(x->left, size);
          else {
              if(rbn_right_mhs(x) >= size) 
                  return search_first_fit_by_size_helper(x->right, size);
              else {
                  //this is an invalid state
                  dump();
                  validate_balance();
                  validate_mhs();
                  assert(0);
              }
          }      
    }

}

rbtnode_mhs * rbtree_mhs::min_node(rbtnode_mhs * tree)
{
	  if (tree == NULL)
		    return NULL;

	  while(tree->left != NULL)
		    tree = tree->left;
	  return tree;
}

rbtnode_mhs* rbtree_mhs::min_node()
{
	  return min_node(m_root);
}
 

rbtnode_mhs * rbtree_mhs::max_node(rbtnode_mhs* tree)
{
  	if (tree == NULL)
	  	return NULL;

	  while(tree->right != NULL)
		  tree = tree->right;
	  return tree;
}


rbtnode_mhs * rbtree_mhs::max_node()
{
	  return max_node(m_root);
}

rbtnode_mhs * rbtree_mhs::successor_helper(rbtnode_mhs *y, rbtnode_mhs *x) {
    while ((y!=NULL) && (x==y->right))
	      {
		        x = y;
		        y = y->parent;
	      }
	  return y;

}
rbtnode_mhs * rbtree_mhs::successor(rbtnode_mhs *x)
{
	  if (x->right != NULL)
		    return min_node(x->right);

	  rbtnode_mhs * y = x->parent;
    return successor_helper(y, x);
}
 
rbtnode_mhs * rbtree_mhs::predecessor_helper(rbtnode_mhs *y, rbtnode_mhs * x){
    while ((y!=NULL) && (x==y->left))
	  {
		    x = y;
		    y = y->parent;
	  }

	  return y;

}
rbtnode_mhs * rbtree_mhs::predecessor(rbtnode_mhs *x)
{
	  if (x->left != NULL)
		    return max_node(x->left);

	  rbtnode_mhs * y = x->parent;
    return successor_helper(y,x);
}

/*
*      px                              px
*     /                               /
*    x                               y                
*   /  \      --(left rotation)-->  / \               # 
*  lx   y                          x  ry     
*     /   \                       /  \
*    ly   ry                      lx  ly  
*  max_hole_size updates are pretty local
*/


void rbtree_mhs::left_rotate(rbtnode_mhs *& root, rbtnode_mhs *x) {
    rbtnode_mhs *y = x->right;

    x->right = y->left;
    rbn_right_mhs(x) = rbn_left_mhs(y);

    if (y->left != NULL)
        y->left->parent = x;

        y->parent = x->parent;

        if (x->parent == NULL){
            root = y;     
        }
        else{
            if (x->parent->left == x) {
                x->parent->left = y;   
            }
            else {
                x->parent->right = y; 
            }
        }      
        y->left = x;
        rbn_left_mhs(y) = mhs_of_subtree(x);
        
        x->parent = y;
}

/*            py                               py
 *           /                                /
 *          y                                x                  
 *         /  \      --(right rotate)-->    /  \                     #
 *        x   ry                           lx   y  
 *       / \                                   / \                   #
 *      lx  rx                                rx  ry
 * 
 */
        
void rbtree_mhs::right_rotate(rbtnode_mhs * &root, rbtnode_mhs * y)
{
    rbtnode_mhs *x = y->left;

    y->left = x->right;
    rbn_left_mhs(y) = rbn_right_mhs(x);

    if (x->right != NULL)
        x->right->parent = y;

    x->parent = y->parent;

    if (y->parent == NULL) {
        root = x;         
    }
    else {
        if (y == y->parent->right)
            y->parent->right = x; 
        else
            y->parent->left = x;  
    }

    x->right = y;
    rbn_right_mhs(x) = mhs_of_subtree(y);
    y->parent = x;
}

//walking from this node up to update the mhs info
//whenver there is change on left/right mhs or size we should recalculate.
//prerequisit: the children of the node are mhs up-to-date.
void rbtree_mhs:: recalculate_mhs(rbtnode_mhs * node) {
    uint64_t * p_node_mhs = 0;
    rbtnode_mhs * parent = node -> parent;

    if(!parent)
      return;

    uint64_t max_mhs = mhs_of_subtree(node);
    if(node == parent->left) {
      p_node_mhs = &rbn_left_mhs(parent);
    } else {
      p_node_mhs = &rbn_right_mhs(parent);
    }
    if(*p_node_mhs != max_mhs) { 
      *p_node_mhs = max_mhs;
      recalculate_mhs(parent);        
    }

}

void rbtree_mhs:: is_new_node_mergable(rbtnode_mhs * pred, rbtnode_mhs * succ,
                                       rbtnode_mhs::blockpair pair,
                                       bool * left_merge, bool * right_merge) {

    if(pred) {
        mhs_uint64_t end_of_pred = rbn_size(pred) + rbn_offset(pred);
        if(end_of_pred < pair.offset)
            *left_merge = false;
        else {
            assert(end_of_pred == pair.offset);
            *left_merge = true;
          }
        }   
    if(succ) {
        mhs_uint64_t begin_of_succ = rbn_offset(succ);
        mhs_uint64_t end_of_node = pair.offset + pair.size;
        if(end_of_node < begin_of_succ) {
            *right_merge = false;
          } else {
            assert(end_of_node == begin_of_succ);
            *right_merge = true;
          }
      }
}

void rbtree_mhs:: absorb_new_node(rbtnode_mhs * pred,  rbtnode_mhs *succ,
                                  rbtnode_mhs::blockpair pair, 
                                  bool left_merge, bool right_merge, bool
                                  is_right_child) {
    assert(left_merge || right_merge);
    if(left_merge && right_merge) {
    //merge to the succ
        if(!is_right_child) {
            rbn_size(succ)+=pair.size;
            rbn_offset(succ) = pair.offset;
            //merge to the pred
            rbn_size(pred)+= rbn_size(succ);
            //to keep the invariant of the tree -no overlapping holes
            rbn_offset(succ)+=rbn_size(succ);
            rbn_size(succ) = 0;
            recalculate_mhs(succ);
            recalculate_mhs(pred);
            //pred dominates succ. this is going to
            //update the pred labels separately.
            //remove succ
            raw_remove(m_root, succ);
      } else {
            rbn_size(pred) += pair.size;
            rbn_offset(succ) = rbn_offset(pred);
            rbn_size(succ)+= rbn_size(pred);
            rbn_offset(pred) += rbn_size(pred);
            rbn_size(pred) = 0;
            recalculate_mhs(pred); 
            recalculate_mhs(succ);                
            //now remove pred
            raw_remove(m_root, pred);
            
      }
    } else if(left_merge) {
        rbn_size(pred) += pair.size;
        recalculate_mhs(pred);
    } else if(right_merge) {
        rbn_offset(succ) -= pair.size;
        rbn_size(succ) += pair.size;
        recalculate_mhs(succ);
    }
}
//this is the most tedious part, but not complicated:
//1.find where to insert the pair
//2.if the pred and succ can merge with the pair. merge with them. either pred
//or succ can be removed.
//3. if only left-mergable or right-mergeable, just merge
//4. non-mergable case. insert the node and run the fixup.

int rbtree_mhs::insert(rbtnode_mhs * & root, rbtnode_mhs::blockpair pair) {
    rbtnode_mhs *x = m_root;
    rbtnode_mhs * y = NULL;
    bool left_merge = false;
    bool right_merge = false;
    rbtnode_mhs * node = NULL;

    while (x != NULL) {
        y = x;
        if (pair.offset < rbn_key(x))
            x = x->left;
        else
            x = x->right;
    }

    //we found where to insert, lets find out the pred and succ for possible
    //merges. 
  //  node->parent = y;
    rbtnode_mhs * pred, * succ;
    if (y!=NULL)  {
        if (pair.offset < rbn_key(y)) {
            //as the left child
            pred = predecessor_helper(y->parent, y);
            succ = y;
            is_new_node_mergable(pred, succ, pair, &left_merge, &right_merge);
            if(left_merge || right_merge) {
                absorb_new_node(pred, succ, pair, left_merge, right_merge, false);
            } else {
            //construct the node
              rbtnode_mhs::mhspair mhsp = {.left_mhs = 0, .right_mhs=0};
              node = new rbtnode_mhs(BLACK, pair, mhsp, nullptr, nullptr, nullptr);
              if(!node) 
                  return -1; 
              y->left = node;
              node->parent = y; 
              recalculate_mhs(node); 
            }
           
        }   
        else {
            //as the right child
            pred = y;
            succ = successor_helper(y->parent, y);
            is_new_node_mergable(pred, succ, pair, &left_merge, &right_merge);
            if(left_merge || right_merge) {
                absorb_new_node(pred, succ, pair, left_merge, right_merge, true);
            } else {
             //construct the node
              rbtnode_mhs::mhspair mhsp = {.left_mhs = 0, .right_mhs=0};
              node = new rbtnode_mhs(BLACK, pair, mhsp, nullptr, nullptr, nullptr);
              if(!node) 
                  return -1;
              y->right = node; 
              node->parent = y;           
              recalculate_mhs(node); 
            }

        }
    }
    else {
        rbtnode_mhs::mhspair mhsp = {.left_mhs = 0, .right_mhs=0};
        node = new rbtnode_mhs(BLACK, pair, mhsp, nullptr, nullptr, nullptr);     
        if(!node) 
            return -1;
        root = node;
    }
    if(!left_merge && !right_merge) {
        assert(node);
        node->color = RED;
        return insert_fixup(root, node);
    }
    return 0;
}

int rbtree_mhs:: insert_fixup(rbtnode_mhs * & root, rbtnode_mhs * node) {
	  rbtnode_mhs  *parent, *gparent;
	  while ((parent = rbn_parent(node)) && rbn_is_red(parent)){
		    gparent = rbn_parent(parent);
		    if (parent == gparent->left){
			      {
				        rbtnode_mhs *uncle = gparent->right;
				        if (uncle && rbn_is_red(uncle))  {
					          rbn_set_black(uncle);
					          rbn_set_black(parent);
					          rbn_set_red(gparent);
					          node = gparent;
					          continue;
				        }
			      }

			      if (parent->right == node)  {
				        rbtnode_mhs *tmp;
				        left_rotate(root, parent);
				        tmp = parent;
				        parent = node;
				        node = tmp;
			      }

			    rbn_set_black(parent);
			    rbn_set_red(gparent);
		      right_rotate(root, gparent);
		    } 
		    else  {
			      {
				        rbtnode_mhs *uncle = gparent->left;
				        if (uncle && rbn_is_red(uncle))  {
					          rbn_set_black(uncle);
					          rbn_set_black(parent);
					          rbn_set_red(gparent);
					          node = gparent;
					          continue;
				        }
			      } 

			      if (parent->left == node) {
				        rbtnode_mhs *tmp;
				        right_rotate(root, parent);
				        tmp = parent;
				        parent = node;
				        node = tmp;
			      }
			      rbn_set_black(parent);
			      rbn_set_red(gparent);
			      left_rotate(root, gparent);
		    }
	  }
    rbn_set_black(root);
    return 0;
}


int rbtree_mhs::insert(rbtnode_mhs::blockpair pair) {
    return  insert(m_root, pair);
}
  
uint64_t rbtree_mhs::remove( size_t size){
    rbtnode_mhs * node = search_first_fit_by_size(size);
    return remove(m_root, node, size);
} 

   
void rbtree_mhs::raw_remove(rbtnode_mhs * &root, rbtnode_mhs * node) {
    rbtnode_mhs *child, *parent;
	  rbtcolor color;

	  if ( (node->left!=NULL) && (node->right!=NULL) ) {
		    rbtnode_mhs *replace = node;
		    replace = replace->right;
		    while (replace->left != NULL)
			      replace = replace->left;

		if (rbn_parent(node)) {
			  if (rbn_parent(node)->left == node)
				    rbn_parent(node)->left = replace;
			  else
				    rbn_parent(node)->right = replace;
		} 
		else {
			root = replace;
    }
		child = replace->right;
		parent = rbn_parent(replace);
		color = rbn_color(replace);

		if (parent == node) {
			  parent = replace;
		} 
		else {
			  if (child)
				    rbn_parent(child) = parent;

			  parent->left = child;
        rbn_left_mhs(parent) = rbn_right_mhs(replace);
			  recalculate_mhs(parent); 
        replace->right = node->right;
			  rbn_set_parent(node->right, replace);
        rbn_right_mhs(replace) = rbn_right_mhs(node);

    }

		replace->parent = node->parent;
		replace->color = node->color;
		replace->left = node->left;
		rbn_left_mhs(replace) = rbn_left_mhs(node);
    node->left->parent = replace;
    recalculate_mhs(replace);

		if (color == BLACK)
			raw_remove_fixup(root, child, parent);

		delete node;
		return ;
	  }

	  if (node->left !=NULL)
		    child = node->left;
	  else 
		    child = node->right;

	  parent = node->parent;
	  color = node->color;

	  if (child)
		    child->parent = parent;

	  if (parent) {
		    if (parent->left == node) {
			      parent->left = child;
            rbn_left_mhs(parent) = child?mhs_of_subtree(child):0;
        }
		    else {
			      parent->right = child;
            rbn_right_mhs(parent) = child?mhs_of_subtree(child):0;
        }
        recalculate_mhs(parent);
	  } else
		    root = child;

	  if (color == BLACK)
		    raw_remove_fixup(root, child, parent);
	  delete node;

}

void rbtree_mhs::raw_remove(uint64_t offset) {
    rbtnode_mhs * node =search_by_offset(offset);
    raw_remove(m_root, node);
}  
static inline uint64_t align(uint64_t value, uint64_t ba_alignment) {
    return ((value + ba_alignment - 1) / ba_alignment) * ba_alignment;
}
uint64_t rbtree_mhs::remove(rbtnode_mhs * & root, rbtnode_mhs * node, size_t size){
    mhs_uint64_t n_offset = rbn_offset(node);
    mhs_uint64_t n_size = rbn_size(node);
    mhs_uint64_t answer_offset(align(rbn_offset(node).to_int(), m_align));
  
    assert((answer_offset + size) <= (n_offset + n_size));
    if(answer_offset == n_offset) {    
        rbn_offset(node)+= size;
        rbn_size(node) -= size;
        recalculate_mhs(node); 
        if(rbn_size(node) == 0) {
            raw_remove(root, node);
        }
        
    } else {
        if(answer_offset+size == n_offset + n_size) {
            rbn_size(node)-= size;
            recalculate_mhs(node);
        } else {
        //well, cut in the middle...
            rbn_size(node) = answer_offset - n_offset;
            recalculate_mhs(node); 
            insert(m_root, {(answer_offset+size), (n_offset + n_size)
                   -(answer_offset+size)});
        }
    }
    return answer_offset.to_int();
}

void rbtree_mhs::raw_remove_fixup(rbtnode_mhs * & root, rbtnode_mhs * node, rbtnode_mhs *
                     parent) {
	  rbtnode_mhs *other;
    while ((!node || rbn_is_black(node)) && node != root) {
	      if (parent->left == node) {
			     other = parent->right;
			      if (rbn_is_red(other)) {
                  //Case 1: the brother of X, w, is read
				        rbn_set_black(other);
				        rbn_set_red(parent);
				        left_rotate(root, parent);
				        other = parent->right;
			      }
			      if ((!other->left || rbn_is_black(other->left)) &&
			          (!other->right || rbn_is_black(other->right))) {
                  // Case 2: w is black and both of w's children are black
				        rbn_set_red(other);
				        node = parent;
				        parent = rbn_parent(node);
			      }
			      else {
			          if (!other->right || rbn_is_black(other->right)) {
                      // Case 3: w is black and left child of w is red but right child is black
					          rbn_set_black(other->left);
					          rbn_set_red(other);
					          right_rotate(root, other);
					          other = parent->right;
				        }
                  // Case 4: w is black and right child of w is red, regardless of left child's color
				        rbn_set_color(other, rbn_color(parent));
				        rbn_set_black(parent);
				        rbn_set_black(other->right);
				        left_rotate(root, parent);
				        node = root;
				        break;
			      }
		  }
		  else {
			    other = parent->left;
			    if (rbn_is_red(other)){
                // Case 1: w is red
				      rbn_set_black(other);
				      rbn_set_red(parent);
				      right_rotate(root, parent);
				      other = parent->left;
			    }
			    if ((!other->left || rbn_is_black(other->left)) &&
			      (!other->right || rbn_is_black(other->right))) {
                // Case 2: w is black and both children are black
				      rbn_set_red(other);
				      node = parent;
				      parent = rbn_parent(node);
			    } else {
				      if (!other->left || rbn_is_black(other->left)) {
                      // Case 3: w is black and left child of w is red whereas right child is black
					        rbn_set_black(other->right);
					        rbn_set_red(other);
					        left_rotate(root, other);
					        other = parent->left;
				      }
                // Case 4:w is black and right child of w is red, regardless of the left child's color
				  rbn_set_color(other, rbn_color(parent));
				  rbn_set_black(parent);
				  rbn_set_black(other->left);
				  right_rotate(root, parent);
				  node = root;
				  break;
			  }
		  }
	  }
    if (node)
		    rbn_set_black(node);
}


void rbtree_mhs::destroy(rbtnode_mhs* &tree)  {
	  if (tree==NULL)
		    return ;

	  if (tree->left != NULL)
		    return destroy(tree->left);
	  if (tree->right != NULL)
		    return destroy(tree->right);

	  delete tree;
	  tree=NULL;
}

void rbtree_mhs::destroy()  {
	  destroy(m_root);
}

void rbtree_mhs::dump(rbtnode_mhs * tree, rbtnode_mhs::blockpair pair,
                       direction dir)
{
	  if(tree != NULL)  {
		    if(dir==0)
			      std::cout << std::setw(2) <<"("<< rbn_offset(tree).to_int() <<"," <<
                rbn_size(tree).to_int() <<", mhs:(" <<
                rbn_left_mhs(tree) <<"," <<
                rbn_right_mhs(tree)<<")"<<")" << "(B) is root" <<  std::endl;
		    else			
			      std::cout << std::setw(2) <<"("<< rbn_offset(tree).to_int() <<","<<
                rbn_size(tree).to_int()<<",mhs:(" << 
                rbn_left_mhs(tree) << "," <<
                rbn_right_mhs(tree) << ")" << ")" <<
              (rbn_is_red(tree)?"(R)":"(B)") << " is " << std::setw(2) <<
              pair.offset.to_int() <<
              "'s "  << std::setw(12) << (dir==RIGHT?"right child" : "left child")<<  std::endl;

		    dump(tree->left, tree->hole, LEFT);
		    dump(tree->right, tree->hole, RIGHT);
	}
}



uint64_t rbtree_mhs::get_effective_size(rbtnode_mhs * node) {
    mhs_uint64_t offset = rbn_offset(node);
    mhs_uint64_t size = rbn_size(node);
    mhs_uint64_t end = offset + size;
    mhs_uint64_t aligned_offset(align(offset.to_int(), m_align));
    if(aligned_offset > end) {
        return 0;
    }
    return (end - aligned_offset).to_int();
}

void rbtree_mhs::dump() {
	  if(m_root != NULL)
		    dump(m_root, m_root->hole, (direction)0);
}


static void vis_bal_f(void *  extra, rbtnode_mhs * node, uint64_t depth) {
    uint64_t ** p = (uint64_t **) extra;
    uint64_t min = *p[0];
    uint64_t max = *p[1];
    if(node->left) {
        rbtnode_mhs * left = node->left;
        assert(node == left->parent);
    } 

    if(node->right) {
        rbtnode_mhs * right = node->right;
        assert(node == right->parent);
    } 

    if(!node->left || !node->right) {
        if(min > depth) {
            *p[0] = depth;
        } else if (max < depth) {
            *p[1] = depth;
        }
    }

}

void rbtree_mhs::validate_balance() {
  uint64_t min_depth = 0xffffffffffffffff;
  uint64_t max_depth = 0;
  if(!m_root) {
      return;
  }
  uint64_t* p[2] = {&min_depth, &max_depth};
  in_order_visitor(vis_bal_f, (void *)p);
  if((min_depth+1)*2 < max_depth+1)
      assert(0);
}

static void vis_cmp_f(void *  extra, rbtnode_mhs * node, uint64_t UU(depth)) {
    rbtnode_mhs::blockpair ** p = (rbtnode_mhs::blockpair **) extra;

    if(*p == NULL) 
        assert(0);
    if((*p)->offset != node->hole.offset) 
        assert(0);

    *p = *p + 1;

}

//validate the input pairs matches with sorted pairs
void rbtree_mhs::validate_inorder(rbtnode_mhs::blockpair * pairs) {
  in_order_visitor(vis_cmp_f, &pairs);
}

uint64_t rbtree_mhs::validate_mhs(rbtnode_mhs * node) {
  if(!node) return 0;
  else {
      uint64_t mhs_left = validate_mhs(node->left);
      uint64_t mhs_right = validate_mhs(node->right);
      if(mhs_left != rbn_left_mhs(node)) {
          printf("assert failure: mhs_left = %" PRIu64 "\n", mhs_left);
          dump(node, node->hole, (direction)0);          
      }
      assert(mhs_left == rbn_left_mhs(node));

      if(mhs_right != rbn_right_mhs(node)) {
          printf("assert failure: mhs_right = %" PRIu64 "\n", mhs_right);
          dump(node, node->hole, (direction)0);          
      }
      assert(mhs_right == rbn_right_mhs(node));
      return std::max(get_effective_size(node),std::max(mhs_left, mhs_right));
  }
}

void rbtree_mhs::validate_mhs() { 
    if(!m_root) return;
    uint64_t mhs_left = validate_mhs(m_root->left);
    uint64_t mhs_right = validate_mhs(m_root->right);
    assert(mhs_left == rbn_left_mhs(m_root));
    assert(mhs_right == rbn_right_mhs(m_root));       
}


