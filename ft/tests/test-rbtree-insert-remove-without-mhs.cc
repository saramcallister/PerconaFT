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

#include "ft/serialize/rbtree_mhs.h"
#include "test.h"

static rbtnode_mhs::blockpair a[] = {
    {10,0},{40,0},{30,0},
    {60,0},{90,0},{70,0},
    {20,0},{50,0},{80,0}
};

static void test_insert_remove(void) {	
    int i;
	  int ilen = (sizeof(a)) / (sizeof(a[0])) ;
    rbtree_mhs * tree = new rbtree_mhs();
    verbose = 1;	
    printf("we are going to insert the following block offsets\n");
	  for(i=0; i<ilen; i++)
		    printf("%" PRIu64 "\t", a[i].offset);

	  for(i=0; i<ilen; i++) {
		    tree->insert(a[i]);
		    if(verbose) {
			      printf("we just inserted %" PRIu64 ", going to dump the tree\n", a[i].offset);
			      tree->dump();
		    }   

	  }

	  printf("pre order:\n");
	  tree->pre_order();

	  printf("in order:\n");
	  tree->in_order();

	  printf("post order:\n");
	  tree->post_order();
	

    printf("min node of the tree:%" PRIu64 "\n", tree->min_node()->hole.offset);
    printf("max node of the tree:%" PRIu64 "\n", tree->max_node()->hole.offset);

	  for(i=0; i<ilen; i++) {
			  tree->raw_remove(a[i].offset);
			  printf("after removed the node %" PRIu64 ", dumping the tree\n",
               a[i].offset);
        tree->dump();
		  }

	  tree->destroy();

}

int
test_main (int argc , const char *argv[]) {
    default_parse_args(argc, argv);
    
    test_insert_remove();
    if (verbose) printf("test ok\n");
    return 0;
}
