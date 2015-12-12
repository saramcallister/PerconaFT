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
#include <algorithm>    
#include <vector>       
#include <ctime>        
#include <cstdlib>    

#define N 1000000
std::vector<rbtnode_mhs::blockpair> input_vector;
rbtnode_mhs::blockpair old_vector[N];

static int myrandom (int i) { return std::rand()%i;}

static void generate_random_input () {
    std::srand ( unsigned ( std::time(0) ) );

    // set some values:
    for (uint64_t i=1; i<N; ++i) 
      {
        input_vector.push_back({i, 0});
        old_vector[i] = {i,0};
      }
    // using built-in random generator:
    std::random_shuffle ( input_vector.begin(), input_vector.end(), myrandom);
}

static void test_insert_remove(void) {	
    int i;
    rbtree_mhs * tree = new rbtree_mhs();
    verbose = 0;	
    generate_random_input();
    if(verbose) {
        printf("\n we are going to insert the following block offsets\n");
        for(i=0; i<N; i++)
		        printf("%" PRIu64 "\t", input_vector[i].offset);
    }
	  for(i=0; i<N; i++) {
		    tree->insert(input_vector[i]);
       // tree->validate_balance();
    }
    tree->validate_balance();
    rbtnode_mhs::blockpair * p_bps = &old_vector[0];
	  tree->validate_inorder(p_bps); 
    printf("min node of the tree:%" PRIu64 "\n", tree->min_node()->hole.offset);
    printf("max node of the tree:%" PRIu64 "\n", tree->max_node()->hole.offset);

	  for(i=0; i<N; i++) {
			// tree->validate_balance();   
       tree->raw_remove(input_vector[i].offset);
		    
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
