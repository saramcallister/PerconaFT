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
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include "cachetable/checkpoint.h"
#include "test.h"
#include "affine_test.h"

void 
shuffle(int *array, size_t n)
{
    if (n > 1) 
    {
        size_t i;
        for (i = 0; i < n - 1; i++) 
        {
          size_t j = i + rand() / (RAND_MAX / (n - i) + 1);
          int t = array[j];
          array[j] = array[i];
          array[i] = t;
        }
    }
}

int
test_main(int argc, const char *argv[]) {
    //set up the node size and basement size for benchmarking
    double nodeMB = parse_args_for_nodesize (argc, argv); 
    //if user supply 4, it means the nodesize = 4MB
    nodesize = nodeMB * (1 << 20);
    size_t B = nodesize /(keysize*8+valsize);
    int fanout = pow (B, epsilon);
    basementsize = nodesize/fanout;
    calculate_numrows();
    calculate_randomnumbers();
    printf("Benchmarking fractal tree based on nodesize = %zu bytes(%lfMBs) \n \t key: %zu bytes (%zu KBs); value: %zu bytes (%zu KBs) \n\t B = %zu, epsilon=0.5, fanout = %d\n Preparing 16 GBs data...\n", nodesize, nodeMB, keysize*8, (keysize*8)/1024, valsize, valsize/1024, B, fanout);

    //set up the data file
    const char *n = "/mnt/db/affine_benchmark_data";
    int r;
    FT_HANDLE t;
    CACHETABLE ct;
    unlink(n);
    toku_cachetable_create(&ct, 0, ZERO_LSN, nullptr);
    r = toku_open_ft_handle(n, 1, &t, nodesize, basementsize, TOKU_NO_COMPRESSION, ct, null_txn, uint64_dbt_cmp); assert(r==0);
    //set up the fanout so the B^e =  fanout.
    toku_ft_handle_set_fanout(t, fanout);
    char val[valsize];
    ZERO_ARRAY(val);
    uint64_t key[keysize]; //key is 16k
    ZERO_ARRAY(key);
    DBT k, v;
    dbt_init(&k, key, keysize*8);
    dbt_init(&v, val, valsize);

    // get numrows random keys
    int *key_array = (int *)malloc(sizeof(int) * numrows)
    for (size_t i=0; i< numrows; i++) {
        key_array[i] = (int) i;
    }
    shuffle(key_array, num_rows);

    for (size_t i=0; i< numrows; i++) {
    	key[0] = toku_htod64(key_array[i]);
	toku_ft_insert(t, &k, &v, null_txn);
    }
    CHECKPOINTER cp = toku_cachetable_get_checkpointer(ct);
    r = toku_checkpoint(cp, NULL, NULL, NULL, NULL, NULL, CLIENT_CHECKPOINT);
    assert_zero(r); 
    r = toku_close_ft_handle_nolsn(t, 0); assert(r==0);
    toku_cachetable_close(&ct);
    return 0;
}
