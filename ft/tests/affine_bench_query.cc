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
#include "cachetable/checkpoint.h"
#include "test.h"
#include "affine_test.h"
#define NUM_QUERY_THREADS 25
static FT_HANDLE t;
static CACHETABLE ct;

static void * random_query(void *arg) {
   char val[valsize];
   ZERO_ARRAY(val);
   uint64_t key[keysize]; //key is 16k
   ZERO_ARRAY(key);
   DBT k;
   int r;
   uint64_t * array = (uint64_t *) arg;
   for(size_t i=0; i< random_numbers/NUM_QUERY_THREADS; i++) {
       key[0] = toku_htod64(array[i]);
       struct check_pair pair = {keysize*8, key, len_ignore, NULL, 0};
       r = toku_ft_lookup(t, toku_fill_dbt(&k, key, keysize*8), lookup_checkf, &pair); assert(r == 0);
   }
   return arg;
}

int
test_main(int argc, const char *argv[]) {
    struct timespec start, end;
    double elapsed;

    //set up the node size and basement size for benchmarking
    double nodeMB = parse_args_for_nodesize (argc, argv); 
    //if user supply 4, it means the nodesize = 4MB
    nodesize = nodeMB * (1 << 20);
    size_t B = nodesize /(keysize*8+valsize);
    int fanout = pow (B, epsilon);
    basementsize = nodesize/fanout;
    calculate_numrows();
    calculate_randomnumbers();

    char default_file_name[30] = "/mnt/db/affine_benchmark_data";
    char secondary_file_name[50] = "/mnt/db/affine_benchmark_data_";
    sprintf(secondary_file_name+strlen("affine_benchmark_data_"), "%lf", nodeMB);
    
    char * n = does_file_exist(secondary_file_name)? secondary_file_name : default_file_name;		

    printf("Benchmarking fractal tree based on nodesize = %zu bytes(%lfMBs) \n \t key: %zu bytes (%zu KBs); value: %zu bytes (%zu KBs) \n\t B = %zu, epsilon=0.5, fanout = %d\n Querying randomly over 16 GBs data file[%s]...\n", nodesize, nodeMB, keysize*8, (keysize*8)/1024, valsize, valsize/1024, B, fanout, n);

    int r;
    toku_cachetable_create(&ct, 0, ZERO_LSN, nullptr);
    r = toku_open_ft_handle(n, 0, &t, nodesize, basementsize, TOKU_NO_COMPRESSION, ct, null_txn, uint64_dbt_cmp); assert(r==0);
    toku_ft_handle_set_fanout(t, fanout);
    uint64_t *array = (uint64_t *)toku_malloc(sizeof(uint64_t) * random_numbers);
    if (array == NULL) {
       fprintf(stderr, "Allocate memory failed\n");
       return -1;
    }
    randomize(array); 
    toku_pthread_t query_tid[NUM_QUERY_THREADS];
    clock_gettime(CLOCK_MONOTONIC, &start);
    for (int i = 0; i < NUM_QUERY_THREADS; i++) {
	uint64_t * args = array + i* (random_numbers/NUM_QUERY_THREADS);
        r = toku_pthread_create(toku_uninstrumented,
                                &query_tid[i],
                                nullptr,
                                random_query,
                                args);
        assert_zero(r);
    }
    for (int i = 0; i < NUM_QUERY_THREADS; i++) {
        void * ret;
        r = toku_pthread_join(query_tid[i], &ret); 
        assert_zero(r);
    }
   clock_gettime(CLOCK_MONOTONIC, &end);
   elapsed = (end.tv_sec - start.tv_sec) +
            (end.tv_nsec - start.tv_nsec) / 1000000000.0;
   printf("op, seq.or.rand, rows, time.s\n");
   printf("query, random, %ld, %lf\n", random_numbers, elapsed);
   toku_free(array);
    r = toku_close_ft_handle_nolsn(t, 0); assert(r==0);
    toku_cachetable_close(&ct);
    return 0;
}
