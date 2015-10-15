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

//In response to the read-commit crash bug in the sysbench, this test is created to test
//the atomicity of the txn manager when handling the child txn snapshot.
//The test is supposed to fail before the read-commit-fix.

#include "test.h"
#include <pthread.h>
#include "ft/txn/txn.h"
struct start_txn_arg {
    DB_ENV *env;
    DB *db;
    DB_TXN * parent;
};

static inline void toku_set_test_txn_sync_callback(void (* cb) (uint64_t, void *), void * extra) {
	set_test_txn_sync_callback(cb, extra);
}

static void test_callback(uint64_t self_tid, void * extra) {
    pthread_t **p = (pthread_t **) extra;
    pthread_t tid_1 = *p[0];
    pthread_t tid_2 = *p[1];
    if(self_tid == tid_1) {
	printf("the 1st thread[tid=%" PRIu64 "] is going to yield...\n", tid_1);
	sched_yield();       
        printf("the 1st thread is resuming...\n");
    } else {
	printf("the 2nd thread[tid=%" PRIu64 "] is going to proceed...\n", tid_2);
    }
    return;
}


static void * start_txn(void * extra) {
    sleep(2);
    struct start_txn_arg * args = (struct start_txn_arg *) extra;
    DB_ENV * env = args -> env;
    DB * db = args->db;
    DB_TXN * parent = args->parent;
    printf("starting txn [thread %" PRIu64 "]\n", pthread_self());
    DB_TXN *txn;
    int r = env->txn_begin(env, parent, &txn,  DB_READ_COMMITTED);
    assert(r == 0);
    //do some random things...
    DBT key, data;
    dbt_init(&key, "hello", 6);
    dbt_init(&data, "world", 6);
    r = db->put(db, txn, &key, &data, 0);
    assert(r == 0);
   
    r = db->get(db, txn, &key, &data, 0);
    assert(r == 0);
   
    r = txn->commit(txn, 0);
    assert(r == 0);
    printf("%s done[thread %" PRIu64 "]\n", __FUNCTION__, pthread_self());
    return extra;
}

int test_main (int UU(argc), char * const UU(argv[])) {
    int r;
    toku_os_recursive_delete(TOKU_TEST_FILENAME);
    r = toku_os_mkdir(TOKU_TEST_FILENAME, S_IRWXU+S_IRWXG+S_IRWXO);
    assert(r == 0);

    DB_ENV *env;
    r = db_env_create(&env, 0);
    assert(r == 0);

    r = env->open(env, TOKU_TEST_FILENAME, DB_INIT_MPOOL|DB_CREATE|DB_THREAD |DB_INIT_LOCK|DB_INIT_LOG|DB_INIT_TXN|DB_PRIVATE, S_IRWXU+S_IRWXG+S_IRWXO);
    assert(r == 0);

    DB *db = NULL;
    r = db_create(&db, env, 0);
    assert(r == 0);

    r = db->open(db, NULL, "testit", NULL, DB_BTREE, DB_AUTO_COMMIT+DB_CREATE, S_IRWXU+S_IRWXG+S_IRWXO);
    assert(r == 0);

    DB_TXN * parent = NULL;
    r = env->txn_begin(env, 0, &parent, DB_READ_COMMITTED);
    assert(r == 0);

    pthread_t tid_1 = 0;
    pthread_t tid_2 = 0;
    pthread_t* callback_extra[2] = {&tid_1, &tid_2};
    toku_set_test_txn_sync_callback(test_callback, callback_extra);

    struct start_txn_arg args = {env, db, parent};

    r = pthread_create(&tid_1, NULL, start_txn, &args);
    assert(r==0);


    r= pthread_create(&tid_2, NULL, start_txn, &args);
    assert(r==0);

     void * ret; 
    r = pthread_join(tid_1, &ret);
    assert(r == 0);
    r = pthread_join(tid_2, &ret);
    assert(r == 0);

    r = parent->commit(parent, 0);
    assert(r ==0);

    r = db->close(db, 0);
    assert(r == 0);

    r = env->close(env, 0);
    assert(r == 0);

    return 0;
}

