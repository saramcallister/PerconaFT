/* -*- mode: C; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: expandtab:ts=8:sw=4:softtabstop=4:
// T(a) put 0
// T(b) put 0, should block
// T(c) put 0, should block
// T(a) commit
// T(b) put 0 succeeds
// T(b) commit
// T(c) put 0 succeeds

#include "test.h"
#include "toku_pthread.h"

static void insert_row(DB *db, DB_TXN *txn, int k, int v, int expect_r) {
    DBT key; dbt_init(&key, &k, sizeof k);
    DBT value; dbt_init(&value, &v, sizeof v);
    int r = db->put(db, txn, &key, &value, 0); assert(r == expect_r);
}

struct insert_one_arg {
    DB_TXN *txn;
    DB *db;
};

static void *insert_one(void *arg) {
    struct insert_one_arg *f_arg = (struct insert_one_arg *) arg;
    DB_TXN *txn = f_arg->txn;
    DB *db = f_arg->db;

    insert_row(db, txn, htonl(0), 0, 0);
    if (txn) {
        int r = txn->commit(txn, 0); assert(r == 0);
    }
    return arg;
}

static void simple_lockwait(DB_ENV *db_env, DB *db, int do_txn, int nrows, int ntxns) {
    int r;

    DB_TXN *txn_init = NULL;
    if (do_txn) {
        r = db_env->txn_begin(db_env, NULL, &txn_init, 0); assert(r == 0);
    }
    for (int k = 0; k < nrows; k++) {
        insert_row(db, txn_init, htonl(k), k, 0);
    }
    if (do_txn) {
        r = txn_init->commit(txn_init, 0); assert(r == 0);
    }

    DB_TXN *txns[ntxns];
    for (int i = 0; i < ntxns; i++) {
        txns[i] = NULL;
        if (do_txn) {
            r = db_env->txn_begin(db_env, NULL, &txns[i], 0); assert(r == 0);
        }
    }

    insert_row(db, txns[0], htonl(0), 0, 0);

    toku_pthread_t tids[ntxns];
    for (int i = 1 ; i < ntxns; i++) {
        struct insert_one_arg *XMALLOC(arg);
        *arg = (struct insert_one_arg) { txns[i], db};
        r = toku_pthread_create(&tids[i], NULL, insert_one, arg);
    }

    sleep(10);
    if (do_txn) {
        r = txns[0]->commit(txns[0], 0); assert(r == 0);
    }

    for (int i = 1; i < ntxns; i++) {
        void *ret = NULL;
        r = toku_pthread_join(tids[i], &ret); assert(r == 0); toku_free(ret);
    }
}

int test_main(int argc, char * const argv[]) {
    uint64_t cachesize = 0;
    uint32_t pagesize = 0;
    int do_txn = 1;
    int nrows = 1000;
    int ntxns = 2;
    const char *db_env_dir = ENVDIR;
    const char *db_filename = "simple_lockwait";
    int db_env_open_flags = DB_CREATE | DB_PRIVATE | DB_INIT_MPOOL | DB_INIT_TXN | DB_INIT_LOCK | DB_INIT_LOG | DB_THREAD;

    // parse_args(argc, argv);
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-v") == 0 || strcmp(argv[i], "--verbose") == 0) {
            verbose++;
            continue;
        }
        if (strcmp(argv[i], "-q") == 0 || strcmp(argv[i], "--quiet") == 0) {
            if (verbose > 0)
                verbose--;
            continue;
        }
        if (strcmp(argv[i], "--nrows") == 0 && i+1 < argc) {
            nrows = atoi(argv[++i]);
            continue;
        }
        if (strcmp(argv[i], "--ntxns") == 0 && i+1 < argc) {
            ntxns = atoi(argv[++i]);
            continue;
        }
        assert(0);
    }

    // setup env
    int r;
    char rm_cmd[strlen(db_env_dir) + strlen("rm -rf ") + 1];
    snprintf(rm_cmd, sizeof(rm_cmd), "rm -rf %s", db_env_dir);
    r = system(rm_cmd); assert(r == 0);

    r = toku_os_mkdir(db_env_dir, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH); assert(r == 0);

    DB_ENV *db_env = NULL;
    r = db_env_create(&db_env, 0); assert(r == 0);
    if (cachesize) {
        const u_int64_t gig = 1 << 30;
        r = db_env->set_cachesize(db_env, cachesize / gig, cachesize % gig, 1); assert(r == 0);
    }
    if (!do_txn)
        db_env_open_flags &= ~(DB_INIT_TXN | DB_INIT_LOG);
    r = db_env->open(db_env, db_env_dir, db_env_open_flags, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH); assert(r == 0);
#if TOKUDB
    r = db_env->set_lock_timeout(db_env, 30 * 1000); assert(r == 0);
#else
    r = db_env->set_lk_detect(db_env, DB_LOCK_YOUNGEST); assert(r == 0);
#endif

    // create the db
    DB *db = NULL;
    r = db_create(&db, db_env, 0); assert(r == 0);
    DB_TXN *create_txn = NULL;
    if (do_txn) {
        r = db_env->txn_begin(db_env, NULL, &create_txn, 0); assert(r == 0);
    }
    if (pagesize) {
        r = db->set_pagesize(db, pagesize); assert(r == 0);
    }
    r = db->open(db, create_txn, db_filename, NULL, DB_BTREE, DB_CREATE, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH); assert(r == 0);
    if (do_txn) {
        r = create_txn->commit(create_txn, 0); assert(r == 0);
    }

    // run test
    simple_lockwait(db_env, db, do_txn, nrows, ntxns);

    // close env
    r = db->close(db, 0); assert(r == 0); db = NULL;
    r = db_env->close(db_env, 0); assert(r == 0); db_env = NULL;

    return 0;
}
