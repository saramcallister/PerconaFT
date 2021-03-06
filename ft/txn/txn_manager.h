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

#include "portability/toku_portability.h"
#include "portability/toku_pthread.h"

#include "ft/txn/txn.h"

void set_test_txn_sync_callback(void (*) (pthread_t, void*), void*);
#define toku_test_txn_sync_callback(a) ((test_txn_sync_callback)? test_txn_sync_callback( a,test_txn_sync_callback_extra) : (void) 0)

#if TOKU_DEBUG_TXN_SYNC
#define toku_debug_txn_sync(a) toku_test_txn_sync_callback(a) 
#else
#define toku_debug_txn_sync(a) ((void) 0)
#endif

typedef struct txn_manager *TXN_MANAGER;

struct referenced_xid_tuple {
    TXNID begin_id;
    TXNID end_id;
    uint32_t references;
};

struct txn_manager {
    toku_mutex_t txn_manager_lock;  // a lock protecting this object
    txn_omt_t live_root_txns; // a sorted tree.
    xid_omt_t live_root_ids;    //contains TXNID x | x is snapshot txn
    TOKUTXN snapshot_head;
    TOKUTXN snapshot_tail;
    uint32_t num_snapshots;
    // Contains 3-tuples: (TXNID begin_id, TXNID end_id, uint64_t num_live_list_references)
    //                    for committed root transaction ids that are still referenced by a live list.
    rx_omt_t referenced_xids;

    TXNID last_xid;
    TXNID last_xid_seen_for_recover;
    TXNID last_calculated_oldest_referenced_xid;
};
typedef struct txn_manager *TXN_MANAGER;

struct txn_manager_state { 
    txn_manager_state(TXN_MANAGER mgr) :
        txn_manager(mgr),
        initialized(false) {
        snapshot_xids.create_no_array();
        referenced_xids.create_no_array();
        live_root_txns.create_no_array();
    }

    // should not copy construct
    txn_manager_state &operator=(txn_manager_state &rhs) = delete;
    txn_manager_state(txn_manager_state &rhs) = delete;

    ~txn_manager_state() {
        snapshot_xids.destroy();
        referenced_xids.destroy();
        live_root_txns.destroy();
    }

    void init();

    TXN_MANAGER txn_manager;
    bool initialized;

    // a snapshot of the txn manager's mvcc state
    // only valid if initialized = true
    xid_omt_t snapshot_xids;
    rx_omt_t referenced_xids;
    xid_omt_t live_root_txns;
};

// represents all of the information needed to run garbage collection
struct txn_gc_info {
    txn_gc_info(txn_manager_state *st, TXNID xid_sgc, TXNID xid_ip, bool mvcc)
        : txn_state_for_gc(st),
          oldest_referenced_xid_for_simple_gc(xid_sgc),
          oldest_referenced_xid_for_implicit_promotion(xid_ip),
          mvcc_needed(mvcc) {
    }

    // a snapshot of the transcation system. may be null.
    txn_manager_state *txn_state_for_gc;

    // the oldest xid in any live list
    //
    // suitible for simple garbage collection that cleans up multiple committed
    // transaction records into one. not suitible for implicit promotions, which
    // must be correct in the face of abort messages - see ftnode->oldest_referenced_xid
    TXNID oldest_referenced_xid_for_simple_gc;

    // lower bound on the oldest xid in any live when the messages to be cleaned
    // had no messages above them. suitable for implicitly promoting a provisonal uxr.
    TXNID oldest_referenced_xid_for_implicit_promotion;

    // whether or not mvcc is actually needed - false during recovery and non-transactional systems
    const bool mvcc_needed;
};

void toku_txn_manager_init(TXN_MANAGER* txn_manager);
void toku_txn_manager_destroy(TXN_MANAGER txn_manager);

TXNID toku_txn_manager_get_oldest_living_xid(TXN_MANAGER txn_manager);

TXNID toku_txn_manager_get_oldest_referenced_xid_estimate(TXN_MANAGER txn_manager);

void toku_txn_manager_handle_snapshot_create_for_child_txn(
    TOKUTXN txn,
    TXN_MANAGER txn_manager,
    TXN_SNAPSHOT_TYPE snapshot_type
    );
void toku_txn_manager_handle_snapshot_destroy_for_child_txn(
    TOKUTXN txn,
    TXN_MANAGER txn_manager,
    TXN_SNAPSHOT_TYPE snapshot_type
    );


// Assign a txnid. Log the txn begin in the recovery log. Initialize the txn live lists.
void toku_txn_manager_start_txn(
    TOKUTXN txn,
    TXN_MANAGER txn_manager,
    TXN_SNAPSHOT_TYPE snapshot_type,
    bool read_only
    );

void toku_txn_manager_start_txn_for_recovery(
    TOKUTXN txn,
    TXN_MANAGER txn_manager,
    TXNID xid
    );

void toku_txn_manager_finish_txn(TXN_MANAGER txn_manager, TOKUTXN txn);

void toku_txn_manager_clone_state_for_gc(
    TXN_MANAGER txn_manager,
    xid_omt_t* snapshot_xids,
    rx_omt_t* referenced_xids,
    xid_omt_t* live_root_txns
    );

void toku_txn_manager_id2txn_unlocked(TXN_MANAGER txn_manager, TXNID_PAIR txnid, TOKUTXN *result);

// Returns a root txn associated with xid. The system as a whole
// assumes that only root txns get prepared, adn therefore only
// root txns will have XIDs associated with them.
int toku_txn_manager_get_root_txn_from_xid (TXN_MANAGER txn_manager, TOKU_XA_XID *xid, DB_TXN **txnp);

uint32_t toku_txn_manager_num_live_root_txns(TXN_MANAGER txn_manager);

typedef int (*txn_mgr_iter_callback)(TOKUTXN txn, void* extra);

int toku_txn_manager_iter_over_live_txns(
    TXN_MANAGER txn_manager, 
    txn_mgr_iter_callback cb,
    void* extra
    );

int toku_txn_manager_iter_over_live_root_txns(
    TXN_MANAGER txn_manager, 
    txn_mgr_iter_callback cb,
    void* extra
    );

int toku_txn_manager_recover_root_txn(
    TXN_MANAGER txn_manager,
    struct tokulogger_preplist preplist[/*count*/],
    long count,
    long *retp, /*out*/
    uint32_t flags
    );

void toku_txn_manager_suspend(TXN_MANAGER txn_manager);
void toku_txn_manager_resume(TXN_MANAGER txn_manager);

void toku_txn_manager_set_last_xid_from_logger(TXN_MANAGER txn_manager, TXNID last_xid);
void toku_txn_manager_set_last_xid_from_recovered_checkpoint(TXN_MANAGER txn_manager, TXNID last_xid);
TXNID toku_txn_manager_get_last_xid(TXN_MANAGER mgr);

bool toku_txn_manager_txns_exist(TXN_MANAGER mgr);

// Test-only function
void toku_txn_manager_increase_last_xid(TXN_MANAGER mgr, uint64_t increment);

TXNID toku_get_youngest_live_list_txnid_for(TXNID xc, const xid_omt_t &snapshot_txnids, const rx_omt_t &referenced_xids);
