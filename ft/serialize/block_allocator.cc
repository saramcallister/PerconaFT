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

#include <algorithm>

#include <string.h>

#include "portability/memory.h"
#include "portability/toku_assert.h"
#include "portability/toku_stdint.h"
#include "portability/toku_stdlib.h"

#include "ft/serialize/block_allocator.h"

FILE *ba_trace_file = nullptr;

void block_allocator::maybe_initialize_trace(void) {
    const char *ba_trace_path = getenv("TOKU_BA_TRACE_PATH");        
    if (ba_trace_path != nullptr) {
        ba_trace_file = toku_os_fopen(ba_trace_path, "w");
        if (ba_trace_file == nullptr) {
            fprintf(stderr, "tokuft: error: block allocator trace path found in environment (%s), "
                            "but it could not be opened for writing (errno %d)\n",
                            ba_trace_path, get_maybe_error_errno());
        } else {
            fprintf(stderr, "tokuft: block allocator tracing enabled, path: %s\n", ba_trace_path);
        }
    }
}

void block_allocator::maybe_close_trace() {
    if (ba_trace_file != nullptr) {
        int r = toku_os_fclose(ba_trace_file);
        if (r != 0) {
            fprintf(stderr, "tokuft: error: block allocator trace file did not close properly (r %d, errno %d)\n",
                            r, get_maybe_error_errno());
        } else {
            fprintf(stderr, "tokuft: block allocator tracing finished, file closed successfully\n");
        }
    }
}


block_allocator::allocation_strategy  block_allocator::get_strategy(void) {
    return _strategy;
}

void block_allocator::set_strategy(enum allocation_strategy strategy) {
    _strategy = strategy;
}
