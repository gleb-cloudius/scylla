/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "service/raft/bounded_clock_adjtimex.hh"

#include <sys/timex.h>

namespace service {

std::optional<raft::time_bounds> bounded_clock_adjtimex::interval_now() {
    // modes == 0 makes this a pure read of the kernel's NTP state; no
    // adjustment is applied.
    struct timex tx = {};
    const int state = ntp_adjtime(&tx);

    // The clock is only trustworthy when it is synchronized. ntp_adjtime()
    // returns TIME_ERROR (and sets STA_UNSYNC) once the kernel's estimated
    // maximum error grows past its limit, or when no NTP source is disciplining
    // the clock. In that case we cannot bound the error, so report nullopt and
    // let the caller fall back to the safe path.
    if (state < 0 || state == TIME_ERROR || (tx.status & STA_UNSYNC)) {
        return std::nullopt;
    }

    // tx.time is the CLOCK_REALTIME value sampled at the same instant as
    // tx.maxerror, so the error bound applies to exactly this timestamp.
    // tx.time.tv_usec is in nanoseconds when STA_NANO is set, otherwise
    // microseconds.
    const auto subsecond = (tx.status & STA_NANO)
            ? std::chrono::nanoseconds(tx.time.tv_usec)
            : std::chrono::nanoseconds(std::chrono::microseconds(tx.time.tv_usec));
    const auto now = std::chrono::system_clock::time_point(
            std::chrono::duration_cast<std::chrono::system_clock::duration>(
                    std::chrono::seconds(tx.time.tv_sec) + subsecond));

    // tx.maxerror is the kernel's conservative maximum error, always in
    // microseconds regardless of STA_NANO. The kernel grows it at the maximum
    // assumed drift rate between NTP updates and resets it on each update, so it
    // is a sound upper bound on |true_time - CLOCK_REALTIME| at tx.time.
    const auto error = std::chrono::microseconds(tx.maxerror);
    return raft::time_bounds{now - error, now + error};
}

} // namespace service
