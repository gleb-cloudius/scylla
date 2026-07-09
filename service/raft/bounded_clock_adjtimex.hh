/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "raft/bounded_clock.hh"

namespace service {

// Concrete raft::bounded_clock backend that reads the local clock's error
// bounds from the Linux kernel's NTP discipline via ntp_adjtime(2)/adjtimex(2).
// Requires no external service at runtime beyond an NTP daemon (chrony/ntpd)
// disciplining the kernel clock; the error bound is read with a single
// non-blocking syscall. Returns nullopt while the clock is unsynchronized
// (STA_UNSYNC / TIME_ERROR), so callers fall back to the safe path.
//
// This lives outside the raft library because it is platform-specific env glue,
// like the other concrete raft integrations in service/raft/.
class bounded_clock_adjtimex final : public raft::bounded_clock {
public:
    std::optional<raft::time_bounds> interval_now() override;
};

} // namespace service
