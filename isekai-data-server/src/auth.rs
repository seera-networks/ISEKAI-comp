// SPDX-FileCopyrightText: 2025 SEERA Networks Corporation <info@seera-networks.com>
// SPDX-License-Identifier: MIT

use crate::CmdOptions;

pub fn authenticate_subject(cmd_opts: &CmdOptions, subject: &str) -> bool {
    if let Some(authorized_subject) = &cmd_opts.authorized_subject {
        return subject == authorized_subject;
    }
    // If no authorized subject is specified, allow all.
    return true;
}
