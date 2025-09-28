#!/usr/bin/env python3
"""
duplicateMessageCheck.py

Create from interaction with GPT 5 Thinking

Find messages that are EXACT duplicates appearing under 2+ distinct groups (e.g., users),
and list the message_ids and group_ids for each such duplicate, along with the first 30
characters of the message.

Usage:
  python3 duplicateMessageCheck.py -d DBNAME -t TABLENAME -g GROUP_FIELD --message_id MESSAGE_ID_FIELD

Examples:
  python3 duplicateMessageCheck.py -d HiTOP -t trns_prtcpnt_v5 -g user_id --message_id message_id


Use with mesage index:
ALTER TABLE trns_prtcpnt_v5 ADD INDEX idx_message_md5 ( (MD5(message)) );
ALTER TABLE trns_prtcpnt_v5 ADD INDEX idx_message (message(255));
-- And optionally: (assuming user_id is 50 chars)
ALTER TABLE trns_prtcpnt_v5 ADD INDEX idx_message_user (message(190), user_id);

Outputs duplicate groups sorted by trimmed message length (shortest -> longest).
For each duplicate group prints message_id, group_id, and the first 30 characters.

"""


#!/usr/bin/env python3
"""
duplicateMessageCheck.py

Find messages that are EXACT duplicates across >= --min-groups distinct groups,
matching on the FULL, EXACT message text (case/whitespace sensitive). Messages
with TRIM(message) length < --min_chars_to_count are ignored entirely.

Outputs duplicate groups sorted by trimmed message length (shortest -> longest).
For each duplicate group prints message_id, group_id, and the first N characters
of the message (N = --excerpt_length, default 196).

Example:
  python3 duplicateMessageCheck.py -d HiTOP -t trns_prtcpnt_v5 -g user_id --message_id message_id
"""

import argparse
import os
import sys
import re
import pymysql
from statistics import mean, median

# --- identifier safety: allow only alnum + underscore ---
_ID_RE = re.compile(r'^[A-Za-z0-9_]+$')

def _safe_ident(name: str, what: str) -> str:
    if not _ID_RE.match(name):
        sys.stderr.write(f"Invalid {what} identifier: {name}\n")
        sys.exit(2)
    return f"`{name}`"

def parse_args():
    p = argparse.ArgumentParser(description="Find exact duplicate messages across 2+ groups (ignoring very short messages).")
    p.add_argument("-d", "--database", required=True, help="Database name")
    p.add_argument("-t", "--table",    required=True, help="Table (corpus) name. Must contain 'message' column.")
    p.add_argument("-g", "--group",    required=True, help="Group-by field (e.g., user_id) to check duplicates across")
    p.add_argument("--message_id",     default="message_id", help="Message ID field name (default: message_id)")
    p.add_argument("--min-groups",     type=int, default=2, help="Minimum distinct groups (default: 2)")
    p.add_argument("--min_chars_to_count", type=int, default=32,
                   help="Minimum TRIM(message) length to consider at all (default: 32)")
    p.add_argument("--excerpt_length", type=int, default=196,
                   help="Number of leading characters to show from each message (default: 196)")
    p.add_argument("--limit",          type=int, default=0,
                   help="Optional limit on number of duplicate message groups to print (0 = no limit)")
    return p.parse_args()

def main():
    args = parse_args()
    if args.min_groups < 2:
        sys.stderr.write("--min-groups must be >= 2\n")
        sys.exit(2)

    db     = args.database
    table  = _safe_ident(args.table, "table")
    gcol   = _safe_ident(args.group, "group field")
    midcol = _safe_ident(args.message_id, "message_id field")
    min_chars      = max(0, int(args.min_chars_to_count))
    limit_groups   = max(0, int(args.limit))
    excerpt_length = max(1, int(args.excerpt_length))

    # Connect using ~/.my.cnf
    try:
        conn = pymysql.connect(
            read_default_file=os.path.expanduser("~/.my.cnf"),
            database=db,
            cursorclass=pymysql.cursors.SSCursor,   # streaming cursor
            charset="utf8mb4",
            autocommit=True,
        )
    except Exception as e:
        sys.stderr.write(f"Connection error: {e}\n")
        sys.exit(1)

    # SQL:
    #  - Subquery d: group by EXACT message, keep only those with >= min_groups distinct groups;
    #    apply TRIM-length filter there so very short messages are ignored entirely.
    #  - Join back on exact message equality to list all rows for those messages.
    #  - Order by trimmed length ascending so we print shortest -> longest.
    #  - SUBSTRING length is parameterized by excerpt_length.
    sql = f"""
        SELECT
            MD5(m.message)                               AS msg_md5,       -- for grouping in client
            SUBSTRING(m.message, 1, %s)                  AS excerpt,
            CHAR_LENGTH(TRIM(m.message))                 AS mlen,
            {midcol}                                     AS message_id,
            {gcol}                                       AS group_id
        FROM {table} AS m
        JOIN (
            SELECT message, CHAR_LENGTH(TRIM(message)) AS mlen
            FROM {table}
            WHERE message IS NOT NULL
              AND CHAR_LENGTH(TRIM(message)) >= %s
            GROUP BY message
            HAVING COUNT(DISTINCT {args.group}) >= %s
        ) AS d
          ON d.message = m.message
        WHERE m.m

