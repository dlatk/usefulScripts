#!/usr/bin/env python3
"""
duplicateMessageCheck.py

Created from interaction with GPT 5 Thinking

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

import argparse
import os
import sys
import re
import pymysql
from statistics import mean, median
from collections import defaultdict

# --- identifier safety: allow only alnum + underscore ---
_ID_RE = re.compile(r'^[A-Za-z0-9_]+$')

def _safe_ident(name: str, what: str) -> str:
    if not _ID_RE.match(name):
        sys.stderr.write(f"Invalid {what} identifier: {name}\n")
        sys.exit(2)
    return f"`{name}`"

def _sql_quote(val):
    """Return a single-quoted SQL literal with internal single quotes escaped."""
    s = str(val).replace("'", "''")
    return f"'{s}'"

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
    p.add_argument("--min_perc_as_partner", type=float, default=25.0,
                   help="Minimum percent (for either group) of messages that are duplicates to flag a partner pair (default: 25)")
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
    thr = float(args.min_perc_as_partner)
    partner_threshold = thr/100.0 if thr > 1.0 else thr

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
        sys.stderr.write(f"[ERROR] Connection error: {e}\n")
        sys.exit(1)

    # SQL for duplicates listing (exact message match; filter by TRIM length)
    sql_dupes = f"""
        SELECT
            MD5(m.message)                               AS msg_md5,
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
        WHERE m.message IS NOT NULL
          AND CHAR_LENGTH(TRIM(m.message)) >= %s
        ORDER BY d.mlen ASC, msg_md5, {gcol}, {midcol}
    """

    # SQL for total message counts per group (for high-fraction detection)
    sql_totals = f"""
        SELECT {gcol} AS group_id, COUNT(*) AS total_msgs
        FROM {table}
        WHERE message IS NOT NULL
          AND CHAR_LENGTH(TRIM(message)) >= %s
        GROUP BY {gcol}
    """

    # Stats
    total_dupe_groups = 0
    total_rows_listed = 0
    distinct_groups_global = set()
    groups_per_duplicate = []
    lengths_per_duplicate = []

    # Per-group partner stats:
    # group_partners[g][h] = {'count': number_of_duplicate_messages_shared, 'max_len': max_trimmed_length}
    group_partners = defaultdict(lambda: defaultdict(lambda: {'count': 0, 'max_len': 0}))

    # Load per-group totals
    group_totals = {}
    cur = conn.cursor()
    try:
        cur.execute(sql_totals, (min_chars,))
        for gid, tot in cur:
            group_totals[gid] = int(tot)
    except Exception as e:
        sys.stderr.write(f"[ERROR] Query error (totals): {e}\n")
        sys.exit(1)
    finally:
        cur.close()

    cur = conn.cursor()

    # State for current duplicate block (by msg_md5)
    current_md5 = None
    current_excerpt = ""
    current_length = None
    current_items = []      # list of (message_id, group_id)
    current_groups = set()

    def _update_partners_for_block():
        """Update partner stats for the current duplicate message block."""
        if not current_groups or current_length is None:
            return
        groups = list(current_groups)
        for i in range(len(groups)):
            a = groups[i]
            for j in range(len(groups)):
                if i == j:
                    continue
                b = groups[j]
                gp = group_partners[a][b]
                gp['count'] += 1
                if current_length > gp['max_len']:
                    gp['max_len'] = current_length

    def flush_block():
        nonlocal total_dupe_groups, total_rows_listed
        nonlocal current_md5, current_excerpt, current_length, current_items, current_groups
        if current_md5 is None or not current_items:
            return

        _update_partners_for_block()

        total_dupe_groups += 1
        total_rows_listed += len(current_items)
        distinct_groups_global.update(current_groups)
        groups_per_duplicate.append(len(current_groups))
        lengths_per_duplicate.append(current_length if current_length is not None else 0)

        if not (limit_groups and total_dupe_groups > limit_groups):
            excerpt = current_excerpt or ""
            excerpt_disp = (excerpt + "…") if len(excerpt) == excerpt_length else excerpt
            print(f"-> LENGTH={current_length:4d} chars  |  distinct_groups={len(current_groups)}  |  excerpt: \"{excerpt_disp}\"")
            for mid, gid in current_items:
                print(f"    message_id={mid}    group_id={gid}")
            print()

        # reset
        current_md5 = None
        current_excerpt = ""
        current_length = None
        current_items = []
        current_groups = set()

    try:
        cur.execute(sql_dupes, (excerpt_length, min_chars, args.min_groups, min_chars))
        for row in cur:
            # row: (msg_md5, excerpt, mlen, message_id, group_id)
            msg_md5, excerpt, mlen, message_id, group_id = row

            if msg_md5 != current_md5:
                flush_block()
                current_md5 = msg_md5
                current_excerpt = excerpt or ""
                current_length = int(mlen) if mlen is not None else None
                current_items = []
                current_groups = set()

            current_items.append((message_id, group_id))
            current_groups.add(group_id)

        flush_block()

    except Exception as e:
        sys.stderr.write(f"[ERROR] Query error (duplicates): {e}\n")
        sys.exit(1)
    finally:
        cur.close()
        conn.close()

    # -------- Per-group partner listing (before summary) --------
    print("==== PER-GROUP DUPLICATE PARTNERS with Multiple Matches ====")
    groups_view = []
    for g, partners in group_partners.items():
        kept = {h: stats for h, stats in partners.items() if stats['count'] >= 2}
        if not kept:
            continue
        sorted_partners = sorted(
            kept.items(),
            key=lambda kv: (-kv[1]['count'], -kv[1]['max_len'], str(kv[0]))
        )
        max_count = sorted_partners[0][1]['count'] if sorted_partners else 0
        groups_view.append((g, max_count, sorted_partners))

    if not groups_view:
        print("(none)\n")
    else:
        groups_view.sort(key=lambda item: (item[1], str(item[0])))  # by max dup count asc
        for g, max_count, sorted_partners in groups_view:
            print(f"group_id={g}  (max duplicate_messages with a single partner: {max_count})")
            for h, stats in sorted_partners:
                print(f"  with {h}: duplicate_messages={stats['count']}, max_len={stats['max_len']}")
            print()

    # -------- High-fraction duplicate partners (> threshold for either group) --------
    print(f"==== HIGH-FRACTION DUPLICATE PARTNERS ({partner_threshold:.2%}+ of messages duplicated for either group) ====")
    high_frac = []
    seen_pairs = set()
    for g, partners in group_partners.items():
        total_g = group_totals.get(g, 0)
        if total_g <= 0:
            continue
        for h, stats in partners.items():
            key = tuple(sorted((str(g), str(h))))
            if key in seen_pairs or g == h:
                continue
            seen_pairs.add(key)

            total_h = group_totals.get(h, 0)
            if total_h <= 0:
                continue
            dup_count = stats['count']
            frac_g = dup_count / total_g if total_g else 0.0
            frac_h = dup_count / total_h if total_h else 0.0
            max_len = max(
                stats['max_len'],
                group_partners[h][g]['max_len'] if h in group_partners and g in group_partners[h] else stats['max_len']
            )
            if (frac_g >= partner_threshold) or (frac_h >= partner_threshold):
                max_frac = max(frac_g, frac_h)
                high_frac.append((max_frac, g, h, dup_count, total_g, total_h, max_len, frac_g, frac_h))

    if not high_frac:
        print("(none)\n")
        groups_to_delete = []
    else:
        high_frac.sort(key=lambda x: (-x[0], -x[3], str(x[1]), str(x[2])))
        for max_frac, g, h, dup_count, total_g, total_h, max_len, frac_g, frac_h in high_frac:
            print(f"pair: {g} <-> {h}  |  duplicate_messages={dup_count}  |  totals=({g}:{total_g}, {h}:{total_h})  "
                  f"|  ratios=({g}:{frac_g:.3f}, {h}:{frac_h:.3f})  |  max_len={max_len}")
        print()

        # NEW: Collect ALL groups to delete: for each pair, pick the side with the higher ratio (ties -> both).
        to_delete_set = set()
        for _, g, h, _, _, _, _, frac_g, frac_h in high_frac:
            if frac_g > frac_h:
                to_delete_set.add(g)
            elif frac_h > frac_g:
                to_delete_set.add(h)
            else:
                to_delete_set.update([g, h])
        groups_to_delete = sorted(to_delete_set, key=lambda x: str(x))

    # -------- Single-line DELETE template for chosen group(s) --------
    if groups_to_delete:
        ids_list = ", ".join(_sql_quote(gid) for gid in groups_to_delete)
        print("==== DELETE TEMPLATE (NOT EXECUTED) ====")
        print("-- Review carefully and BACK UP your tables first.")
        print(f"DELETE FROM [TABLE] WHERE {gcol} IN ({ids_list});")
        print("-- Replace [TABLE] with the table(s) you wish to modify. This removes ALL rows whose group matches the chosen id(s).")
        print()

    # -------- Summary statistics --------
    print("==== SUMMARY STATISTICS ====")
    print(f"Duplicate exact-message texts found (groups): {total_dupe_groups:,}")
    print(f"Total duplicate rows listed:                 {total_rows_listed:,}")
    print(f"Distinct groups involved:                    {len(distinct_groups_global):,}")

    if groups_per_duplicate:
        print(f"Groups per duplicate - min/median/mean/max: "
              f"{min(groups_per_duplicate)} / {median(groups_per_duplicate):.2f} / "
              f"{mean(groups_per_duplicate):.2f} / {max(groups_per_duplicate)}")
    else:
        print("Groups per duplicate - min/median/mean/max: 0 / 0 / 0 / 0")

    if lengths_per_duplicate:
        print(f"Message length among duplicates - min/max:  {min(lengths_per_duplicate)} / {max(lengths_per_duplicate)}")
    else:
        print("Message length among duplicates - min/max:  0 / 0")

    if limit_groups and total_dupe_groups > limit_groups:
        print(f"[INFO] Printed {limit_groups} duplicate groups (limit). "
              f"{total_dupe_groups - limit_groups} additional groups were not printed.")

if __name__ == "__main__":
    main()

