import argparse
import os
import re
import glob                         
import pandas as pd
import oracledb
from datetime import datetime, timedelta

# ──────────────────────────────────────────────────────────────────────────────
# Argument Parsing
# ──────────────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Validate file or table data quality")

# Table validation
parser.add_argument("--table_name",           help="Table name to validate")
parser.add_argument("--pk_column",            help="Primary key column name")
parser.add_argument("--date_column",          help="Date column for freshness check (integer surrogate key, e.g. 20240101)")
parser.add_argument("--execution_date",       help="Expected date in YYYY-MM-DD format")
parser.add_argument("--skip_freshness_check", action="store_true",
                    help="Skip freshness check if no data exists")

# File validation
parser.add_argument("--file_path",            help="Direct file path or glob pattern")
parser.add_argument("--file_pattern",         help="Alternative glob pattern parameter")
parser.add_argument("--delimiter",            default="|",
                    help="CSV delimiter (default: |)")
parser.add_argument("--allow_missing_file",   action="store_true",
                    help="Don't fail if file doesn't exist (useful for optional extracts)")
parser.add_argument("--search_days_back",     type=int, default=0,
                    help="Search for files from N days back if current date not found")

# Common
parser.add_argument("--mandatory_columns",    required=True,
                    help="Comma-separated list of required columns")
parser.add_argument("--numeric_columns",      help="Comma-separated list of numeric columns")
parser.add_argument("--flag_columns",         help="Comma-separated list of Y/N flag columns")
parser.add_argument("--min_rows",             required=True, type=int,
                    help="Minimum required row count")

# DB connection
parser.add_argument("--db_user",              default="system",
                    help="Database user")
parser.add_argument("--db_password",          default="oracle123",
                    help="Database password")
parser.add_argument("--db_dsn",               default="host.docker.internal/orcl",
                    help="Database DSN")

args = parser.parse_args()

# Parse comma-separated lists
mandatory_columns = [c.strip() for c in args.mandatory_columns.split(",")]
numeric_columns   = [c.strip() for c in args.numeric_columns.split(",")] if args.numeric_columns else []
flag_columns      = [c.strip() for c in args.flag_columns.split(",")]    if args.flag_columns    else []

# embedding it in any SQL string, preventing SQL injection from the CLI.
_SAFE_IDENT = re.compile(r'^[A-Za-z0-9_$#]{1,128}$')

def _safe_table(name: str) -> str:
    """Raise if name contains characters that could enable SQL injection."""
    if not _SAFE_IDENT.match(name):
        raise ValueError(
            f"Unsafe table/column name rejected: '{name}'. "
            "Only alphanumeric characters, _, $, # are allowed."
        )
    return name


# ──────────────────────────────────────────────────────────────────────────────
# FILE VALIDATION
# ──────────────────────────────────────────────────────────────────────────────
if args.file_path or args.file_pattern:

    pattern = args.file_pattern if args.file_pattern else args.file_path

    # Resolve file path (glob or direct)
    if '*' in pattern or '?' in pattern:
        files = sorted(glob.glob(pattern), reverse=True)

        # not every occurrence of the 8-digit string in the full path.
        if not files and args.search_days_back > 0:
            basename = os.path.basename(pattern)
            dirpart  = os.path.dirname(pattern)
            date_match = re.search(r'(\d{8})', basename)

            if date_match:
                original_date_str = date_match.group(1)
                original_date     = datetime.strptime(original_date_str, '%Y%m%d')

                for days_back in range(1, args.search_days_back + 1):
                    prev_date_str  = (original_date - timedelta(days=days_back)).strftime('%Y%m%d')
                    prev_basename  = basename.replace(original_date_str, prev_date_str, 1)
                    prev_pattern   = os.path.join(dirpart, prev_basename)
                    prev_files     = sorted(glob.glob(prev_pattern), reverse=True)
                    if prev_files:
                        files = prev_files
                        break

        if not files:
            pattern_dir = os.path.dirname(pattern)
            if os.path.exists(pattern_dir):
                dir_files = sorted(os.listdir(pattern_dir))
                shown = dir_files[:10]
                extra = len(dir_files) - len(shown)
                listing = "\n  ".join(shown) + (f"\n  ... and {extra} more" if extra > 0 else "")
                print(f"Contents of {pattern_dir}:\n  {listing}")
            else:
                print(f"Directory does not exist: {pattern_dir}")

            if args.allow_missing_file:
                print(f"WARNING: no file matched '{pattern}' — skipping (--allow_missing_file)")
                exit(0)
            raise FileNotFoundError(
                f"No file found matching pattern: {pattern}\n"
                "  Use --allow_missing_file to skip, or --search_days_back N "
                "to search previous dates."
            )

        file_path = files[0]
    else:
        file_path = pattern
        if not os.path.exists(file_path):
            if args.allow_missing_file:
                print(f"WARNING: file not found '{file_path}' — skipping (--allow_missing_file)")
                exit(0)
            raise FileNotFoundError(f"File not found: {file_path}")

    # Read CSV
    try:
        df = pd.read_csv(file_path, delimiter=args.delimiter)
    except Exception as e:
        raise ValueError(f"Failed to read file '{file_path}': {e}") from e

    df.columns = df.columns.str.strip()

    # ── Row count ────────────────────────────────────────────────────────────
    if len(df) < args.min_rows:
        raise ValueError(
            f"Row count {len(df):,} is below the required minimum {args.min_rows:,} "
            f"in '{os.path.basename(file_path)}'"
        )

    # ── Mandatory columns ────────────────────────────────────────────────────
    missing_cols = [c for c in mandatory_columns if c not in df.columns]
    if missing_cols:
        raise ValueError(
            f"Missing mandatory columns: {missing_cols}\n"
            f"  Available: {list(df.columns)}"
        )

    null_issues = [
        f"{c}: {df[c].isnull().sum():,} NULL values"
        for c in mandatory_columns if df[c].isnull().sum() > 0
    ]
    if null_issues:
        raise ValueError("NULL values in mandatory columns:\n  " + "\n  ".join(null_issues))

    # ── Numeric columns ──────────────────────────────────────────────────────
    if numeric_columns:
        numeric_issues = []
        for col in numeric_columns:
            if col not in df.columns:
                raise ValueError(f"Missing numeric column: {col}")

            # so that originally-blank/NaN cells are not counted as conversion failures.
            original_nonempty = (
                df[col]
                .astype(str)
                .str.strip()
                .str.upper()
                .replace({'': False, 'NAN': False, 'NONE': False, 'NULL': False})
                .apply(lambda v: bool(v) if not isinstance(v, bool) else v)
            )

            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .str.replace(',',   '', regex=False)
                .str.replace('₹',  '', regex=False)
                .str.replace('$',  '', regex=False)
                .str.replace('Rs.','', regex=False)
                .str.replace('Rs', '', regex=False)
                .replace(['', 'nan', 'None', 'NULL', 'null'], None)
            )
            converted = pd.to_numeric(df[col], errors='coerce')

            # A conversion failure = was non-empty before cleaning, is NaN after
            failed_mask  = converted.isnull() & df[col].notna()
            failed_count = failed_mask.sum()
            if failed_count > 0:
                bad_vals = df[col][failed_mask].unique()[:5]
                numeric_issues.append(f"{col}: {failed_count} invalid values (e.g. {list(bad_vals)})")

            df[col] = converted

        if numeric_issues:
            raise ValueError("Invalid numeric values:\n  " + "\n  ".join(numeric_issues))

    # ── Flag columns ─────────────────────────────────────────────────────────
    if flag_columns:
        flag_issues = []
        for col in flag_columns:
            if col not in df.columns:
                raise ValueError(f"Missing flag column: {col}")
            
            # NaN cells don't become the string "NAN" and falsely fail the Y/N check.
            cleaned = df[col].fillna('').astype(str).str.strip().str.upper()
            invalid = cleaned[~cleaned.isin(['Y', 'N', ''])]
            if len(invalid) > 0:
                flag_issues.append(
                    f"{col}: {len(invalid)} invalid values {list(invalid.unique()[:5])}"
                )
            # Also catch genuine nulls / empties as invalid flags
            empty_count = (cleaned == '').sum()
            if empty_count > 0:
                flag_issues.append(f"{col}: {empty_count} empty/NULL values (expected Y or N)")

        if flag_issues:
            raise ValueError("Invalid flag values (expected Y/N):\n  " + "\n  ".join(flag_issues))

    # ── PK duplicate check ───────────────────────────────────────────────────
    if args.pk_column:
        if args.pk_column not in df.columns:
            raise ValueError(f"Primary key column not found: {args.pk_column}")
        dupes = df[df.duplicated(subset=[args.pk_column], keep=False)]
        if len(dupes) > 0:
            examples = list(dupes[args.pk_column].unique()[:5])
            raise ValueError(
                f"Found {len(dupes):,} duplicate rows on '{args.pk_column}'. "
                f"Examples: {examples}"
            )

    # ── Summary ──────────────────────────────────────────────────────────────
    file_kb = os.path.getsize(file_path) / 1024
    parts = [
        f"FILE OK | {os.path.basename(file_path)} | "
        f"{file_kb:.1f} KB | {len(df):,} rows × {len(df.columns)} cols",
        f"  mandatory={len(mandatory_columns)}"
        + (f" | numeric={len(numeric_columns)}" if numeric_columns else "")
        + (f" | flags={len(flag_columns)}"       if flag_columns    else "")
        + (f" | pk={args.pk_column}"             if args.pk_column  else ""),
    ]
    print("\n".join(parts))
    exit(0)


# ──────────────────────────────────────────────────────────────────────────────
# TABLE VALIDATION  (Oracle)
# ──────────────────────────────────────────────────────────────────────────────
if args.table_name:

    
    _safe_table(args.table_name)
    if args.pk_column:
        _safe_table(args.pk_column)
    if args.date_column:
        _safe_table(args.date_column)

    try:
        conn = oracledb.connect(
            user=args.db_user,
            password=args.db_password,
            dsn=args.db_dsn
        )
        cur = conn.cursor()
    except Exception as e:
        raise ConnectionError(f"Database connection failed: {e}") from e

    # Verify table exists
    try:
        cur.execute(f"SELECT COUNT(*) FROM {args.table_name} WHERE ROWNUM <= 1")
        cur.fetchone()
    except Exception as e:
        cur.close(); conn.close()
        raise ValueError(f"Table not found or inaccessible: {args.table_name} — {e}") from e

    # ── Row count ────────────────────────────────────────────────────────────
    cur.execute(f"SELECT COUNT(*) FROM {args.table_name}")
    row_count = cur.fetchone()[0]
    if row_count < args.min_rows:
        cur.close(); conn.close()
        raise ValueError(
            f"Row count {row_count:,} is below the required minimum {args.min_rows:,} "
            f"in table '{args.table_name}'"
        )

    # ── Mandatory columns (NULL check) ───────────────────────────────────────
    null_issues = []
    for col in mandatory_columns:
        _safe_table(col)   # validate each column name too
        try:
            cur.execute(f"SELECT COUNT(*) FROM {args.table_name} WHERE {col} IS NULL")
            null_count = cur.fetchone()[0]
            if null_count > 0:
                null_issues.append(f"{col}: {null_count:,} NULL values")
        except Exception as e:
            cur.close(); conn.close()
            raise ValueError(f"Column not found or error on '{col}': {e}") from e

    if null_issues:
        cur.close(); conn.close()
        raise ValueError("NULL values in mandatory columns:\n  " + "\n  ".join(null_issues))

    # ── PK duplicate check ───────────────────────────────────────────────────
    if args.pk_column:
        
        duplicate_count = 0
        try:
            cur.execute(f"""
                SELECT COUNT(*) FROM (
                    SELECT {args.pk_column}
                    FROM   {args.table_name}
                    GROUP  BY {args.pk_column}
                    HAVING COUNT(*) > 1
                )
            """)
            duplicate_count = cur.fetchone()[0]

            if duplicate_count > 0:
                cur.execute(f"""
                    SELECT {args.pk_column}, COUNT(*) AS cnt
                    FROM   {args.table_name}
                    GROUP  BY {args.pk_column}
                    HAVING COUNT(*) > 1
                    FETCH FIRST 5 ROWS ONLY
                """)
                examples = [f"{r[0]} ({r[1]}×)" for r in cur.fetchall()]
                cur.close(); conn.close()
                raise ValueError(
                    f"Found {duplicate_count:,} duplicate PKs on '{args.pk_column}'. "
                    f"Examples: {', '.join(examples)}"
                )
        except oracledb.Error as e:
            cur.close(); conn.close()
            raise ValueError(f"Error checking PK column '{args.pk_column}': {e}") from e

    # ── Freshness check ──────────────────────────────────────────────────────
    # NOTE: date_column is assumed to be an INTEGER surrogate key (e.g. 20240101).
    # If the column is an Oracle DATE/TIMESTAMP type, bind a proper date string instead.
    if args.date_column and args.execution_date:
        try:
            exec_date = datetime.strptime(args.execution_date, "%Y-%m-%d")   # validate format
        except ValueError:
            cur.close(); conn.close()
            raise ValueError(
                f"Invalid --execution_date format: '{args.execution_date}' (expected YYYY-MM-DD)"
            )

        date_key = int(args.execution_date.replace("-", ""))   # e.g. 20240101

        # For fact tables: confirm the date exists in dim_date first
        if args.table_name.lower().startswith('fact'):
            try:
                cur.execute(
                    "SELECT COUNT(*) FROM dim_date WHERE date_id = :1", [date_key]
                )
                if cur.fetchone()[0] == 0:
                    msg = (
                        f"Date {args.execution_date} ({date_key}) not found in dim_date. "
                        "Load dim_date first or use --skip_freshness_check."
                    )
                    if args.skip_freshness_check:
                        print(f"WARNING: {msg}")
                    else:
                        cur.close(); conn.close()
                        raise ValueError(msg)
            except oracledb.Error as e:
                if "table or view does not exist" in str(e).lower():
                    pass   # dim_date not yet loaded — silently skip
                else:
                    cur.close(); conn.close()
                    raise

        # Check records exist for the execution date
        try:
            cur.execute(
                f"SELECT COUNT(*) FROM {args.table_name} WHERE {args.date_column} = :1",
                [date_key]
            )
            record_count = cur.fetchone()[0]
            if record_count == 0:
                msg = (
                    f"No data for date {args.execution_date} in "
                    f"'{args.table_name}'.{args.date_column}. "
                    "Use --skip_freshness_check to allow."
                )
                if args.skip_freshness_check:
                    print(f"WARNING: {msg}")
                else:
                    cur.close(); conn.close()
                    raise ValueError(msg)
        except oracledb.Error as e:
            cur.close(); conn.close()
            raise ValueError(
                f"Error checking date column '{args.date_column}': {e}"
            ) from e

    # ── Numeric columns ──────────────────────────────────────────────────────
    if numeric_columns:
        for col in numeric_columns:
            _safe_table(col)
            try:
                cur.execute(f"""
                    SELECT MIN({col}), MAX({col}), AVG({col}),
                           COUNT(*), COUNT({col})
                    FROM   {args.table_name}
                """)
                mn, mx, avg, total, non_null = cur.fetchone()
                
                avg_str = f"{avg:.2f}" if avg is not None else "N/A"
                _ = f"{col}: min={mn}, max={mx}, avg={avg_str}, non-null={non_null:,}/{total:,}"
                # Stats are available here if you want to log them; we surface them in summary
            except Exception as e:
                cur.close(); conn.close()
                raise ValueError(f"Error validating numeric column '{col}': {e}") from e

    # ── Summary ──────────────────────────────────────────────────────────────
    cur.close()
    conn.close()

    parts = [
        f"TABLE OK | {args.table_name} | {row_count:,} rows",
        f"  mandatory={len(mandatory_columns)}"
        + (f" | pk={args.pk_column}"          if args.pk_column  else "")
        + (f" | freshness={args.execution_date}" if args.date_column and args.execution_date else "")
        + (f" | numeric={len(numeric_columns)}"  if numeric_columns else ""),
    ]
    print("\n".join(parts))
    exit(0)


# ──────────────────────────────────────────────────────────────────────────────
# No target specified
# ──────────────────────────────────────────────────────────────────────────────
raise SystemExit(
    "ERROR: no validation target specified.\n"
    "  File : --file_path data.csv  --mandatory_columns id,name --min_rows 100\n"
    "  Table: --table_name dim_product --mandatory_columns product_id --min_rows 1000"
)