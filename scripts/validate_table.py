import argparse
import os
import glob
import pandas as pd
import oracledb
from datetime import datetime, timedelta

print("🔍 DATA VALIDATION SCRIPT")
print(f"⏰ Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'='*70}\n")

# --------------------------------------------------
# Argument Parsing
# --------------------------------------------------
parser = argparse.ArgumentParser(description="Validate file or table data quality")

# Table validation arguments
parser.add_argument("--table_name", help="Table name to validate")
parser.add_argument("--pk_column", help="Primary key column name")
parser.add_argument("--date_column", help="Date column for freshness check")
parser.add_argument("--execution_date", help="Expected date in YYYY-MM-DD format")
parser.add_argument("--skip_freshness_check", action="store_true", 
                   help="Skip freshness check if no data exists")

# File validation arguments
parser.add_argument("--file_path", help="Direct file path or glob pattern")
parser.add_argument("--file_pattern", help="Alternative glob pattern parameter")
parser.add_argument("--delimiter", default="|", help="CSV delimiter (default: |)")
parser.add_argument("--allow_missing_file", action="store_true",
                   help="Don't fail if file doesn't exist (useful for optional extracts)")
parser.add_argument("--search_days_back", type=int, default=0,
                   help="Search for files from N days back if current date not found")

# Common validation arguments
parser.add_argument("--mandatory_columns", required=True, 
                   help="Comma-separated list of required columns")
parser.add_argument("--numeric_columns", 
                   help="Comma-separated list of numeric columns")
parser.add_argument("--flag_columns", 
                   help="Comma-separated list of Y/N flag columns")
parser.add_argument("--min_rows", required=True, type=int, 
                   help="Minimum required row count")

# Database connection arguments
parser.add_argument("--db_user", default="system", help="Database user")
parser.add_argument("--db_password", default="oracle123", help="Database password")
parser.add_argument("--db_dsn", default="host.docker.internal/orcl", help="Database DSN")

args = parser.parse_args()

# Parse comma-separated arguments
mandatory_columns = [col.strip() for col in args.mandatory_columns.split(",")]
numeric_columns = [col.strip() for col in args.numeric_columns.split(",")] if args.numeric_columns else []
flag_columns = [col.strip() for col in args.flag_columns.split(",")] if args.flag_columns else []
min_rows = args.min_rows

# ==================================================
# 📄 FILE VALIDATION LOGIC
# ==================================================
if args.file_path or args.file_pattern:
    print("🗂️  FILE VALIDATION MODE")
    print(f"{'='*70}\n")
    
    # Determine which parameter to use
    pattern = args.file_pattern if args.file_pattern else args.file_path
    
    print(f"🔍 Searching for files with pattern: {pattern}")
    
    # Find matching files
    if '*' in pattern or '?' in pattern:
        files = sorted(glob.glob(pattern), reverse=True)
        
        # If no files found and search_days_back is set, try previous dates
        if not files and args.search_days_back > 0:
            print(f"⚠️  No files found for current pattern")
            print(f"🔄 Searching up to {args.search_days_back} days back...\n")
            
            # Extract date pattern from the file path
            for days_back in range(1, args.search_days_back + 1):
                # Try to find date in pattern (YYYYMMDD format)
                import re
                date_match = re.search(r'(\d{8})', pattern)
                
                if date_match:
                    original_date_str = date_match.group(1)
                    original_date = datetime.strptime(original_date_str, '%Y%m%d')
                    prev_date = original_date - timedelta(days=days_back)
                    prev_date_str = prev_date.strftime('%Y%m%d')
                    
                    # Replace date in pattern
                    prev_pattern = pattern.replace(original_date_str, prev_date_str)
                    print(f"   Trying: {prev_pattern}")
                    
                    prev_files = sorted(glob.glob(prev_pattern), reverse=True)
                    if prev_files:
                        files = prev_files
                        print(f"   ✅ Found file from {days_back} day(s) ago\n")
                        break
        
        if not files:
            # List directory contents to help debug
            pattern_dir = os.path.dirname(pattern)
            if os.path.exists(pattern_dir):
                print(f"\n📂 Contents of {pattern_dir}:")
                dir_files = sorted(os.listdir(pattern_dir))
                if dir_files:
                    for f in dir_files[:10]:  # Show first 10 files
                        print(f"   - {f}")
                    if len(dir_files) > 10:
                        print(f"   ... and {len(dir_files) - 10} more files")
                else:
                    print(f"   (directory is empty)")
            else:
                print(f"\n⚠️  Directory does not exist: {pattern_dir}")
            
            if args.allow_missing_file:
                print(f"\n⚠️  WARNING: No file found matching pattern (allowed by --allow_missing_file)")
                print(f"   Pattern: {pattern}")
                print(f"   Skipping validation\n")
                exit(0)
            else:
                raise FileNotFoundError(
                    f"❌ No file found matching pattern: {pattern}\n"
                    f"   Use --allow_missing_file to skip validation when file is missing\n"
                    f"   Use --search_days_back N to search previous dates"
                )
        
        file_path = files[0]
        print(f"📄 Found {len(files)} file(s), validating latest: {os.path.basename(file_path)}")
    else:
        # Direct file path
        file_path = pattern
        if not os.path.exists(file_path):
            if args.allow_missing_file:
                print(f"\n⚠️  WARNING: File not found (allowed by --allow_missing_file)")
                print(f"   File: {file_path}")
                print(f"   Skipping validation\n")
                exit(0)
            else:
                raise FileNotFoundError(f"❌ File not found: {file_path}")
        print(f"📄 Validating file: {os.path.basename(file_path)}")

    print(f"📂 Full path: {file_path}\n")
    
    # Get file size
    file_size = os.path.getsize(file_path)
    print(f"📊 File size: {file_size:,} bytes ({file_size / 1024:.2f} KB)\n")
    
    # Read file
    try:
        df = pd.read_csv(file_path, delimiter=args.delimiter)
        print(f"✅ File loaded successfully: {len(df):,} rows, {len(df.columns)} columns\n")
    except Exception as e:
        raise ValueError(f"❌ Failed to read file: {str(e)}")

    # Normalize column names (strip whitespace, handle case sensitivity)
    df.columns = df.columns.str.strip()
    
    print("📋 Detected columns:")
    for i, col in enumerate(df.columns, 1):
        print(f"   {i:2d}. {col}")
    print()

    # ----------------------------
    # 1️⃣ Row count validation
    # ----------------------------
    print(f"1️⃣  ROW COUNT VALIDATION")
    if len(df) < min_rows:
        raise ValueError(f"❌ Row count {len(df):,} is less than minimum required {min_rows:,}")
    print(f"✅ Row count OK: {len(df):,} (minimum: {min_rows:,})\n")

    # ----------------------------
    # 2️⃣ Mandatory columns validation
    # ----------------------------
    print(f"2️⃣  MANDATORY COLUMNS VALIDATION")
    missing_columns = []
    for col in mandatory_columns:
        if col not in df.columns:
            missing_columns.append(col)
    
    if missing_columns:
        raise ValueError(
            f"❌ Missing mandatory columns: {missing_columns}\n"
            f"   Available columns: {list(df.columns)}"
        )
    
    # Check for NULL values
    null_issues = []
    for col in mandatory_columns:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            null_issues.append(f"{col}: {null_count} NULL values")
    
    if null_issues:
        raise ValueError(f"❌ NULL values found in mandatory columns:\n   " + "\n   ".join(null_issues))
    
    print(f"✅ All {len(mandatory_columns)} mandatory columns present with no NULLs:")
    for col in mandatory_columns:
        print(f"   ✓ {col}")
    print()

    # ----------------------------
    # 3️⃣ Numeric columns validation
    # ----------------------------
    if numeric_columns:
        print(f"3️⃣  NUMERIC COLUMNS VALIDATION")
        numeric_issues = []
        
        for col in numeric_columns:
            if col not in df.columns:
                raise ValueError(f"❌ Missing numeric column: {col}")

            # Store original for error reporting
            original_col = df[col].copy()
            
            # Clean and convert
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .str.replace(',', '', regex=False)      # Remove commas
                .str.replace('₹', '', regex=False)      # Remove rupee symbol
                .str.replace('$', '', regex=False)      # Remove dollar sign
                .str.replace('Rs.', '', regex=False)    # Remove Rs.
                .str.replace('Rs', '', regex=False)     # Remove Rs
                .replace(['', 'nan', 'None', 'NULL', 'null'], None)
            )

            # Convert to numeric
            df[col] = pd.to_numeric(df[col], errors="coerce")

            # Check for conversion failures
            invalid_mask = df[col].isnull() & original_col.notna()
            invalid_count = invalid_mask.sum()
            
            if invalid_count > 0:
                bad_values = original_col[invalid_mask].unique()[:5]
                numeric_issues.append(
                    f"{col}: {invalid_count} invalid values (e.g., {list(bad_values)})"
                )

        if numeric_issues:
            raise ValueError(f"❌ Invalid numeric values found:\n   " + "\n   ".join(numeric_issues))
        
        print(f"✅ All {len(numeric_columns)} numeric columns validated:")
        for col in numeric_columns:
            min_val = df[col].min()
            max_val = df[col].max()
            avg_val = df[col].mean()
            print(f"   ✓ {col}: min={min_val:.2f}, max={max_val:.2f}, avg={avg_val:.2f}")
        print()

    # ----------------------------
    # 4️⃣ Flag columns validation
    # ----------------------------
    if flag_columns:
        print(f"4️⃣  FLAG COLUMNS VALIDATION")
        flag_issues = []
        
        for col in flag_columns:
            if col not in df.columns:
                raise ValueError(f"❌ Missing flag column: {col}")
            
            # Normalize flag values
            df[col] = df[col].astype(str).str.strip().str.upper()
            
            # Check for invalid values
            invalid_mask = ~df[col].isin(["Y", "N"])
            invalid_count = invalid_mask.sum()
            
            if invalid_count > 0:
                invalid_values = df[col][invalid_mask].unique()
                flag_issues.append(
                    f"{col}: {invalid_count} invalid values ({list(invalid_values)})"
                )
        
        if flag_issues:
            raise ValueError(f"❌ Invalid flag values found (expected Y/N):\n   " + "\n   ".join(flag_issues))
        
        print(f"✅ All {len(flag_columns)} flag columns validated:")
        for col in flag_columns:
            y_count = (df[col] == 'Y').sum()
            n_count = (df[col] == 'N').sum()
            print(f"   ✓ {col}: Y={y_count:,}, N={n_count:,}")
        print()

    # ----------------------------
    # 5️⃣ Duplicate check (if PK specified)
    # ----------------------------
    if args.pk_column:
        print(f"5️⃣  DUPLICATE CHECK")
        if args.pk_column not in df.columns:
            raise ValueError(f"❌ Primary key column not found: {args.pk_column}")
        
        duplicates = df[df.duplicated(subset=[args.pk_column], keep=False)]
        if len(duplicates) > 0:
            dup_values = duplicates[args.pk_column].unique()[:5]
            raise ValueError(
                f"❌ Found {len(duplicates):,} duplicate rows on {args.pk_column}\n"
                f"   Example duplicate values: {list(dup_values)}"
            )
        
        print(f"✅ No duplicates found on primary key: {args.pk_column}\n")

    print(f"{'='*70}")
    print("🎉 FILE VALIDATION SUCCESS")
    print(f"{'='*70}")
    print(f"   File: {os.path.basename(file_path)}")
    print(f"   Rows: {len(df):,}")
    print(f"   Columns: {len(df.columns)}")
    print(f"   Mandatory columns: {len(mandatory_columns)} ✓")
    if numeric_columns:
        print(f"   Numeric columns: {len(numeric_columns)} ✓")
    if flag_columns:
        print(f"   Flag columns: {len(flag_columns)} ✓")
    print(f"{'='*70}\n")
    exit(0)

# ==================================================
# 🗄️ TABLE VALIDATION LOGIC (ORACLE)
# ==================================================
if args.table_name:
    print("🗄️  TABLE VALIDATION MODE")
    print(f"{'='*70}\n")
    
    print(f"🔍 Validating table: {args.table_name}")
    
    # Connect to database
    try:
        conn = oracledb.connect(
            user=args.db_user,
            password=args.db_password,
            dsn=args.db_dsn
        )
        cur = conn.cursor()
        print(f"✅ Connected to database: {args.db_dsn}\n")
    except Exception as e:
        raise ConnectionError(f"❌ Failed to connect to database: {str(e)}")

    # Verify table exists
    try:
        cur.execute(f"SELECT COUNT(*) FROM {args.table_name} WHERE ROWNUM <= 1")
        cur.fetchone()
    except Exception as e:
        raise ValueError(f"❌ Table not found or not accessible: {args.table_name}\n   Error: {str(e)}")

    # ----------------------------
    # 1️⃣ Row count validation
    # ----------------------------
    print(f"1️⃣  ROW COUNT VALIDATION")
    cur.execute(f"SELECT COUNT(*) FROM {args.table_name}")
    row_count = cur.fetchone()[0]

    if row_count < min_rows:
        raise ValueError(f"❌ Row count {row_count:,} is less than minimum required {min_rows:,}")

    print(f"✅ Row count OK: {row_count:,} (minimum: {min_rows:,})\n")

    # ----------------------------
    # 2️⃣ Mandatory columns NULL check
    # ----------------------------
    print(f"2️⃣  MANDATORY COLUMNS VALIDATION")
    null_issues = []
    
    for col in mandatory_columns:
        try:
            cur.execute(
                f"SELECT COUNT(*) FROM {args.table_name} WHERE {col} IS NULL"
            )
            null_count = cur.fetchone()[0]
            if null_count > 0:
                null_issues.append(f"{col}: {null_count:,} NULL values")
        except Exception as e:
            raise ValueError(f"❌ Column not found or error: {col}\n   Error: {str(e)}")
    
    if null_issues:
        raise ValueError(f"❌ NULL values found in mandatory columns:\n   " + "\n   ".join(null_issues))

    print(f"✅ All {len(mandatory_columns)} mandatory columns have no NULLs:")
    for col in mandatory_columns:
        print(f"   ✓ {col}")
    print()

    # ----------------------------
    # 3️⃣ Duplicate primary key check
    # ----------------------------
    if args.pk_column:
        print(f"3️⃣  DUPLICATE PRIMARY KEY CHECK")
        try:
            cur.execute(f"""
                SELECT COUNT(*) FROM (
                    SELECT {args.pk_column}
                    FROM {args.table_name}
                    GROUP BY {args.pk_column}
                    HAVING COUNT(*) > 1
                )
            """)

            duplicate_count = cur.fetchone()[0]
            if duplicate_count > 0:
                # Get example duplicates
                cur.execute(f"""
                    SELECT {args.pk_column}, COUNT(*) as cnt
                    FROM {args.table_name}
                    GROUP BY {args.pk_column}
                    HAVING COUNT(*) > 1
                    FETCH FIRST 5 ROWS ONLY
                """)
                examples = cur.fetchall()
                dup_examples = [f"{row[0]} ({row[1]} times)" for row in examples]
                
                raise ValueError(
                    f"❌ Found {duplicate_count:,} duplicate primary keys\n"
                    f"   Examples: {', '.join(dup_examples)}"
                )

            print(f"✅ No duplicate primary keys on column: {args.pk_column}\n")
        except oracledb.Error as e:
            if duplicate_count > 0:
                raise
            raise ValueError(f"❌ Error checking primary key column: {args.pk_column}\n   Error: {str(e)}")

    # ----------------------------
    # 4️⃣ Freshness check
    # ----------------------------
    if args.date_column and args.execution_date:
        print(f"4️⃣  FRESHNESS CHECK")
        
        # Convert date to proper format
        try:
            exec_date = datetime.strptime(args.execution_date, "%Y-%m-%d")
            date_key = int(args.execution_date.replace("-", ""))
            print(f"   Checking for date: {args.execution_date} (date_key: {date_key})")
        except ValueError:
            raise ValueError(f"❌ Invalid execution_date format: {args.execution_date} (expected YYYY-MM-DD)")
        
        # For fact tables, check if date exists in dim_date first
        if args.table_name.lower().startswith('fact'):
            try:
                cur.execute("SELECT COUNT(*) FROM dim_date WHERE date_id = :1", [date_key])
                date_exists = cur.fetchone()[0] > 0
                
                if not date_exists:
                    if args.skip_freshness_check:
                        print(f"⚠️  WARNING: Date {args.execution_date} not in dim_date (skipped)")
                        print(f"   Recommendation: Load dim_date first\n")
                    else:
                        raise ValueError(
                            f"❌ Date {args.execution_date} ({date_key}) not found in dim_date\n"
                            f"   Load dim_date first or use --skip_freshness_check"
                        )
            except oracledb.Error as e:
                if "table or view does not exist" in str(e).lower():
                    print(f"⚠️  WARNING: dim_date table not found, skipping date dimension check")
                else:
                    raise
        
        # Check for data on execution date
        try:
            cur.execute(
                f"""
                SELECT COUNT(*) FROM {args.table_name}
                WHERE {args.date_column} = :1
                """,
                [date_key]
            )
            record_count = cur.fetchone()[0]
            
            if record_count == 0:
                if args.skip_freshness_check:
                    print(f"⚠️  WARNING: No data for date {args.execution_date} (skipped)\n")
                else:
                    raise ValueError(
                        f"❌ No data found for date {args.execution_date}\n"
                        f"   Use --skip_freshness_check to allow empty dates"
                    )
            else:
                print(f"✅ Freshness check passed: {record_count:,} records for {args.execution_date}\n")
        except oracledb.Error as e:
            raise ValueError(f"❌ Error checking date column: {args.date_column}\n   Error: {str(e)}")

    # ----------------------------
    # 5️⃣ Numeric columns validation (optional for tables)
    # ----------------------------
    if numeric_columns:
        print(f"5️⃣  NUMERIC COLUMNS VALIDATION")
        for col in numeric_columns:
            try:
                cur.execute(f"""
                    SELECT 
                        MIN({col}) as min_val,
                        MAX({col}) as max_val,
                        AVG({col}) as avg_val,
                        COUNT(*) as total_count,
                        COUNT({col}) as non_null_count
                    FROM {args.table_name}
                """)
                stats = cur.fetchone()
                print(f"   ✓ {col}: min={stats[0]}, max={stats[1]}, avg={stats[2]:.2f}, non-null={stats[4]:,}/{stats[3]:,}")
            except Exception as e:
                raise ValueError(f"❌ Error validating numeric column {col}: {str(e)}")
        print()

    print(f"{'='*70}")
    print("🎉 TABLE VALIDATION SUCCESS")
    print(f"{'='*70}")
    print(f"   Table: {args.table_name}")
    print(f"   Rows: {row_count:,}")
    print(f"   Mandatory columns: {len(mandatory_columns)} ✓")
    if args.pk_column:
        print(f"   Primary key: {args.pk_column} ✓")
    if args.date_column and args.execution_date:
        print(f"   Freshness: {args.execution_date} ✓")
    print(f"{'='*70}\n")

    cur.close()
    conn.close()
    exit(0)

# ==================================================
# ❌ NO VALIDATION TARGET SPECIFIED
# ==================================================
print("❌ ERROR: No validation target specified")
print("   Use --file_path or --file_pattern for file validation")
print("   Use --table_name for table validation")
print("\nExample usage:")
print("   # File validation:")
print("   python validate_table.py --file_path data.csv --mandatory_columns id,name --min_rows 100")
print("\n   # Table validation:")
print("   python validate_table.py --table_name dim_product --mandatory_columns product_id,product_name --min_rows 1000")
exit(1)
