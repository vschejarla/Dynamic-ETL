import argparse
import oracledb

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------
DB_CONFIG = {
    "user": "target_dw",
    "password": "target_dw123",
    "dsn": "host.docker.internal/orcl"
}

# ---------------------------------------------------------
# VALIDATION FUNCTIONS
# ---------------------------------------------------------
def check_row_count(cur, table):
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    count = cur.fetchone()[0]
    print(f"✔ {table} → Row Count = {count}")
    return count


def check_not_null(cur, table, columns):
    for col in columns:
        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL")
        bad = cur.fetchone()[0]

        if bad > 0:
            print(f"❌ NULL FOUND → {table}.{col} has {bad} NULL values")
        else:
            print(f"✔ {table}.{col} has NO NULL values")


def check_numeric_validity(cur, table, columns):
    for col in columns:
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM {table}
            WHERE REGEXP_LIKE({col}, '[^0-9\.]')
            """
        )
        bad = cur.fetchone()[0]

        if bad > 0:
            print(f"❌ INVALID NUMBER → {table}.{col} contains non-numeric values")
        else:
            print(f"✔ {table}.{col} numeric check passed")


def check_fk(cur, table, fk_col, ref_table, ref_col):
    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM {table} t
        LEFT JOIN {ref_table} r
        ON t.{fk_col} = r.{ref_col}
        WHERE t.{fk_col} IS NOT NULL
        AND r.{ref_col} IS NULL
        """
    )
    bad = cur.fetchone()[0]

    if bad > 0:
        print(f"❌ FK MISMATCH → {table}.{fk_col} has {bad} invalid foreign keys")
    else:
        print(f"✔ Foreign Key OK → {table}.{fk_col}")


# ---------------------------------------------------------
# MAIN VALIDATION LOGIC
# ---------------------------------------------------------
def validate_table(table):
    conn = oracledb.connect(**DB_CONFIG)
    cur = conn.cursor()

    print(f"\n==============================")
    print(f" VALIDATING → {table}")
    print(f"==============================")

    # --------------------------------------
    # TABLE-SPECIFIC VALIDATION RULES
    # --------------------------------------
    if table == "DIM_STORE_DW":
        check_row_count(cur, table)
        check_not_null(cur, table, [
            "STORE_KEY", "STORE_NAME", "STORE_CITY", "STORE_STATE"
        ])
        check_fk(cur, table, "CHAIN_KEY", "DIM_STORE_CHAIN_DW", "CHAIN_KEY")

    elif table == "DIM_STORE_CHAIN_DW":
        check_row_count(cur, table)
        check_not_null(cur, table, ["CHAIN_KEY", "CHAIN_NAME"])

    elif table == "DIM_PRODUCT_DW":
        check_row_count(cur, table)
        check_not_null(cur, table, [
            "PRODUCT_KEY", "PRODUCT_NAME", "CATEGORY_KEY", "MANUFACTURER_KEY"
        ])
        check_fk(cur, table, "CATEGORY_KEY", "DIM_CATEGORY", "CATEGORY_KEY")
        check_fk(cur, table, "SUB_CATEGORY_KEY", "DIM_SUB_CATEGORY", "SUB_CATEGORY_KEY")
        check_fk(cur, table, "MANUFACTURER_KEY", "DIM_MANUFACTURER", "MANUFACTURER_KEY")

    elif table == "DIM_CATEGORY":
        check_row_count(cur, table)
        check_not_null(cur, table, ["CATEGORY_KEY", "CATEGORY_NAME"])

    elif table == "DIM_SUB_CATEGORY":
        check_row_count(cur, table)
        check_not_null(cur, table, ["SUB_CATEGORY_KEY", "SUB_CATEGORY_NAME"])

    elif table == "DIM_MANUFACTURER":
        check_row_count(cur, table)
        check_not_null(cur, table, ["MANUFACTURER_KEY", "MANUFACTURER_NAME"])

    elif table == "DIM_DISTRIBUTOR_DW":
        check_row_count(cur, table)
        check_not_null(cur, table, ["DIST_KEY", "DIST_NAME"])

    elif table == "DIM_DATE_DW":
        check_row_count(cur, table)
        check_not_null(cur, table, ["DATE_ID", "FULL_DATE"])

    elif table == "FACT_SALES_DW":
        check_row_count(cur, table)
        check_not_null(cur, table, ["SALES_KEY", "DATE_ID", "STORE_KEY", "PRODUCT_KEY", "DIST_KEY"])
        check_numeric_validity(cur, table, ["QUANTITY_SOLD", "UNIT_PRICE", "NET_AMOUNT"])
        
        # foreign keys
        check_fk(cur, table, "DATE_ID", "DIM_DATE_DW", "DATE_ID")
        check_fk(cur, table, "STORE_KEY", "DIM_STORE_DW", "STORE_KEY")
        check_fk(cur, table, "PRODUCT_KEY", "DIM_PRODUCT_DW", "PRODUCT_KEY")
        check_fk(cur, table, "DIST_KEY", "DIM_DISTRIBUTOR_DW", "DIST_KEY")

    else:
        print(f"❌ ERROR: Unknown table '{table}'")

    cur.close()
    conn.close()

    print(f"🎉 VALIDATION COMPLETE FOR {table}")


# ---------------------------------------------------------
# CLI ENTRY POINT
# ---------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True, help="Table name to validate")
    args = parser.parse_args()

    validate_table(args.table)
