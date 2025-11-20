"""
loading.py
Module for the Load stage of ETL.
Handles inserting, replacing, and upserting data into DuckDB.
Fully compatible with the transform & extract modules.
"""


import logging
import duckdb


logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# === LOAD INTO DUCKDB (with overwrite or upsert) ===
# -----------------------------------------------------------------------------


def load_to_duckdb(df, table_name, conn, primary_key=None, overwrite=False):
    tmp_name = "tmp_df"

    # Register dataframe or fallback via parquet
    try:
        conn.register(tmp_name, df)
    except Exception:
        logger.warning(f"[LOAD] Failed to register DataFrame directly, writing to Parquet fallback.")
        tmp_parquet = "/tmp/etl_tmp.parquet"
        df.to_parquet(tmp_parquet)
        conn.execute(
            f"CREATE OR REPLACE TEMPORARY TABLE {tmp_name} AS SELECT * FROM read_parquet('{tmp_parquet}')"
        )

    # MAIN LOAD LOGIC
    try:
        if overwrite:
        # Full replace
            conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM {tmp_name}")
            logger.info(f"[LOAD] Replaced table {table_name}")
        else:
            # Ensure table exists with same schema
            conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM {tmp_name} LIMIT 0")

            if primary_key:
            # UPSERT approach: delete matching PK rows then insert
                try:
                    conn.execute(f"DELETE FROM {table_name} WHERE {primary_key} IN (SELECT {primary_key} FROM {tmp_name})")
                    logger.info( f"[LOAD] Deleted existing rows in {table_name} for UPSERT on key={primary_key}")
                except Exception as e:
                    logger.warning(f"[LOAD] PK delete failed or PK missing ({primary_key}). Falling back to append. Error: {e}")
                conn.execute(f"INSERT INTO {table_name} SELECT * FROM {tmp_name}")
                logger.info(f"[LOAD] Upserted into {table_name} (key={primary_key})")
            else:
            # Append-only
                conn.execute(f"INSERT INTO {table_name} SELECT * FROM {tmp_name}")
                logger.info(f"[LOAD] Appended data into {table_name}")
    finally:
        try:
            conn.unregister(tmp_name)
        except Exception:
            pass