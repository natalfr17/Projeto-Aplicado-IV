"""
Project: Mais Formandos
Module: extract_helpers

This module provides helper functions for extracting, processing, and inserting
data from CSV files into a SQLite database.
"""

import logging
import traceback
from pathlib import Path
from sqlite3 import Connection

import pandas as pd

from extract_config import (
    DEFAULT_CHUNK_SIZE,
    MAIN_TABLE_COLUMNS,
    SELECTED_COLUMNS,
    TABLE_MAPPINGS,
)


def remove_duplicates_from_table(
    conn: Connection, table_name: str, unique_columns: list[str]
):
    """
    Removes duplicate rows from a table based on unique columns.

    Args:
        conn (sqlite3.Connection): The SQLite database connection.
        table_name (str): The name of the table to deduplicate.
        unique_columns (list): The columns to consider for uniqueness.
    """
    try:
        unique_columns_str = ", ".join(unique_columns)
        query = f"""
        DELETE FROM {table_name}
        WHERE rowid NOT IN (
            SELECT MIN(rowid)
            FROM {table_name}
            GROUP BY {unique_columns_str}
        )
        """
        conn.execute(query)
        conn.commit()
        logging.info(f"Removed duplicates from table '{table_name}'")
    except Exception as e:
        logging.error(f"Error removing duplicates from table '{table_name}': {e}")


def extract_dataframe_from_csv(
    file_path: str, chunksize: int = DEFAULT_CHUNK_SIZE, usecols=None
):
    """
    Extracts and processes data from a CSV file.

    Args:
        file_path (str): The path to the CSV file.
        chunksize (int): The number of rows per chunk.
        usecols (list): List of columns to read from the CSV file.

    Yields:
        pd.DataFrame: The processed DataFrame chunk.
    """
    dtype = {}
    if usecols:
        all_columns = pd.read_csv(
            file_path, encoding="latin-1", sep=";", nrows=0
        ).columns.tolist()

        # Filter columns starting with "QT_ING" or "QT_CONC"
        qt_columns = [
            col
            for col in all_columns
            if col.startswith("QT_ING") or col.startswith("QT_CONC")
        ]

        # Combine specified columns with QT columns
        usecols = list(set(usecols + qt_columns))
        for col in usecols:
            if col.startswith("NO_") or col.startswith("SG_"):
                dtype[col] = "string"
            elif col.startswith("IN_"):
                dtype[col] = "int8"
            elif col.startswith("QT_"):
                dtype[col] = "int32"

    # Read the CSV file
    for chunk in pd.read_csv(
        file_path,
        encoding="latin-1",
        sep=";",
        usecols=usecols,
        chunksize=chunksize,
        dtype=dtype,
    ):
        chunk = chunk.dropna(how="any", axis=0)
        # Convert relevant columns to integers, handling NaN values
        for col in [
            "CO_UF",
            "CO_REGIAO",
            "CO_MUNICIPIO",
            "TP_GRAU_ACADEMICO",
        ]:
            if col in chunk.columns:
                chunk[col] = chunk[col].astype(int)
        yield chunk


def insert_unique_values(
    df: pd.DataFrame, table_name: str, conn: Connection, columns: list[str]
):
    """
    Inserts unique values into a table.

    Args:
        df (pd.DataFrame): The DataFrame containing the data.
        table_name (str): The name of the table to insert data into.
        conn (sqlite3.Connection): The SQLite database connection.
        columns (list): The columns to consider for uniqueness.
    """
    unique_values = df[columns].drop_duplicates()
    unique_values.to_sql(table_name, conn, if_exists="append", index=False)


def process_csv_files(directory: str, conn: Connection, chunksize=DEFAULT_CHUNK_SIZE):
    """
    Processes CSV files in a directory and adds the data to a SQLite database.

    Args:
        directory (str): The directory containing the CSV files.
        conn (sqlite3.Connection): The SQLite database connection.
        chunksize (int): Number of rows to process per chunk.
    """
    logging.info("Starting CSV file processing.")
    # Track total rows processed
    total_rows_processed = 0

    directory = Path(directory)
    if not directory.exists():
        logging.error(f"Directory does not exist: {directory}")
        return

    # Recursively find all CSV files
    for file_path in directory.rglob("*.csv"):
        logging.info(f"Starting processing for file: {file_path.name}")
        try:
            # Process the file in chunks
            for chunk in extract_dataframe_from_csv(
                file_path, usecols=SELECTED_COLUMNS, chunksize=chunksize
            ):
                # Dynamically add QT_ columns to MAIN_TABLE_COLUMNS
                qt_columns = [col for col in chunk.columns if col.startswith("QT_")]
                all_main_table_columns = MAIN_TABLE_COLUMNS + qt_columns
                
                # Insert unique values into normalized tables
                for table_name, columns in TABLE_MAPPINGS.items():
                    if all(col in chunk.columns for col in columns):
                        insert_unique_values(chunk, table_name, conn, columns)

                # Insert data into the main table
                if all(col in chunk.columns for col in all_main_table_columns):
                    main_table_data = chunk[all_main_table_columns]
                    main_table_data.to_sql(
                        "microdados", conn, if_exists="append", index=False
                    )

                # Log the number of rows processed in the current chunk
                rows_in_chunk = len(chunk)
                total_rows_processed += rows_in_chunk
                logging.info(
                    f"Inserted {rows_in_chunk} rows from {file_path.name} into 'microdados' table."
                )

        except Exception as e:
            logging.error(f"Error processing file {file_path.name}: {e}")
            logging.debug(traceback.format_exc())
        else:
            logging.info(f"Finished processing file: {file_path.name}")

    logging.info(f"Total rows processed: {total_rows_processed}")

    # Deduplicate tables
    remove_duplicates_from_table(conn, "microdados", MAIN_TABLE_COLUMNS)
    for table_name, columns in TABLE_MAPPINGS.items():
        remove_duplicates_from_table(conn, table_name, columns)

    logging.info("CSV file processing completed.")
