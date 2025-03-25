"""
Project: Mais Formandos
Module: extract_helpers

This module provides helper functions for extracting, processing, and inserting
data from CSV files into a SQLite database.
It includes functions to create mapping tables, add DataFrames to the database,
extract data from CSV files, insert unique values into normalized tables,
and process multiple CSV files in a directory.

Functions:
    create_mapping_table(df: pd.DataFrame, no_col: str, co_col: str, conn: Connection):
    add_dataframe_to_db(df: pd.DataFrame, table_name: str, conn: Connection):
    extract_dataframe_from_csv(file_path: str, chunksize: int, usecols=None):
    insert_unique_values(df, table_name, conn, columns):
    process_csv_files(directory: str, conn: Connection, usecols: None):

"""

import os
import pandas as pd
import logging
from sqlite3 import Connection


def create_mapping_table(df: pd.DataFrame, no_col: str, co_col: str, conn: Connection):
    """
    Creates a mapping table for a given NO_ and CO_ column pair.

    Args:
        df (pd.DataFrame): The DataFrame containing the columns.
        no_col (str): The name of the NO_ column.
        co_col (str): The name of the CO_ column.
        conn (sqlite3.Connection): The SQLite database connection.
    """
    mapping_table_name = f"{no_col[3:].lower()}"
    mapping_df = df[[co_col, no_col]].drop_duplicates().reset_index(drop=True)

    # Convert CO_ column to integer if possible
    try:
        mapping_df[co_col] = mapping_df[co_col].astype(int)
    except ValueError:
        logging.warning(f"Column {co_col} could not be converted to integer.")

    # Check if the mapping table already exists
    existing_entries = (
        pd.read_sql_query(f"SELECT {co_col} FROM {mapping_table_name}", conn)
        if conn.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{mapping_table_name}'"
        ).fetchone()
        else pd.DataFrame()
    )

    if not existing_entries.empty:
        # Filter out entries that already exist in the mapping table
        mapping_df = mapping_df[~mapping_df[co_col].isin(existing_entries[co_col])]

    if not mapping_df.empty:
        mapping_df.to_sql(mapping_table_name, conn, if_exists="append", index=False)
        logging.info(
            f"Mapping table '{mapping_table_name}' updated with {len(mapping_df)} new entries."
        )


def add_dataframe_to_db(df: pd.DataFrame, table_name: str, conn: Connection):
    """
    Adds a DataFrame to a SQLite database.

    Args:
        df (pd.DataFrame): The DataFrame to add.
        table_name (str): The name of the table to add the data to.
        conn (sqlite3.Connection): The SQLite database connection.
    """
    try:
        # Ensure efficient data types
        no_columns = []
        for col in df.columns:
            if col.startswith("NO_"):
                co_col = col.replace("NO_", "CO_")
                create_mapping_table(df, col, co_col, conn)
                no_columns.append(col)
            elif col.startswith("CO_"):
                # Convert CO_ column to integer if possible
                try:
                    df[col] = df[col].astype(int)
                except ValueError:
                    logging.warning(f"Column {col} could not be converted to integer.")
            elif col.startswith("SG_"):
                df[col] = df[col].astype("category")
            elif col.startswith("IN_") or col.startswith("TP_"):
                df[col] = df[col].astype("int8")
            elif col.startswith("QT_"):
                df[col] = df[col].astype("int32")

        # Drop NO_ columns from the DataFrame
        df = df.drop(columns=no_columns)
        df.to_sql(table_name, conn, if_exists="append", index=False)
        logging.info(f"Data inserted into table '{table_name}'")
    except Exception as e:
        logging.error(f"Error inserting data into table '{table_name}': {e}")


def extract_dataframe_from_csv(file_path: str, chunksize: int, usecols=None):
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
        yield chunk


def insert_unique_values(df, table_name, conn, columns):
    unique_values = df[columns].drop_duplicates()
    unique_values.to_sql(table_name, conn, if_exists="append", index=False)


def process_csv_files(directory: str, conn: Connection, usecols: None):
    """
    Processes CSV files in a directory and adds the data to a SQLite database.

    Args:
        directory (str): The directory containing the CSV files.
        conn (sqlite3.Connection): The SQLite database connection.
    """
    for root, _, files in os.walk(directory):
        for filename in files:
            if filename.lower().endswith(".csv"):
                file_path = os.path.join(root, filename)
                logging.info(f"Reading file: {filename}")

                for chunk in extract_dataframe_from_csv(
                    file_path, usecols=usecols, chunksize=50000
                ):
                    # Convert relevant columns to integers
                    for col in [
                        "CO_UF",
                        "CO_REGIAO",
                        "CO_MUNICIPIO",
                        "TP_GRAU_ACADEMICO",
                    ]:
                        if col in chunk.columns:
                            chunk[col] = chunk[col].astype("int32")

                    # Insert unique values into normalized tables
                    insert_unique_values(
                        chunk, "regions", conn, ["CO_REGIAO", "NO_REGIAO"]
                    )
                    insert_unique_values(
                        chunk, "states", conn, ["CO_UF", "NO_UF", "SG_UF"]
                    )
                    insert_unique_values(
                        chunk,
                        "municipalities",
                        conn,
                        ["CO_MUNICIPIO", "NO_MUNICIPIO", "CO_UF"],
                    )
                    insert_unique_values(
                        chunk,
                        "institutions",
                        conn,
                        [
                            "CO_IES",
                            "TP_ORGANIZACAO_ACADEMICA",
                            "TP_REDE",
                            "TP_CATEGORIA_ADMINISTRATIVA",
                        ],
                    )
                    insert_unique_values(
                        chunk,
                        "courses",
                        conn,
                        [
                            "CO_CURSO",
                            "NO_CURSO",
                            "CO_CINE_ROTULO",
                            "CO_CINE_AREA_GERAL",
                            "CO_CINE_AREA_ESPECIFICA",
                            "CO_CINE_AREA_DETALHADA",
                            "TP_GRAU_ACADEMICO",
                            "IN_GRATUITO",
                            "TP_MODALIDADE_ENSINO",
                            "TP_NIVEL_ACADEMICO",
                        ],
                    )
                    insert_unique_values(
                        chunk,
                        "cine_rotulos",
                        conn,
                        ["CO_CINE_ROTULO", "NO_CINE_ROTULO"],
                    )
                    insert_unique_values(
                        chunk,
                        "cine_areas_gerais",
                        conn,
                        ["CO_CINE_AREA_GERAL", "NO_CINE_AREA_GERAL"],
                    )
                    insert_unique_values(
                        chunk,
                        "cine_areas_especificas",
                        conn,
                        ["CO_CINE_AREA_ESPECIFICA", "NO_CINE_AREA_ESPECIFICA"],
                    )
                    insert_unique_values(
                        chunk,
                        "cine_areas_detalhadas",
                        conn,
                        ["CO_CINE_AREA_DETALHADA", "NO_CINE_AREA_DETALHADA"],
                    )

                    # Insert data into the main table
                    main_table_columns = [
                        "NU_ANO_CENSO",
                        "CO_REGIAO",
                        "CO_UF",
                        "CO_MUNICIPIO",
                        "CO_IES",
                        "CO_CURSO",
                    ]
                    main_table_data = chunk[main_table_columns]
                    main_table_data.to_sql(
                        "microdados", conn, if_exists="append", index=False
                    )
