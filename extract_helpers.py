"""
Project: Mais Formandos
Module: extract_helpers

This module provides helper functions for extracting, processing, and inserting
data from CSV files into a SQLite database.
It includes functions to create mapping tables, add DataFrames to the database,
extract data from CSV files, insert unique values into normalized tables,
and process multiple CSV files in a directory.

"""

import os
import pandas as pd
from pathlib import Path
import logging
from sqlite3 import Connection
from extract_config import SELECTED_COLUMNS, MAIN_TABLE_COLUMNS


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
        yield chunk.dropna(how="any", axis=0)


def insert_unique_values(df, table_name, conn, columns):
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


def process_csv_files(directory: str, conn: Connection, chunksize=50000):
    """
    Processes CSV files in a directory and adds the data to a SQLite database.

    Args:
        directory (str): The directory containing the CSV files.
        conn (sqlite3.Connection): The SQLite database connection.
        chunksize (int): Number of rows to process per chunk.
    """
    # Define table mappings for normalized data
    table_mappings = {
        "regions": ["CO_REGIAO", "NO_REGIAO"],
        "states": ["CO_UF", "NO_UF", "SG_UF"],
        "municipalities": ["CO_MUNICIPIO", "NO_MUNICIPIO", "CO_UF"],
        "institutions": [
            "CO_IES",
            "TP_ORGANIZACAO_ACADEMICA",
            "TP_REDE",
            "TP_CATEGORIA_ADMINISTRATIVA",
        ],
        "courses": [
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
        "cine_rotulos": ["CO_CINE_ROTULO", "NO_CINE_ROTULO"],
        "cine_areas_gerais": ["CO_CINE_AREA_GERAL", "NO_CINE_AREA_GERAL"],
        "cine_areas_especificas": [
            "CO_CINE_AREA_ESPECIFICA",
            "NO_CINE_AREA_ESPECIFICA",
        ],
        "cine_areas_detalhadas": ["CO_CINE_AREA_DETALHADA", "NO_CINE_AREA_DETALHADA"],
    }

    directory = Path(directory)
    if not directory.exists():
        logging.error(f"Directory does not exist: {directory}")
        return

    # Track total rows processed
    total_rows_processed = 0

    # Recursively find all CSV files
    for file_path in directory.rglob("*.csv"):
        logging.info(f"Reading file: {file_path.name}")

        try:
            # Process the file in chunks
            for chunk in extract_dataframe_from_csv(
                file_path, usecols=SELECTED_COLUMNS, chunksize=chunksize
            ):
                # Convert relevant columns to integers, handling NaN values
                for col in [
                    "CO_UF",
                    "CO_REGIAO",
                    "CO_MUNICIPIO",
                    "TP_GRAU_ACADEMICO",
                ]:
                    if col in chunk.columns:
                        chunk[col] = chunk[col].astype(int)

                # # Drop rows with NaN values in critical columns
                # chunk = chunk.dropna(
                #     subset=[
                #         "CO_UF",
                #         "CO_REGIAO",
                #         "CO_MUNICIPIO",
                #         "TP_GRAU_ACADEMICO",
                #     ]
                # )

                # Insert unique values into normalized tables
                for table_name, columns in table_mappings.items():
                    if all(col in chunk.columns for col in columns):
                        insert_unique_values(chunk, table_name, conn, columns)

                # Insert data into the main table
                if all(col in chunk.columns for col in MAIN_TABLE_COLUMNS):
                    main_table_data = chunk[MAIN_TABLE_COLUMNS]
                    main_table_data.to_sql(
                        "microdados", conn, if_exists="append", index=False
                    )

                # Log the number of rows processed in the current chunk
                rows_in_chunk = len(chunk)
                total_rows_processed += rows_in_chunk
                logging.info(f"Processed {rows_in_chunk} rows from {file_path.name}")

        except Exception as e:
            logging.error(f"Error processing file {file_path.name}: {e}")

    logging.info(f"Total rows processed: {total_rows_processed}")
