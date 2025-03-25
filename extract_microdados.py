"""
Project: Mais Formandos
Module: extract_microdados

This module handles the extraction and processing of microdata from the INEP Censo da Educação Superior.

Functions:
- extract_microdados: Extracts and processes CSV files for specified years and stores the data in a SQLite database.
- test_microdados: Tests the extraction process for a specific year by processing a sample CSV file.
"""

import os
import sqlite3
import logging

from extract_helpers import (
    extract_dataframe_from_csv,
    add_dataframe_to_db,
    process_csv_files,
)

# Configure logging to log to a file
log_file_path = os.path.join(os.path.dirname(__file__), "extract_microdados.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(log_file_path), logging.StreamHandler()],
)

# Define the subset of columns to read
selected_columns = [
    "NU_ANO_CENSO",
    "NO_REGIAO",
    "CO_REGIAO",
    "NO_UF",
    "SG_UF",
    "CO_UF",
    "NO_MUNICIPIO",
    "CO_MUNICIPIO",
    "TP_ORGANIZACAO_ACADEMICA",
    "TP_REDE",
    "TP_CATEGORIA_ADMINISTRATIVA",
    "CO_IES",
    "NO_CURSO",
    "CO_CURSO",
    "NO_CINE_ROTULO",
    "CO_CINE_ROTULO",
    "CO_CINE_AREA_GERAL",
    "NO_CINE_AREA_GERAL",
    "CO_CINE_AREA_ESPECIFICA",
    "NO_CINE_AREA_ESPECIFICA",
    "CO_CINE_AREA_DETALHADA",
    "NO_CINE_AREA_DETALHADA",
    "TP_GRAU_ACADEMICO",
    "IN_GRATUITO",
    "TP_MODALIDADE_ENSINO",
    "TP_NIVEL_ACADEMICO",
]


def extract_microdados(start_year=2023, end_year=2023):
    """
    Extracts and processes CSV files for specified years and stores the data in a SQLite database.

    Args:
        start_year (int): The starting year for processing files.
        end_year (int): The ending year for processing files.
    """
    db_path = os.path.join(os.path.dirname(__file__), "inep.db")
    data_dir = os.path.join(
        os.path.dirname(__file__), "INEP", "Microdados_Censo_da_Educação_Superior"
    )

    with sqlite3.connect(db_path) as conn:
        for year in range(start_year, end_year + 1):
            year_dir = os.path.join(data_dir, str(year), "dados")
            if os.path.exists(year_dir):
                logging.info(
                    f"Processing files for year {year} in directory {year_dir}"
                )
                try:
                    process_csv_files(year_dir, conn, usecols=selected_columns)
                except Exception as e:
                    logging.error(f"Error processing files for year {year}: {e}")
            else:
                logging.warning(f"Directory for year {year} not found: {year_dir}")


def test_extract_microdados(year=2023):
    """
    Tests the extraction process for a specific year by processing a sample CSV file.

    Args:
        year (int): The year to test the extraction process.
    """
    data_dir = os.path.join(
        os.path.dirname(__file__), "INEP", "Microdados_Censo_da_Educação_Superior"
    )
    test_db_path = os.path.join(os.path.dirname(__file__), "inep_test.db")
    year_dir = os.path.join(data_dir, str(year), "dados")
    sample_file = os.path.join(year_dir, f"MICRODADOS_CADASTRO_CURSOS_{year}.csv")

    if os.path.exists(sample_file):
        logging.info(
            f"Testing extraction process for year {year} using file {sample_file}"
        )
        try:
            with sqlite3.connect(test_db_path) as conn:
                for chunk in extract_dataframe_from_csv(
                    sample_file, chunksize=50000, usecols=selected_columns
                ):
                    logging.info(
                        f"Successfully processed a chunk of the sample file for year {year}"
                    )
                    add_dataframe_to_db(chunk, "MICRODADOS", conn)
                    logging.info(f"Chunk size: {len(chunk)}")
        except Exception as e:
            logging.error(f"Error processing sample file for year {year}: {e}")
    else:
        logging.warning(f"Sample file for year {year} not found: {sample_file}")
        return None


if __name__ == "__main__":
    test_extract_microdados()
    print("End of loading")
