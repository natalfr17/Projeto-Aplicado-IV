"""
Project: Mais Formandos
Module: extract_microdados

This module handles the extraction and processing of microdata from the INEP Censo da Educação Superior.
"""

import os
import sqlite3
import logging

from extract_helpers import (
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
    Extracts and processes CSV files for specified years and
    stores the data in a SQLite database.

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


if __name__ == "__main__":
    extract_microdados()
    print("End of loading")
