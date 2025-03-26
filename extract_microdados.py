"""
Project: Mais Formandos
Module: extract_microdados

This module handles the extraction and processing of microdata from the INEP Censo da Educação Superior.
"""

import os
import sqlite3
import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler
import argparse
from extract_helpers import process_csv_files


# Constants
LOG_FILE = Path(__file__).parent / "extract_microdados.log"
DB_FILE = Path(__file__).parent / "inep.db"
DATA_DIR = Path(__file__).parent / "INEP" / "Microdados_Censo_da_Educação_Superior"

# Configure logging to log to a file
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3),
        logging.StreamHandler(),
    ],
)
# Define the subset of columns to read
SELECTED_COLUMNS = [
    "NU_ANO_CENSO",  # Ano de referência do Censo da Educação Superior
    "NO_REGIAO",  # Nome da região geográfica da sede administrativa ou reitoria da IES
    "CO_REGIAO",  # Código da região geográfica da sede administrativa ou reitoria da IES
    "NO_UF",  # Nome da Unidade da Federação da sede administrativa ou reitoria da IES
    "SG_UF",  # Sigla da Unidade da Federação da sede administrativa ou reitoria da IES
    "CO_UF",  # Código da Unidade da Federação da sede administrativa ou reitoria da IES
    "NO_MUNICIPIO",  # Nome do Município da sede administrativa ou reitoria da IES
    "CO_MUNICIPIO",  # Código do Município da sede administrativa ou reitoria da IES
    "TP_ORGANIZACAO_ACADEMICA",  # Tipo de Organização Acadêmica da IES
    "TP_REDE",  # Rede de Ensino
    "TP_CATEGORIA_ADMINISTRATIVA",  # Tipo de Categoria Administrativa da IES
    "CO_IES",  # Código único de identificação da IES
    "NO_CURSO",  # Nome do curso
    "CO_CURSO",  # Código do curso
    "NO_CINE_ROTULO",  # Nome do rótulo CINE
    "CO_CINE_ROTULO",  # Código do rótulo CINE
    "CO_CINE_AREA_GERAL",  # Código da área geral CINE
    "NO_CINE_AREA_GERAL",  # Nome da área geral CINE
    "CO_CINE_AREA_ESPECIFICA",  # Código da área específica CINE
    "NO_CINE_AREA_ESPECIFICA",  # Nome da área específica CINE
    "CO_CINE_AREA_DETALHADA",  # Código da área detalhada CINE
    "NO_CINE_AREA_DETALHADA",  # Nome da área detalhada CINE
    "TP_GRAU_ACADEMICO",  # Tipo de grau acadêmico
    "IN_GRATUITO",  # Informa se o curso é gratuito
    "TP_MODALIDADE_ENSINO",  # Tipo de modalidade de ensino
    "TP_NIVEL_ACADEMICO",  # Tipo de nível acadêmico
]


def extract_microdados(start_year=2023, end_year=2023):
    """
    Extracts and processes CSV files for specified years and
    stores the data in a SQLite database.

    Args:
        start_year (int): The starting year for processing files.
        end_year (int): The ending year for processing files.
    """

    with sqlite3.connect(DB_FILE) as conn:
        for year in range(start_year, end_year + 1):
            year_dir = DATA_DIR / str(year) / "dados"
            if year_dir.exists():
                logging.info(
                    f"Processing files for year {year} in directory {year_dir}"
                )
                try:
                    process_csv_files(year_dir, conn, usecols=SELECTED_COLUMNS)
                except Exception as e:
                    logging.error(f"Error processing files for year {year}: {e}")
            else:
                logging.warning(f"Directory for year {year} not found: {year_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract and process microdata.")
    parser.add_argument(
        "--start_year",
        type=int,
        default=2023,
        help="Starting year for processing files",
    )
    parser.add_argument(
        "--end_year", type=int, default=2023, help="Ending year for processing files"
    )
    args = parser.parse_args()

    try:
        extract_microdados(args.start_year, args.end_year)
        print("End of loading")
    except Exception as e:
        logging.critical(f"Critical error: {e}")
        exit(1)
