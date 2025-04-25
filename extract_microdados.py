"""
Project: Mais Formandos
Module: extract_microdados

This module handles the extraction and processing of microdata from the INEP Censo da Educação Superior.
"""

import argparse
import logging
import traceback
import sqlite3
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from extract_helpers import process_csv_files

# Constants
LOG_FILE = Path(__file__).parent / "extract_microdados.log"
DB_FILE = Path(__file__).parent / "inep.db"
DATA_DIR = Path(__file__).parent / "INEP" / "Microdados_Censo_da_Educação_Superior"


def extract_microdados(start_year: int, end_year: int):
    """
    Extracts and processes CSV files for specified years and
    stores the data in a SQLite database.

    Args:
        start_year (int): The starting year for processing files.
        end_year (int): The ending year for processing files.
    """

    logging.info(f"Starting microdata extraction for years {start_year} to {end_year}.")

    with sqlite3.connect(DB_FILE) as conn:
        for year in range(start_year, end_year + 1):
            year_dir = DATA_DIR / str(year) / "dados"
            if year_dir.exists():
                logging.info(
                    f"Processing files for year {year} in directory {year_dir}"
                )
                try:
                    process_csv_files(year_dir, conn)
                except Exception as e:
                    logging.error(f"Error processing files for year {year}: {e}")
                    logging.debug(traceback.format_exc())
                else:
                    logging.info(f"Successfully processed files for year {year}.")
            else:
                logging.warning(f"Directory for year {year} not found: {year_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract and process microdata.")
    parser.add_argument(
        "--start_year",
        type=int,
        default=2010,
        help="Starting year for processing files",
    )
    parser.add_argument(
        "--end_year", type=int, default=2023, help="Ending year for processing files"
    )
    args = parser.parse_args()

    # Configure logging to log to a file
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3),
            logging.StreamHandler(),
        ],
    )

    try:
        extract_microdados(args.start_year, args.end_year)
        print("End of loading")
    except Exception as e:
        logging.critical(f"Critical error: {e}")
        sys.exit(1)
