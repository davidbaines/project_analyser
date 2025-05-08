import argparse
import csv
import os
import re
import sys
import logging
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
PROJECTS_FOLDER=Path(os.getenv("PROJECTS_FOLDER")"))

try:
    from machine.corpora import ParatextTextCorpus
    # from machine.scripture import VerseRef # For type hinting if needed
except ImportError:
    print("Error: The 'machine' library (SIL NLP toolkit) is not installed or accessible.", file=sys.stderr)
    print("Please install it: pip install sil-machine", file=sys.stderr)
    sys.exit(1)

# --- Logging Setup ---
log_format = '%(asctime)s - %(levelname)s - %(name)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format)
logger = logging.getLogger(__name__)

# --- Constants for VRS file generation ---
# Copied from count_verses.py for consistent book ordering
BOOK_ORDER = [
    "GEN", "EXO", "LEV", "NUM", "DEU", "JOS", "JDG", "RUT", "1SA", "2SA",
    "1KI", "2KI", "1CH", "2CH", "EZR", "NEH", "EST", "JOB", "PSA", "PRO",
    "ECC", "SNG", "ISA", "JER", "LAM", "EZK", "DAN", "HOS", "JOL", "AMO",
    "OBA", "JON", "MIC", "NAM", "HAB", "ZEP", "HAG", "ZEC", "MAL",
    # NT
    "MAT", "MRK", "LUK", "JHN", "ACT", "ROM", "1CO", "2CO", "GAL", "EPH",
    "PHP", "COL", "1TH", "2TH", "1TI", "2TI", "TIT", "PHM", "HEB", "JAS",
    "1PE", "2PE", "1JN", "2JN", "3JN", "JUD", "REV",
    # Deuterocanon / Apocrypha
    "TOB", "JDT", "ESG", "WIS", "SIR", "BAR", "LJE", "S3Y", "SUS", "BEL",
    "1MA", "2MA", "3MA", "4MA", "1ES", "2ES", "MAN", "PS2", "ODA", "PSS",
    "EZA", "5EZ", "6EZ", "DAG", "LAO", "FRT", "BAK", "OTH", "CNC", "GLO",
    "TDX", "NDX", "XXA", "XXB", "XXC", "XXD", "XXE", "XXF", "XXG"
]
BOOK_SORT_KEY = {book: i for i, book in enumerate(BOOK_ORDER)}

# Regex for parsing verse strings like "1", "1a", "1-2" from corpus VerseRef.verse
VERSE_STR_PATTERN = re.compile(r"(\d+)([a-zA-Z]?)")

def parse_verse_string(verse_str: str) -> tuple[int, str]:
    """Parses a verse string (e.g., '1', '1a') into number and subdivision."""
    if not verse_str: # Handle empty verse strings if they occur
        return 0, ""
    match = VERSE_STR_PATTERN.match(verse_str)
    if match:
        return int(match.group(1)), match.group(2)
    try:
        return int(verse_str), ""
    except ValueError:
        logger.warning(f"Could not parse verse string '{verse_str}' into an integer.")
        return 0, ""

def get_project_name(project_path: Path) -> str:
    """Derives a project name from the project folder name."""
    return project_path.name

def is_paratext_folder(candidate_path: Path) -> bool:
    """
    Checks if a folder 'looks like' a Paratext project.
    A Paratext project folder contains .SFM or .usfm files (case-insensitive)
    and also a Settings.xml file.
    """
    if not candidate_path.is_dir():
        return False

    has_settings_xml = (candidate_path / "Settings.xml").is_file()
    if not has_settings_xml:
        return False

    has_sfm_files = any(candidate_path.glob("*.[sS][fF][mM]"))
    has_usfm_files = any(candidate_path.glob("*.[uU][sS][fF][mM]"))

    return has_sfm_files or has_usfm_files