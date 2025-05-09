#!/usr/bin/env python3

# Standard library imports
import argparse
import os
import re # For date pattern matching
import sys
from collections import Counter, defaultdict
from datetime import datetime
from typing import Union
from pathlib import Path

# Third-party library imports
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm
import unicodedataplus as ud

# Local application/library specific imports (sil-machine)
import machine.corpora
try: 
    from machine.corpora import (
        DictionaryTextCorpus,
        FileParatextProjectSettingsParser,
        MemoryText,
        ParatextTextCorpus,
        Text,
        TextCorpus,
        TextRow,
        UsfmFileTextCorpus,
        create_versification_ref_corpus,
        extract_scripture_corpus,
    )
    from machine.scripture import ORIGINAL_VERSIFICATION, VerseRef, VersificationType, get_books # book_id_to_number removed as canon.py has its own
    from machine.tokenization import WhitespaceTokenizer
    from machine.scripture import ENGLISH_VERSIFICATION
    from machine.scripture.canon import ALL_BOOK_IDS as CANON_ALL_BOOK_IDS, is_canonical, book_id_to_number
    from machine.scripture.verse_ref import Versification
    from machine.utils.typeshed import StrPath
    from machine.corpora.usfm_parser_handler import UsfmParserHandler
    from machine.corpora.usfm_parser_state import UsfmElementType, UsfmParserElement, UsfmParserState
    from machine.corpora.usfm_stylesheet import UsfmStylesheet # is_cell_range removed as it's not used
    from machine.corpora.usfm_tag import UsfmTextType
    from machine.corpora.usfm_token import UsfmToken, UsfmTokenType
    from machine.corpora.usfm_tokenizer import UsfmTokenizer
    from machine.corpora.paratext_project_settings import ParatextProjectSettings    
    SIL_MACHINE_AVAILABLE = True
except ImportError:
    # This single catch-all can remain if you want a general warning, 
    # or be removed if you prefer individual import errors to halt execution naturally.
    # For development, letting individual ImportErrors occur can be more informative.
    SIL_MACHINE_AVAILABLE = False
    print("Warning: One or more sil-machine components could not be imported. Functionality may be limited.")
    print("Warning: Verse counting for Book_Stats sheet will be skipped.")

# --- Configuration & Constants ---
N_WORDS = 10
N_MARKERS = 10
N_PUNCTUATION = 10

SCRIPT_DETECTION_SAMPLE_SIZE = 5000 # Number of word-forming characters to sample for script detection

# Use ALL_BOOK_IDS from canon.py for canonical book order and validation
BOOK_ORDER = CANON_ALL_BOOK_IDS # This will be used for sorting columns in Excel

def is_word_char(char):
    """Determines if a character is part of a word based on Unicode category."""
    category = ud.category(char)
    # Exclude standalone digits (Number, Decimal Digit)
    if category == 'Nd':
        return False
    # Letters (L), Numbers (N), Connector Punctuation (Pc, e.g. underscore)
    # Nonspacing Marks (Mn), Spacing Combining Marks (Mc) can be part of words in some languages
    return category.startswith('L') or \
           category == 'Pc' or \
           category.startswith('M')


def is_punctuation_char(char):
    """
    Determines if a character is punctuation, with special attention
    to characters that can function as quotation marks (e.g., grave accent).
    This ensures such characters are collected for later analysis by the query script.
    """
    category = ud.category(char)
    # Standard Punctuation categories (e.g., Pd, Ps, Pe, Pc, Po, Pi, Pf)
    if category.startswith('P'):
        return True
    # Specific check for GRAVE ACCENT (U+0060), which is category 'Sk' (Symbol, modifier)
    # and is often used as a quotation mark.
    if char == '`': # GRAVE ACCENT itself
        return True
    return False


def is_paratext_project_folder(candidate_path: Path) -> bool:
    """
    Checks if a folder 'looks like' a Paratext project.
    A Paratext project folder typically contains a 'Settings.xml' file
    and at least one .SFM or .usfm file (case-insensitive).
    """
    if not candidate_path.is_dir():
        return False

    has_settings_xml = (candidate_path / "Settings.xml").is_file()
    if not has_settings_xml:
        # Some very old projects might not have Settings.xml but still have USFM files.
        # For this script, Settings.xml is a strong indicator we want to rely on for metadata.
        return False

    # Check for either .SFM or .usfm files
    has_sfm_files = any(f.suffix.lower() == ".sfm" for f in candidate_path.iterdir() if f.is_file())
    has_usfm_files = any(f.suffix.lower() == ".usfm" for f in candidate_path.iterdir() if f.is_file())

    return has_sfm_files or has_usfm_files


# Regex to find _yyyy_mm_dd or _yyyymmdd suffixes
DATE_SUFFIX_PATTERN = re.compile(r"(.+?)(_(\d{4})_(\d{2})_(\d{2})|_(\d{8}))$")

def project_contains_filtered_books(project_path: Path, book_filter_list: set) -> bool:
    """
    Checks if a project directory contains USFM files for ALL of the books in book_filter_list,
    by attempting to parse Settings.xml and checking for expected filenames.
    """
    if not book_filter_list: # No filter means all books are included.
        return True

    settings_parser = FileParatextProjectSettingsParser(str(project_path))
    try:
        settings = settings_parser.parse()
    except Exception as e: 
        print(f"Debug: Parsing settings caused Exception {e}.\n Could not parse Settings.xml for {project_path.name}. Assuming it doesn't qualify.")
        return False

    if not settings:
        print(f"Debug: Could not parse Settings.xml for {project_path.name} during pre-scan. Assuming it doesn't qualify.")
        return False

    # Ensure the settings object has the get_book_file_name method
    if not hasattr(settings, 'get_book_file_name'):
        print(f"Debug: Parsed settings for {project_path.name} does not have 'get_book_file_name' method. Assuming it doesn't qualify.")
        return False 

    found_books_in_filter = set()

    for book_id_filter in book_filter_list:
        # The get_book_file_name method should handle canonical book IDs.
        # It might raise an error or return an unexpected string for invalid book_id_filter.
        try:
            expected_filename = settings.get_book_file_name(book_id_filter)
            if (project_path / expected_filename).exists():
                found_books_in_filter.add(book_id_filter)
        except Exception as e_fn:
            # This might happen if book_id_filter is not valid for the project's settings 
            # (e.g., not in its versification or an unknown ID to the method)
            # Or if get_book_file_name has an internal issue.
            # print(f"Debug: Error calling get_book_file_name for {book_id_filter} in {project_path.name}: {e_fn}")
            continue # If we can't get a filename or it errors, this book_id_filter can't be confirmed.
    
    # Check if all books from the filter were found
    return found_books_in_filter == book_filter_list

def get_project_paths(base_folder, limit_n_projects_to_scan=None, active_book_filter_for_scan=None):
    """
    Scans the base_folder for Paratext project directories.
    If multiple versions of a project exist with date suffixes (e.g., Proj_2023_01_15),
    only the latest dated version or an undated version is selected.
    If limit_n_projects_to_scan is set, stops scanning after finding that many qualifying projects.
    """
    base_folder_path = Path(base_folder)
    candidate_paths = []
    if not base_folder_path.is_dir():
        print(f"Error: Projects folder '{base_folder}' not found or is not a directory.")
        return candidate_paths

    qualifying_projects_found_count = 0
    for item_path in tqdm(base_folder_path.iterdir(), desc="Scanning for project folders"):
        project_path_obj = item_path # item_path is already a Path object
        if is_paratext_project_folder(project_path_obj):
            if limit_n_projects_to_scan is not None: # If we are limiting the scan
                if active_book_filter_for_scan: # And if there's a book filter for qualification
                    if project_contains_filtered_books(project_path_obj, active_book_filter_for_scan):
                        candidate_paths.append(project_path_obj)
                        qualifying_projects_found_count += 1
                else: # No book filter, so any valid project qualifies
                    candidate_paths.append(project_path_obj)
                    qualifying_projects_found_count += 1
                
                if qualifying_projects_found_count >= limit_n_projects_to_scan:
                    print(f"\nFound {limit_n_projects_to_scan} qualifying project(s) for initial scan. Stopping directory search.")
                    break 
            else: # Not limiting the scan, add all valid projects
                candidate_paths.append(project_path_obj)

    projects_by_base_name = defaultdict(list)
    for path in candidate_paths:
        match = DATE_SUFFIX_PATTERN.match(path.name)
        if match:
            base_name = match.group(1)
            date_obj = None
            if match.group(3): # _yyyy_mm_dd format
                year, month, day = int(match.group(3)), int(match.group(4)), int(match.group(5))
                date_obj = datetime(year, month, day)
            elif match.group(6): # _yyyymmdd format
                date_str = match.group(6)
                date_obj = datetime(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:]))
            projects_by_base_name[base_name].append({"path": path, "date": date_obj})
        else:
            projects_by_base_name[path.name].append({"path": path, "date": None}) # Undated

    final_project_paths = []
    for base_name, versions in projects_by_base_name.items():
        undated_versions = [v for v in versions if v["date"] is None]
        dated_versions = sorted([v for v in versions if v["date"] is not None], key=lambda x: x["date"], reverse=True)

        if dated_versions: # Prefer latest dated version if it exists
            final_project_paths.append(dated_versions[0]["path"])
        elif undated_versions: # Otherwise, take an undated version if one exists
            final_project_paths.append(undated_versions[0]["path"])
        # If neither (should not happen if versions list is not empty), nothing is added for this base_name
            
    return final_project_paths


def analyze_project_data(project_path, num_extreme_words, book_filter_list=None):
    """
    Analyzes a single Paratext project using sil-machine (or mocks).
    Returns a dictionary containing all collected data for the project.
    """
    project_path_obj = Path(project_path) # Ensure project_path is a Path object
    project_name = project_path_obj.name
    print(f"Analyzing project: {project_name}...")

    # Initialize data structure for this project
    project_results = {
        "ProjectName": project_name,
        "ProjectFolderPath": str(project_path_obj), # Store as string for serialization if needed
        "ProcessingStatus": "Success", # Will be updated on error
        "ErrorMessage": "",
        "DateAnalyzed": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "TotalBooksProcessed": 0,
        "DetectedScript": "Unknown",
        "ScriptDirection": "Unknown",
        "HasCustomSty": False,
        "LanguageCode": "Unknown", # Changed from LanguageTag
        # Detailed data
        "SFMMarkersByBook": defaultdict(Counter),  # {book_id: Counter(marker: count)}
        "PunctuationByBook": defaultdict(Counter), # {book_id: {char: count}}; used for summary TopN
        "PunctuationByNameAndBook": defaultdict(lambda: defaultdict(int)), # {unicode_name: {book_id: count}}
        "BookStats": {}, # {book_id: verse_count}
        "AllWordsInProject": [], # Temp list to collect all words for shortest/longest
    }

    try:
        # 1. Check for custom.sty
        custom_sty_path = project_path_obj / "custom.sty"
        project_results["HasCustomSty"] = custom_sty_path.exists()

        # 2. Attempt to load project settings for LTR/RTL and Script
        # Stylesheet for UsfmTokenizer will be the library's default usfm.sty
        try:
            settings_parser = FileParatextProjectSettingsParser(str(project_path_obj))
            settings = settings_parser.parse()
            if settings:
                if hasattr(settings, 'is_right_to_left'):
                    project_results["ScriptDirection"] = "RTL" if settings.is_right_to_left else "LTR"
                else:
                    project_results["ScriptDirection"] = "Unknown"
                    
                # Extract LanguageCode
                project_results["LanguageCode"] = getattr(settings, 'language_code', 'Unknown')

                if project_results["LanguageCode"] == 'Unknown':
                    # This is common if the <LanguageIsoCode> tag is missing or empty
                    project_results["ProcessingStatus"] = "Warning"
                    project_results["ErrorMessage"] += "LanguageCode (ISO code) missing from settings. "
                
            else:
                # This means settings_parser.parse() returned None
                warning_msg = f"Settings.xml for {project_name} could not be parsed or was empty. "
                print(f"Warning: {warning_msg}Using default project settings.")
                project_results["ProcessingStatus"] = "Warning"
                project_results["ErrorMessage"] += warning_msg
        except Exception as e_settings:
            # This means an exception occurred during parsing or accessing attributes
            warning_msg = f"Error accessing Settings.xml for {project_name}: {e_settings}. "
            print(f"Warning: {warning_msg}Using default project settings.")
            project_results["ProcessingStatus"] = "Warning"
            project_results["ErrorMessage"] += f"Error accessing Settings.xml for {project_name}: {e_settings}. "

        # 3. Initialize USFM Stylesheet and Tokenizer
        # Always use the default usfm.sty from the sil-machine library for tokenization.
        try:
            default_sil_stylesheet_path = Path(machine.corpora.__file__).parent / "usfm.sty"
            if default_sil_stylesheet_path.exists():
                stylesheet = UsfmStylesheet(str(default_sil_stylesheet_path))
            else:
                raise FileNotFoundError("Default SIL usfm.sty not found.")
        except Exception as e_stylesheet:
            print(f"Critical: Failed to load default SIL usfm.sty: {e_stylesheet}. Skipping USFM processing for {project_name}.")
            project_results["ProcessingStatus"] = "Error"
            project_results["ErrorMessage"] = f"Default stylesheet loading failed: {e_stylesheet}"
            return project_results

        tokenizer = UsfmTokenizer(stylesheet)

        # 4. Find and process USFM files
        usfm_file_patterns = ["*.SFM", "*.sfm", "*.USFM", "*.usfm"]
        usfm_files = []
        for pattern in usfm_file_patterns:
            usfm_files.extend(project_path_obj.glob(pattern))
        
        if not usfm_files:
            project_results["ProcessingStatus"] = "Warning"
            project_results["ErrorMessage"] = "No USFM files found in project."
            return project_results

        processed_book_ids = set()
        current_book_id_for_file = None
        text_sample_for_script_detection = []

        files_to_process = []
        if book_filter_list and settings and hasattr(settings, 'get_book_file_name'):
            print(f"Project {project_name}: Targeting specific files based on BOOK_FILTER and project settings.")
            for book_id_from_filter in book_filter_list:
                if not is_canonical(book_id_from_filter): # Ensure we only try to get filenames for canonical books from filter
                    print(f"Info: Book ID '{book_id_from_filter}' in filter is not canonical. Skipping filename generation for it.")
                    continue
                try:
                    expected_filename_str = settings.get_book_file_name(book_id_from_filter)
                    expected_file_path = project_path_obj / expected_filename_str
                    if expected_file_path.exists():
                        files_to_process.append(expected_file_path)
                    else:
                        print(f"Warning: Expected file '{expected_filename_str}' for book '{book_id_from_filter}' not found in project {project_name}.")
                except Exception as e_get_fn:
                    print(f"Warning: Could not determine filename for book '{book_id_from_filter}' in project {project_name} using settings: {e_get_fn}")
        else:
            # No book filter, or settings/get_book_file_name not available; process all found USFM files
            for pattern in usfm_file_patterns:
                files_to_process.extend(project_path_obj.glob(pattern))

        if not files_to_process:
            project_results["ProcessingStatus"] = "Warning"
            project_results["ErrorMessage"] = "No USFM files to process (either none found or none matched filter)."
            return project_results # Early exit if no files to process

        for usfm_file_path in files_to_process:
            try:
                with open(usfm_file_path, "r", encoding="utf-8-sig") as file: # utf-8-sig handles BOM
                    content = file.read()
            except Exception as e_file_read:
                print(f"Warning: Could not read USFM file {usfm_file_path}: {e_file_read}")
                continue 

            usfm_tokens = tokenizer.tokenize(content)
            current_word = ""
            # Reset book ID for each file initially; it should be set by \id
            current_book_id_for_file = None 
            currently_in_verse_text_block = False # State for current file
            
            # TEMPORARY: Remove the slice [:10] to process all tokens
            for token in usfm_tokens: # token is a UsfmToken object
                # Inside the loop, for debugging:
                # print(f"DBG: File: {usfm_file_path.name}, BookCtx: {current_book_id_for_file}, Token Type: {token.type}, Token Marker: '{token.marker}', Token Text: '{token.text}', Token Data: '{token.data}'")
                if token.type == UsfmTokenType.BOOK:
                    # For BOOK type, the book code (e.g., "GEN") is expected in token.text or token.data.
                    # Based on DBG, token.data seems reliable for the book code.
                    book_code_candidate = None
                    if token.data and isinstance(token.data, str) and token.data.strip():
                        book_code_candidate = token.data.strip().upper()
                    elif token.text and isinstance(token.text, str) and token.text.strip() and token.text.upper() != "NONE": # Fallback, avoid literal "NONE"
                        book_code_candidate = token.text.strip().upper()
                    
                    if book_code_candidate:
                        # Optional: Check if the book is in our canonical list
                        # This check is still useful even if we targeted files by filter,
                        # as a file named for GEN might internally have \id PSA.
                        if not is_canonical(book_code_candidate): 
                            print(f"Info: Book ID '{book_code_candidate}' from \id tag in {usfm_file_path.name} is not canonical. Skipping content of this \id block.")
                            current_book_id_for_file = None 
                            currently_in_verse_text_block = False 
                            continue # Skip to next token, effectively ignoring this \id block
                        if book_filter_list and book_code_candidate not in book_filter_list: # Should be rare if file targeting worked
                            print(f"Info: Book ID '{book_code_candidate}' from \id tag in {usfm_file_path.name} is not in book_filter_list. Skipping content of this \id block.")
                            current_book_id_for_file = None 
                            currently_in_verse_text_block = False # Reset context
                            continue
                        current_book_id_for_file = book_code_candidate
                        processed_book_ids.add(current_book_id_for_file)
                        # sys.exit(0) # Make sure this is removed for full processing

                active_book_id_for_counting = current_book_id_for_file

                # Update verse text context state.
                # We are "in verse text" after a \v marker.
                # This state is reset by major structural markers like new book or chapter,
                # or by note markers that typically contain non-scriptural text.
                # Paragraph markers (\p, \q, etc.) usually *contain* verse text, so they don't reset the flag if already true.
                if token.type == UsfmTokenType.VERSE:
                    currently_in_verse_text_block = True
                elif token.type in [
                    UsfmTokenType.BOOK,
                    UsfmTokenType.CHAPTER,
                    UsfmTokenType.NOTE # Note markers often contain explanatory text, not scripture.
                                       # This means text within \f ... \f* won't be counted as verse words/punctuation.
                    # Consider if other specific UsfmTokenType should reset this flag, e.g., some peripheral markers.
                    # For now, PARAGRAPH type tokens (\p, \q, \s, etc.) do NOT reset the flag if it's already true.
                ]:
                    currently_in_verse_text_block = False

                # A. SFM Marker Handling
                # Count all tokens that are not pure text, end markers, or unknown (unless unknown is a marker type)
                # UsfmTokenType.TEXT, UsfmTokenType.END are not markers we count.
                # UsfmTokenType.UNKNOWN might be ignorable or might need specific handling if it represents a marker.
                # For now, let's count anything that's not TEXT or END as a potential marker occurrence.
                if token.type not in [UsfmTokenType.TEXT, UsfmTokenType.END]:
                    # For BOOK, CHAPTER, VERSE, NOTE, CHARACTER, MILESTONE, PARAGRAPH types,
                    # token.marker usually holds the marker (e.g., "id", "c", "v", "p", "wj", "qt").
                    # token.text for BOOK/CHAPTER/VERSE holds the number/code.
                    # Let's prioritize token.marker for identifying the SFM tag.
                    actual_marker_tag = None
                    if token.marker:
                        actual_marker_tag = token.marker.lower() # Normalize to lowercase
                    elif token.text and token.type != UsfmTokenType.BOOK: # Fallback for some cases, but avoid using book ID as marker
                        actual_marker_tag = token.text.lower()
                    
                    if actual_marker_tag: 
                        full_marker = actual_marker_tag if actual_marker_tag.startswith("\\") else f"\\{actual_marker_tag}"
                        if active_book_id_for_counting: # Only count if we have a book context
                            project_results["SFMMarkersByBook"][active_book_id_for_counting][full_marker] += 1

                # B. Text Content Handling (for words and punctuation)
                # Only process if it's a TEXT token AND we are in a verse text block context
                if token.type == UsfmTokenType.TEXT and token.text and currently_in_verse_text_block:
                    text_content = token.text
                    # Collect text for script detection if within a canonical book context
                    if len("".join(text_sample_for_script_detection)) < SCRIPT_DETECTION_SAMPLE_SIZE:
                        text_sample_for_script_detection.append(text_content)

                    for char_in_text in text_content:
                        if is_word_char(char_in_text):
                            current_word += char_in_text
                        else:
                            if current_word: # A word was just completed
                                # Words are added to AllWordsInProject
                                project_results["AllWordsInProject"].append(current_word.lower())
                                current_word = ""
                            if is_punctuation_char(char_in_text):
                                # Punctuation is counted per book if book context is active
                                if active_book_id_for_counting: 
                                    project_results["PunctuationByBook"][active_book_id_for_counting][char_in_text] += 1
                                    try:
                                        char_name = ud.name(char_in_text)
                                    except ValueError:
                                        char_name = f"U+{ord(char_in_text):04X}" # Unicode codepoint if no name
                                    project_results["PunctuationByNameAndBook"][char_name][active_book_id_for_counting] += 1
                    
                    if current_word: # Catch any word at the end of the text_content block
                        # Words are added to AllWordsInProject
                        project_results["AllWordsInProject"].append(current_word.lower())
                        current_word = "" # Reset for next token or text segment

        project_results["TotalBooksProcessed"] = len(processed_book_ids)
        if not project_results["TotalBooksProcessed"] and project_results["ProcessingStatus"] == "Success" and usfm_files:
            # If USFM files were found but no books processed (e.g., no \id markers)
            project_results["ProcessingStatus"] = "Warning"
            project_results["ErrorMessage"] = "USFM files found, but no book IDs processed (e.g., missing \\id markers)."
        elif not project_results["TotalBooksProcessed"] and project_results["ProcessingStatus"] == "Success" and not usfm_files:
             # This case is handled earlier when usfm_files is empty
            pass
        
        # 5. Perform script detection on the collected text sample
        if text_sample_for_script_detection:
            full_sample_text = "".join(text_sample_for_script_detection)[:SCRIPT_DETECTION_SAMPLE_SIZE]
            script_counts = Counter()
            for char_in_sample in full_sample_text:
                if is_word_char(char_in_sample): # Only consider word-forming characters
                    try:
                        script_name = ud.script(char_in_sample)
                        script_counts[script_name] += 1
                    except ValueError: # Character might not have a script name (e.g. some symbols, spaces)
                        pass
            if script_counts:
                most_common_script = script_counts.most_common(1)[0][0]
                project_results["DetectedScript"] = most_common_script

        # 6. Get verse counts for processed books if sil-machine is available
        if SIL_MACHINE_AVAILABLE and processed_book_ids:
            try:
                # 'settings' is the ParatextProjectSettings object loaded earlier
                # 'project_path_obj' is the Path to the project
                if settings: 
                    corpus = ParatextTextCorpus(str(project_path_obj), settings)
                    book_verse_counts_detail = defaultdict(set) # Stores {book_id: {(b,c,v) tuples}}

                    for book_id_for_verses in processed_book_ids:
                        # Ensure we only try for canonical books that were actually processed
                        if not is_canonical(book_id_for_verses): 
                            continue
                        try:
                            text_obj = corpus.get_text(book_id_for_verses)
                            
                            if text_obj:
                                rows_or_segments_iterator = None
                                if hasattr(text_obj, 'segments'):
                                    rows_or_segments_iterator = text_obj.segments
                                elif hasattr(text_obj, 'get_rows'): # Fallback to get_rows()
                                    try:
                                        rows_or_segments_iterator = text_obj.get_rows()
                                    except Exception as e_get_rows:
                                        msg_get_rows_err = f"Error calling get_rows() for book {book_id_for_verses} in {project_name} (type: {type(text_obj).__name__}): {e_get_rows}. "
                                        if "Warning" not in project_results["ProcessingStatus"] and "Error" not in project_results["ProcessingStatus"]:
                                            project_results["ProcessingStatus"] = "Warning"
                                        project_results["ErrorMessage"] += msg_get_rows_err
                                
                                if rows_or_segments_iterator:
                                    for row_or_segment in rows_or_segments_iterator:
                                        # Both TextRow (from get_rows) and Segment (from .segments) should have a .ref
                                        if hasattr(row_or_segment, 'ref') and row_or_segment.ref:
                                            # Store unique verse references (book_num, chapter_num, verse_num)
                                            book_verse_counts_detail[book_id_for_verses].add(
                                                (row_or_segment.ref.book_num, row_or_segment.ref.chapter_num, row_or_segment.ref.verse_num)
                                            )
                                else: # text_obj exists but no way to get segments/rows was found
                                    msg_no_iterable = f"Text object for book {book_id_for_verses} in {project_name} (type: {type(text_obj).__name__}) provided no means to iterate segments or rows. Cannot count verses. "
                                    if "Warning" not in project_results["ProcessingStatus"] and "Error" not in project_results["ProcessingStatus"]:
                                        project_results["ProcessingStatus"] = "Warning"
                                        project_results["ErrorMessage"] += msg_no_iterable
                            # else: text_obj is None (book not found in corpus), no error message needed here specifically for verse counting
                        except Exception as e_corpus_text:
                            msg_corpus_text_err = f"Error getting/processing text for book {book_id_for_verses} in {project_name} for verse counting: {e_corpus_text}. "
                            # print(msg_corpus_text_err) # For immediate feedback
                            pass
                                
                    project_results["BookStats"] = {
                        book: len(verse_tuples_set) for book, verse_tuples_set in book_verse_counts_detail.items()
                    }
            except Exception as e_corpus_init:
                print(f"Warning: Could not get verse counts for {project_name} via ParatextTextCorpus: {e_corpus_init}")
    except Exception as e:
        project_results["ProcessingStatus"] = "Error"
        project_results["ErrorMessage"] = f"Analysis failed: {type(e).__name__}: {str(e)}"
        print(f"Error analyzing project {project_name}: {e}")
        import traceback
        traceback.print_exc()

    return project_results


# --- Report Generation ---
def generate_detailed_project_report(project_results, output_folder, num_extreme_words):
    """Generates the detailed XLSX report for a single project."""
    output_folder_path = Path(output_folder)
    project_name = project_results["ProjectName"]
    output_path = output_folder_path / f"{project_name}_details.xlsx"

    # --- Calculate summary-level aggregates for this single project ---
    project_summary_aggregates = {}
    # SFM
    sfm_summary_counter_project = Counter()
    for book_markers in project_results.get("SFMMarkersByBook", {}).values():
        sfm_summary_counter_project.update(book_markers) # No exclusion list here, full project data
    project_summary_aggregates["TotalUniqueSFMMarkers_Project"] = len(sfm_summary_counter_project)
    project_summary_aggregates["TotalSFMMarkerInstances_Project"] = sum(sfm_summary_counter_project.values())
    project_summary_aggregates["TopNCommonSFMMarkers_Project"] = ", ".join(f"{m} ({c})" for m, c in sfm_summary_counter_project.most_common(N_MARKERS))

    # Punctuation
    punct_summary_counter_project = Counter()
    for book_puncts in project_results.get("PunctuationByBook", {}).values():
        punct_summary_counter_project.update(book_puncts)
    project_summary_aggregates["TotalUniquePunctuationChars_Project"] = len(punct_summary_counter_project)
    project_summary_aggregates["TotalPunctuationInstances_Project"] = sum(punct_summary_counter_project.values())
    project_summary_aggregates["TopNCommonPunctuation_Project"] = ", ".join(f"{p} ({c})" for p, c in punct_summary_counter_project.most_common(N_PUNCTUATION))

    # Word Extremes
    if project_results.get("AllWordsInProject"):
        unique_words_project = sorted(list(set(project_results["AllWordsInProject"])), key=lambda w: (len(w), w))
        project_summary_aggregates[f"{num_extreme_words}_ShortestWords_Project"] = ", ".join(unique_words_project[:num_extreme_words])
        project_summary_aggregates[f"{num_extreme_words}_LongestWords_Project"] = ", ".join(unique_words_project[-num_extreme_words:])
    else:
        project_summary_aggregates[f"{num_extreme_words}_ShortestWords_Project"] = ""
        project_summary_aggregates[f"{num_extreme_words}_LongestWords_Project"] = ""
    # --- End Calculate summary-level aggregates ---

    try:
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Sheet 1: Project_Metadata
            meta_cols = [
                "ProjectName", "ProjectFolderPath", "ProcessingStatus", "ErrorMessage",
                "DateAnalyzed", "TotalBooksProcessed", 
                "LanguageCode", 
                "DetectedScript", "ScriptDirection", "HasCustomSty"
            ]
            meta_df_data = {k: [project_results.get(k, "")] for k in meta_cols}
            pd.DataFrame(meta_df_data).to_excel(writer, sheet_name="Project_Metadata", index=False)

            # Sheet 2: SFM_Markers_By_Book
            # Pivot: SFMMarker rows, BookID columns, SFM Marker counts as values
            sfm_pivot_ready_data = defaultdict(lambda: defaultdict(int))
            seen_book_ids_for_sfm = set()
            for book_id, markers_counter in project_results["SFMMarkersByBook"].items():
                seen_book_ids_for_sfm.add(book_id)
                for marker, count in markers_counter.items():
                    sfm_pivot_ready_data[marker][book_id] = count
            
            if sfm_pivot_ready_data:
                sfm_pivot_df = pd.DataFrame.from_dict(sfm_pivot_ready_data, orient='index').fillna(0).astype(int)
                ordered_book_cols_sfm = [b for b in BOOK_ORDER if b in seen_book_ids_for_sfm]
                for book_col in ordered_book_cols_sfm: # Ensure all relevant book columns exist
                    if book_col not in sfm_pivot_df.columns:
                        sfm_pivot_df[book_col] = 0
                if ordered_book_cols_sfm:
                    sfm_pivot_df = sfm_pivot_df[ordered_book_cols_sfm] # Order columns
                sfm_pivot_df = sfm_pivot_df.sort_index() # Sort SFMMarkers alphabetically
                sfm_pivot_df.to_excel(writer, sheet_name="SFM_Markers_By_Book", index=True, index_label="SFMMarker")
            else:
                pd.DataFrame().to_excel(writer, sheet_name="SFM_Markers_By_Book", index=False)

            # Sheet 3: Punctuation_By_Book (Pivot: UnicodeName rows, BookID columns, Punctuation counts values)
            punct_by_name_data = project_results["PunctuationByNameAndBook"]
            if punct_by_name_data:
                punct_pivot_df = pd.DataFrame.from_dict(punct_by_name_data, orient='index').fillna(0).astype(int)

                seen_book_ids_for_punct = set()
                for book_counts_for_name in punct_by_name_data.values():
                    seen_book_ids_for_punct.update(book_counts_for_name.keys())
                
                ordered_book_cols_punct = [b for b in BOOK_ORDER if b in seen_book_ids_for_punct]

                for book_col in ordered_book_cols_punct: # Ensure all relevant book columns exist
                    if book_col not in punct_pivot_df.columns:
                        punct_pivot_df[book_col] = 0
                if ordered_book_cols_punct:
                    punct_pivot_df = punct_pivot_df[ordered_book_cols_punct] # Order columns
                
                punct_pivot_df = punct_pivot_df.sort_index() # Sort UnicodeNames alphabetically
                punct_pivot_df.to_excel(writer, sheet_name="Punctuation_By_Book", index=True, index_label="UnicodeName")
            else:
                pd.DataFrame().to_excel(writer, sheet_name="Punctuation_By_Book", index=False)

            # Sheet 4: Word_Extremes
            extreme_words_data = []
            if project_results["AllWordsInProject"]:
                # Get unique words, sort by length, then alphabetically for tie-breaking
                unique_words = sorted(list(set(project_results["AllWordsInProject"])), key=lambda w: (len(w), w))
                shortest = unique_words[:num_extreme_words]
                longest = unique_words[-num_extreme_words:]
                for w in shortest: extreme_words_data.append({"Type": "Shortest", "Word": w, "Length": len(w)})
                for w in longest: extreme_words_data.append({"Type": "Longest", "Word": w, "Length": len(w)})
            extreme_df = pd.DataFrame(extreme_words_data if extreme_words_data else [], columns=["Type", "Word", "Length"])
            extreme_df.to_excel(writer, sheet_name="Word_Extremes_Project", index=False)

            # Sheet: Book_Stats
            book_stats_dict = project_results.get("BookStats", {})
            if book_stats_dict:
                book_stats_list = [{"BookCode": book, "VerseCount": count} 
                                   for book, count in book_stats_dict.items()]
                book_stats_df = pd.DataFrame(book_stats_list, columns=["BookCode", "VerseCount"])
                
                # Sort by canonical book order
                book_stats_df['BookCode'] = pd.Categorical(book_stats_df['BookCode'], categories=BOOK_ORDER, ordered=True)
                book_stats_df = book_stats_df.sort_values('BookCode').dropna(subset=['BookCode'])
                if not book_stats_df.empty: # Ensure VerseCount is int if df is not empty
                    book_stats_df['VerseCount'] = book_stats_df['VerseCount'].astype(int)
                
                book_stats_df.to_excel(writer, sheet_name="Book_Stats", index=False)
            else:
                pd.DataFrame(columns=["BookCode", "VerseCount"]).to_excel(writer, sheet_name="Book_Stats", index=False)

            # Sheet 5: Project_Summary_Data (for this project)
            # Combine basic metadata with calculated aggregates for this sheet
            summary_data_for_sheet = {**meta_df_data, **{k: [v] for k,v in project_summary_aggregates.items()}}
            # Ensure all expected columns are present for consistency, even if empty
            expected_summary_cols = meta_cols + list(project_summary_aggregates.keys())
            for col in expected_summary_cols:
                if col not in summary_data_for_sheet:
                    summary_data_for_sheet[col] = [""]
            
            pd.DataFrame(summary_data_for_sheet).to_excel(writer, sheet_name="Project_Summary_Data", index=False)

        print(f"Detailed report generated: {output_path}")
        return True # Indicate success
    except Exception as e:
        print(f"Error generating detailed report for {project_name}: {e}")
        project_results["ProcessingStatus"] = "Error"
        project_results["ErrorMessage"] = f"Report generation failed: {str(e)}"
        # We might want to log this error more formally if the detailed report fails to write
        return False # Indicate failure


def collate_master_summary_report(main_output_folder, details_output_folder_override, num_extreme_words, sfm_exclude_list=None):
    """
    Collates data from individual project_details.xlsx files to create the master summary.
    Scans for detailed reports in details_output_folder_override if provided, otherwise in main_output_folder.
    """
    print("\nCollating master summary report from detailed project files...")
    
    detailed_reports_folder = details_output_folder_override if details_output_folder_override else main_output_folder
    if not Path(detailed_reports_folder).exists():
        print(f"Error: Folder for detailed reports '{detailed_reports_folder}' not found. Cannot collate summary.")
        return

    detail_files = list(Path(detailed_reports_folder).glob("*_details.xlsx"))

    if not detail_files:
        print(f"No detailed project reports (*_details.xlsx) found in '{detailed_reports_folder}'. Cannot generate master summary.")
        return

    print(f"Found {len(detail_files)} detailed project reports to collate.")

    summary_list = []
    for detail_file_path in tqdm(detail_files, desc="Collating summaries"):
        try:
            # Read the "Project_Summary_Data" sheet which should contain a single row
            project_summary_df = pd.read_excel(detail_file_path, sheet_name="Project_Summary_Data")
            if not project_summary_df.empty:
                project_entry = project_summary_df.iloc[0].to_dict()
                
                # Apply SFM exclusion list for the master summary's TopN
                # We need to re-read the SFMMarkersByBook to do this accurately if exclusions apply
                # Or, the detailed summary could store the full counter, and we filter here.
                # For now, let's assume the TopN in Project_Summary_Data is project-wide (no exclusions)
                # and we'll use those directly. If sfm_exclude_list is active, this TopN might differ.
                # A more robust way: store raw counters in Project_Summary_Data or re-calc here.
                # For simplicity, we'll use the pre-calculated TopN from the detail file for now.
                
                # Rename keys from "_Project" to "_Summary" for the master file for clarity
                master_summary_entry = {}
                for key, value in project_entry.items():
                    if key.endswith("_Project"):
                        master_summary_entry[key.replace("_Project", "_Summary")] = value
                    else:
                        master_summary_entry[key] = value
                
                master_summary_entry["PathToDetailedReport"] = str(detail_file_path.resolve())
                summary_list.append(master_summary_entry)
            else:
                print(f"Warning: 'Project_Summary_Data' sheet in {detail_file_path} is empty. Skipping.")
        except Exception as e:
            #print(f"Warning: Could not read or process summary data from {detail_file_path}: {e}")
            # Optionally, create a basic entry with error status for this project
            project_name_from_filename = detail_file_path.name.replace("_details.xlsx", "")
            summary_list.append({
                "ProjectName": project_name_from_filename,
                "ProcessingStatus": "Error reading detail file",
                "ErrorMessage": str(e),
                "PathToDetailedReport": str(detail_file_path.resolve())
            })

    if not summary_list:
        print("No valid project summary data found in detailed files. Master summary not generated.")
        return

    summary_df = pd.DataFrame(summary_list)
    # Define column order for the summary report
    summary_column_order = [
        "ProjectName", "ProcessingStatus", "ErrorMessage", "DateAnalyzed",
        "TotalBooksProcessed", "LanguageCode", 
        "DetectedScript", "ScriptDirection", "HasCustomSty",
        "TotalUniqueSFMMarkers_Summary", "TotalSFMMarkerInstances_Summary", "TopNCommonSFMMarkers_Summary",
        "TotalUniquePunctuationChars_Summary", "TotalPunctuationInstances_Summary", "TopNCommonPunctuation_Summary",
        f"{num_extreme_words}_ShortestWords_Summary", f"{num_extreme_words}_LongestWords_Summary",
        "PathToDetailedReport", "ProjectFolderPath" # PathToDetailedReport before ProjectFolderPath
    ]
    # Ensure all columns are present, adding any missing ones
    for col in summary_column_order:
        if col not in summary_df.columns:
            summary_df[col] = "" # Add as empty string if missing

    summary_df = summary_df[summary_column_order] # Reorder

    summary_xlsx_path = os.path.join(main_output_folder, "project_analysis_summary.xlsx")
    summary_csv_path = os.path.join(main_output_folder, "project_analysis_summary.csv")

    try:
        summary_df.to_excel(summary_xlsx_path, index=False, engine='openpyxl')
        print(f"Master summary report generated: {summary_xlsx_path}")
        summary_df.to_csv(summary_csv_path, index=False)
        print(f"Master summary CSV generated: {summary_csv_path}")
    except Exception as e:
        print(f"Error generating master summary report: {e}")


# --- Main Execution ---
def main():
    load_dotenv() # Load environment variables from .env file

    default_projects_folder = os.getenv("PROJECTS_FOLDER")
    default_output_folder = os.getenv("OUTPUT_FOLDER")
    default_details_output_folder = os.getenv("DETAILS_OUTPUT_FOLDER")
    process_n_projects_env = os.getenv("PROCESS_N_PROJECTS")
    book_filter_env = os.getenv("BOOK_FILTER")

    parser = argparse.ArgumentParser(description="Analyze Paratext project folders using sil-machine.")
    parser.add_argument(
        "projects_folder", nargs="?", default=default_projects_folder,
        help="Path to the folder containing Paratext projects (overrides .env PROJECTS_FOLDER)."
    )
    parser.add_argument(
        "--output_folder", default=default_output_folder,
        help="Path to the folder where reports will be saved (overrides .env OUTPUT_FOLDER)."
    )
    parser.add_argument(
        "--details_output_folder", default=default_details_output_folder,
        help="Path to a separate folder for detailed project reports.  If specified, detailed reports go here, otherwise they go to the main output_folder."
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Force reprocessing of projects and regeneration of detailed reports even if they exist."
    )
    parser.add_argument(
        "--n_words", type=int, default=N_WORDS,
        help=f"Number of shortest and longest words to record (default: {N_WORDS})."
    )
    parser.add_argument(
        "--exclude_sfm_summary", type=str, default="",
        help="Comma-separated list of SFM markers to exclude from summary statistics (e.g., \\id,\\usfm)."
    )
    parser.add_argument(
        "--process_n_projects", type=int,
        help="Limit the number of projects to process (overrides .env PROCESS_N_PROJECTS)."
    )
    parser.add_argument(
        "--book_filter", type=str,
        help="Comma-separated list of Book IDs to process (e.g., GEN,PSA,MAT). Overrides .env BOOK_FILTER."
    )
    args = parser.parse_args()

    if not args.projects_folder:
        print("Error: Projects folder not specified via argument or .env file (PROJECTS_FOLDER).")
        return
    if not args.output_folder:
        print("Error: Output folder not specified via argument or .env file (OUTPUT_FOLDER).")
        return

    main_output_folder_path = Path(args.output_folder)
    main_output_folder_path.mkdir(parents=True, exist_ok=True)
    
    details_output_folder_path = main_output_folder_path
    # If a specific details_output_folder is provided (either by arg or .env), use it and create it.
    if args.details_output_folder:
        details_output_folder_path = Path(args.details_output_folder)
        details_output_folder_path.mkdir(parents=True, exist_ok=True)
        print(f"Detailed reports will be saved in: {details_output_folder_path}")
    
    sfm_exclusion_list_for_summary = [marker.strip() for marker in args.exclude_sfm_summary.split(',') if marker.strip()]

    # Determine the limit for N projects
    limit_n_projects = None
    if args.process_n_projects is not None:
        limit_n_projects = args.process_n_projects
    elif process_n_projects_env:
        try:
            limit_n_projects = int(process_n_projects_env)
            if limit_n_projects <= 0: limit_n_projects = None # Treat 0 or negative as no limit
        except ValueError:
            print(f"Warning: Invalid value for PROCESS_N_PROJECTS in .env: '{process_n_projects_env}'. Processing all projects.")
    
    # Determine the book filter list
    active_book_filter = None
    if args.book_filter:
        active_book_filter = {book_id.strip().upper() for book_id in args.book_filter.split(',') if book_id.strip()}
    elif book_filter_env:
        active_book_filter = {book_id.strip().upper() for book_id in book_filter_env.split(',') if book_id.strip()}

    print(f"Scanning for projects in: {args.projects_folder}")
    # Pass the determined limit_n_projects and active_book_filter to get_project_paths
    project_paths = get_project_paths(args.projects_folder, limit_n_projects, active_book_filter)

    if not project_paths:
        print("No Paratext projects found (or none met the filter criteria for initial scan).")
        return
    
    # The number of projects to *actually process* might be further limited by limit_n_projects,
    # even if get_project_paths returned more (e.g., due to date logic selecting multiple from a pre-limited scan)
    print(f"Selected {len(project_paths)} project(s) after date filtering and initial scan limits.")
    projects_processed_count = 0

    for proj_path in project_paths:
        if limit_n_projects is not None and projects_processed_count >= limit_n_projects:
            print(f"Reached processing limit of {limit_n_projects} projects.")
            break

        # proj_path is already a Path object from get_project_paths if changes applied
        project_name = proj_path.name 
        detailed_report_path = details_output_folder_path / f"{project_name}_details.xlsx"

        current_project_data = None
        if not args.force and Path(detailed_report_path).exists():
            print(f"Detailed report for {project_name} already exists. Skipping analysis (use --force to override).")
            # To include this in the summary, we would need to load its summary-relevant data.
            # For now, skipped projects are not included in the current summary run.
            # A more advanced version could load data from the existing XLSX.
            # For this version, if you want a complete summary, use --force or delete old detailed files.
            continue # Skip to the next project

        current_project_data = analyze_project_data(str(proj_path), args.n_words, active_book_filter) # analyze_project_data expects str or Path
        current_project_data["ActualDetailedReportPath"] = detailed_report_path # Store where it will be saved

        if current_project_data: # If analysis ran (even if it resulted in an error status)
            if current_project_data.get("ProcessingStatus") != "Error" or args.force:
                 # Generate detailed report if analysis was successful, or if forced
                 # (even if analysis had warnings, we might still want the report)
                 if current_project_data.get("ProcessingStatus") != "Error in Main Loop": # Avoid if main loop itself failed before analysis
                    report_generated_successfully = generate_detailed_project_report(current_project_data, str(details_output_folder_path), args.n_words)
                    if report_generated_successfully:
                        projects_processed_count += 1
                    # else: error already printed by generate_detailed_project_report
            # No longer appending to all_project_analysis_results

    # After all projects are processed (or limit is reached), collate the master summary
    collate_master_summary_report(str(main_output_folder_path), str(details_output_folder_path) if args.details_output_folder else None, args.n_words, sfm_exclusion_list_for_summary)
    
    print(f"\nFinished processing. {projects_processed_count} projects had detailed reports generated or updated in this run.")

if __name__ == "__main__":
    main()
