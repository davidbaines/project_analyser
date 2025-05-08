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
import multiprocessing
import traceback

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

except ImportError:
    print("Warning: One or more sil-machine components could not be imported. Functionality may be limited.")
    # Optionally, re-raise or sys.exit if sil-machine is critical
    # raise

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
    if category == 'Nd': # Exclude standalone digits
        return False
    return category.startswith('L') or \
           category == 'Pc' or \
           category.startswith('M')

def is_punctuation_char(char):
    """Determines if a character is punctuation based on Unicode category."""
    return ud.category(char).startswith('P')

def is_paratext_project_folder(candidate_path: Path) -> bool:
    """
    Checks if a folder 'looks like' a Paratext project.
    """
    if not candidate_path.is_dir():
        return False
    has_settings_xml = (candidate_path / "Settings.xml").is_file()
    if not has_settings_xml:
        return False
    has_sfm_files = any(f.suffix.lower() == ".sfm" for f in candidate_path.iterdir() if f.is_file())
    has_usfm_files = any(f.suffix.lower() == ".usfm" for f in candidate_path.iterdir() if f.is_file())
    return has_sfm_files or has_usfm_files

DATE_SUFFIX_PATTERN = re.compile(r"(.+?)(_(\d{4})_(\d{2})_(\d{2})|_(\d{8}))$")

def project_contains_filtered_books(project_path: Path, book_filter_list: set) -> bool:
    if not book_filter_list:
        return True
    try:
        settings_parser = FileParatextProjectSettingsParser(str(project_path))
        settings = settings_parser.parse()
    except Exception: # Broad catch if settings parsing itself fails
        return False # Cannot determine if it contains books if settings are unreadable

    if not settings or not hasattr(settings, 'get_book_file_name'):
        return False

    found_books_in_filter = set()
    for book_id_filter in book_filter_list:
        try:
            expected_filename = settings.get_book_file_name(book_id_filter)
            if (project_path / expected_filename).exists():
                found_books_in_filter.add(book_id_filter)
        except Exception:
            continue
    return found_books_in_filter == book_filter_list

def get_project_paths(base_folder, limit_n_projects_to_scan=None, active_book_filter_for_scan=None):
    base_folder_path = Path(base_folder)
    candidate_paths = []
    if not base_folder_path.is_dir():
        print(f"Error: Projects folder '{base_folder}' not found or is not a directory.")
        return candidate_paths

    qualifying_projects_found_count = 0
    # Use a simple list for iteration if not using tqdm, or adapt tqdm for non-interactive use in workers if needed
    folder_items = list(base_folder_path.iterdir())
    
    # The tqdm progress bar is better in the main thread before parallel processing starts.
    # If this function is called from a worker, tqdm might behave unexpectedly.
    # For now, assuming it's called from main_mp before pool creation.
    for item_path in tqdm(folder_items, desc="Scanning for project folders (initial pass)"):
        project_path_obj = item_path
        if is_paratext_project_folder(project_path_obj):
            if limit_n_projects_to_scan is not None:
                if active_book_filter_for_scan:
                    if project_contains_filtered_books(project_path_obj, active_book_filter_for_scan):
                        candidate_paths.append(project_path_obj)
                        qualifying_projects_found_count += 1
                else:
                    candidate_paths.append(project_path_obj)
                    qualifying_projects_found_count += 1
                
                if qualifying_projects_found_count >= limit_n_projects_to_scan:
                    print(f"\nFound {limit_n_projects_to_scan} qualifying project(s) for initial scan. Stopping directory search.")
                    break 
            else:
                candidate_paths.append(project_path_obj)

    projects_by_base_name = defaultdict(list)
    for path in candidate_paths:
        match = DATE_SUFFIX_PATTERN.match(path.name)
        if match:
            base_name = match.group(1)
            date_obj = None
            if match.group(3):
                year, month, day = int(match.group(3)), int(match.group(4)), int(match.group(5))
                date_obj = datetime(year, month, day)
            elif match.group(6):
                date_str = match.group(6)
                date_obj = datetime(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:]))
            projects_by_base_name[base_name].append({"path": path, "date": date_obj})
        else:
            projects_by_base_name[path.name].append({"path": path, "date": None})

    final_project_paths = []
    for base_name, versions in projects_by_base_name.items():
        undated_versions = [v for v in versions if v["date"] is None]
        dated_versions = sorted([v for v in versions if v["date"] is not None], key=lambda x: x["date"], reverse=True)

        if dated_versions:
            final_project_paths.append(dated_versions[0]["path"])
        elif undated_versions:
            final_project_paths.append(undated_versions[0]["path"])
            
    return final_project_paths

def analyze_project_data(project_path, num_extreme_words, book_filter_list=None):
    project_path_obj = Path(project_path)
    project_name = project_path_obj.name
    # In multiprocessing, direct print might be messy. Consider logging or returning messages.
    # For now, keeping print for simplicity, but be aware of interleaved output.
    # print(f"Analyzing project: {project_name} (PID: {os.getpid()})...")

    project_results = {
        "ProjectName": project_name,
        "ProjectFolderPath": str(project_path_obj),
        "ProcessingStatus": "Success",
        "ErrorMessage": "",
        "DateAnalyzed": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "TotalBooksProcessed": 0,
        "DetectedScript": "Unknown",
        "ScriptDirection": "Unknown",
        "HasCustomSty": False,
        "LanguageCode": "Unknown",
        "SFMMarkersByBook": defaultdict(Counter),
        "PunctuationByBook": defaultdict(Counter),
        "PunctuationByNameAndBook": defaultdict(lambda: defaultdict(int)),
        "AllWordsInProject": [],
    }

    try:
        custom_sty_path = project_path_obj / "custom.sty"
        project_results["HasCustomSty"] = custom_sty_path.exists()

        settings = None # Initialize settings
        try:
            settings_parser = FileParatextProjectSettingsParser(str(project_path_obj))
            settings = settings_parser.parse()
            if settings:
                project_results["ScriptDirection"] = "RTL" if getattr(settings, 'is_right_to_left', False) else "LTR"
                project_results["LanguageCode"] = getattr(settings, 'language_code', 'Unknown')
                if project_results["LanguageCode"] == 'Unknown':
                    project_results["ProcessingStatus"] = "Warning"
                    project_results["ErrorMessage"] += "LanguageCode (ISO code) missing from settings. "
            else:
                warning_msg = f"Settings.xml for {project_name} could not be parsed or was empty. "
                project_results["ProcessingStatus"] = "Warning"
                project_results["ErrorMessage"] += warning_msg
        except Exception as e_settings:
            warning_msg = f"Error accessing Settings.xml for {project_name}: {e_settings}. "
            project_results["ProcessingStatus"] = "Warning"
            project_results["ErrorMessage"] += warning_msg

        try:
            default_sil_stylesheet_path = Path(machine.corpora.__file__).parent / "usfm.sty"
            if default_sil_stylesheet_path.exists():
                stylesheet = UsfmStylesheet(str(default_sil_stylesheet_path))
            else:
                raise FileNotFoundError("Default SIL usfm.sty not found.")
        except Exception as e_stylesheet:
            project_results["ProcessingStatus"] = "Error"
            project_results["ErrorMessage"] = f"Default stylesheet loading failed: {e_stylesheet}"
            return project_results

        tokenizer = UsfmTokenizer(stylesheet)

        usfm_file_patterns = ["*.SFM", "*.sfm", "*.USFM", "*.usfm"]
        files_to_process = []
        if book_filter_list and settings and hasattr(settings, 'get_book_file_name'):
            for book_id_from_filter in book_filter_list:
                if not is_canonical(book_id_from_filter):
                    continue
                try:
                    expected_filename_str = settings.get_book_file_name(book_id_from_filter)
                    expected_file_path = project_path_obj / expected_filename_str
                    if expected_file_path.exists():
                        files_to_process.append(expected_file_path)
                except Exception: # Ignore errors getting filename for a specific book
                    pass # Error message for this already handled in project_contains_filtered_books or main loop
        else:
            for pattern in usfm_file_patterns:
                files_to_process.extend(project_path_obj.glob(pattern))

        if not files_to_process:
            project_results["ProcessingStatus"] = "Warning"
            project_results["ErrorMessage"] += "No USFM files to process (either none found or none matched filter)."
            # No return here, script detection might still run if sample was collected from other means (not current logic)

        processed_book_ids = set()
        text_sample_for_script_detection = []

        for usfm_file_path in files_to_process:
            try:
                with open(usfm_file_path, "r", encoding="utf-8-sig") as file:
                    content = file.read()
            except Exception: # Skip file if unreadable
                continue 

            usfm_tokens = tokenizer.tokenize(content)
            current_word = ""
            current_book_id_for_file = None 
            currently_in_verse_text_block = False
            
            for token in usfm_tokens:
                if token.type == UsfmTokenType.BOOK:
                    book_code_candidate = None
                    if token.data and isinstance(token.data, str) and token.data.strip():
                        book_code_candidate = token.data.strip().upper()
                    elif token.text and isinstance(token.text, str) and token.text.strip() and token.text.upper() != "NONE":
                        book_code_candidate = token.text.strip().upper()
                    
                    if book_code_candidate:
                        if not is_canonical(book_code_candidate): 
                            current_book_id_for_file = None 
                            currently_in_verse_text_block = False 
                            continue
                        if book_filter_list and book_code_candidate not in book_filter_list:
                            current_book_id_for_file = None 
                            currently_in_verse_text_block = False
                            continue
                        current_book_id_for_file = book_code_candidate
                        processed_book_ids.add(current_book_id_for_file)

                active_book_id_for_counting = current_book_id_for_file

                if token.type == UsfmTokenType.VERSE:
                    currently_in_verse_text_block = True
                elif token.type in [UsfmTokenType.BOOK, UsfmTokenType.CHAPTER, UsfmTokenType.NOTE]:
                    currently_in_verse_text_block = False

                if token.type not in [UsfmTokenType.TEXT, UsfmTokenType.END]:
                    actual_marker_tag = None
                    if token.marker:
                        actual_marker_tag = token.marker.lower()
                    elif token.text and token.type != UsfmTokenType.BOOK:
                        actual_marker_tag = token.text.lower()
                    
                    if actual_marker_tag: 
                        full_marker = actual_marker_tag if actual_marker_tag.startswith("\\") else f"\\{actual_marker_tag}"
                        if active_book_id_for_counting:
                            project_results["SFMMarkersByBook"][active_book_id_for_counting][full_marker] += 1

                if token.type == UsfmTokenType.TEXT and token.text and currently_in_verse_text_block:
                    text_content = token.text
                    if len("".join(text_sample_for_script_detection)) < SCRIPT_DETECTION_SAMPLE_SIZE:
                        text_sample_for_script_detection.append(text_content)

                    for char_in_text in text_content:
                        if is_word_char(char_in_text):
                            current_word += char_in_text
                        else:
                            if current_word:
                                project_results["AllWordsInProject"].append(current_word.lower())
                                current_word = ""
                            if is_punctuation_char(char_in_text):
                                if active_book_id_for_counting: 
                                    project_results["PunctuationByBook"][active_book_id_for_counting][char_in_text] += 1
                                    try:
                                        char_name = ud.name(char_in_text)
                                    except ValueError:
                                        char_name = f"U+{ord(char_in_text):04X}"
                                    project_results["PunctuationByNameAndBook"][char_name][active_book_id_for_counting] += 1
                    if current_word:
                        project_results["AllWordsInProject"].append(current_word.lower())
                        current_word = ""

        project_results["TotalBooksProcessed"] = len(processed_book_ids)
        if not project_results["TotalBooksProcessed"] and project_results["ProcessingStatus"] == "Success" and files_to_process:
            project_results["ProcessingStatus"] = "Warning"
            project_results["ErrorMessage"] += "USFM files processed, but no book IDs recognized (e.g., missing \\id markers or filter mismatch)."
        
        if text_sample_for_script_detection:
            full_sample_text = "".join(text_sample_for_script_detection)[:SCRIPT_DETECTION_SAMPLE_SIZE]
            script_counts = Counter()
            for char_in_sample in full_sample_text:
                if is_word_char(char_in_sample):
                    try:
                        script_name = ud.script(char_in_sample)
                        script_counts[script_name] += 1
                    except ValueError:
                        pass
            if script_counts:
                most_common_script = script_counts.most_common(1)[0][0]
                project_results["DetectedScript"] = most_common_script

    except Exception as e:
        project_results["ProcessingStatus"] = "Error"
        project_results["ErrorMessage"] = f"Analysis failed: {type(e).__name__}: {str(e)}. Trace: {traceback.format_exc()}"
        # print(f"Error analyzing project {project_name} (PID: {os.getpid()}): {e}")
        # traceback.print_exc() # This might be too verbose for general use from workers

    return project_results

def generate_detailed_project_report(project_results, output_folder_str, num_extreme_words):
    output_folder_path = Path(output_folder_str)
    project_name = project_results["ProjectName"]
    output_path = output_folder_path / f"{project_name}_details.xlsx"

    project_summary_aggregates = {}
    sfm_summary_counter_project = Counter()
    for book_markers in project_results.get("SFMMarkersByBook", {}).values():
        sfm_summary_counter_project.update(book_markers)
    project_summary_aggregates["TotalUniqueSFMMarkers_Project"] = len(sfm_summary_counter_project)
    project_summary_aggregates["TotalSFMMarkerInstances_Project"] = sum(sfm_summary_counter_project.values())
    project_summary_aggregates["TopNCommonSFMMarkers_Project"] = ", ".join(f"{m} ({c})" for m, c in sfm_summary_counter_project.most_common(N_MARKERS))

    punct_summary_counter_project = Counter()
    for book_puncts in project_results.get("PunctuationByBook", {}).values():
        punct_summary_counter_project.update(book_puncts)
    project_summary_aggregates["TotalUniquePunctuationChars_Project"] = len(punct_summary_counter_project)
    project_summary_aggregates["TotalPunctuationInstances_Project"] = sum(punct_summary_counter_project.values())
    project_summary_aggregates["TopNCommonPunctuation_Project"] = ", ".join(f"{p} ({c})" for p, c in punct_summary_counter_project.most_common(N_PUNCTUATION))

    if project_results.get("AllWordsInProject"):
        unique_words_project = sorted(list(set(project_results["AllWordsInProject"])), key=lambda w: (len(w), w))
        project_summary_aggregates[f"{num_extreme_words}_ShortestWords_Project"] = ", ".join(unique_words_project[:num_extreme_words])
        project_summary_aggregates[f"{num_extreme_words}_LongestWords_Project"] = ", ".join(unique_words_project[-num_extreme_words:])
    else:
        project_summary_aggregates[f"{num_extreme_words}_ShortestWords_Project"] = ""
        project_summary_aggregates[f"{num_extreme_words}_LongestWords_Project"] = ""

    try:
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            meta_cols = [
                "ProjectName", "ProjectFolderPath", "ProcessingStatus", "ErrorMessage",
                "DateAnalyzed", "TotalBooksProcessed", "LanguageCode",
                "DetectedScript", "ScriptDirection", "HasCustomSty"
            ]
            meta_df_data = {k: [project_results.get(k, "")] for k in meta_cols}
            pd.DataFrame(meta_df_data).to_excel(writer, sheet_name="Project_Metadata", index=False)

            sfm_pivot_ready_data = defaultdict(lambda: defaultdict(int))
            seen_book_ids_for_sfm = set()
            for book_id, markers_counter in project_results["SFMMarkersByBook"].items():
                seen_book_ids_for_sfm.add(book_id)
                for marker, count in markers_counter.items():
                    sfm_pivot_ready_data[marker][book_id] = count
            
            if sfm_pivot_ready_data:
                sfm_pivot_df = pd.DataFrame.from_dict(sfm_pivot_ready_data, orient='index').fillna(0).astype(int)
                ordered_book_cols_sfm = [b for b in BOOK_ORDER if b in seen_book_ids_for_sfm]
                for book_col in ordered_book_cols_sfm:
                    if book_col not in sfm_pivot_df.columns: sfm_pivot_df[book_col] = 0
                if ordered_book_cols_sfm: sfm_pivot_df = sfm_pivot_df[ordered_book_cols_sfm]
                sfm_pivot_df = sfm_pivot_df.sort_index()
                sfm_pivot_df.to_excel(writer, sheet_name="SFM_Markers_By_Book", index=True, index_label="SFMMarker")
            else:
                pd.DataFrame().to_excel(writer, sheet_name="SFM_Markers_By_Book", index=False)

            punct_by_name_data = project_results["PunctuationByNameAndBook"]
            if punct_by_name_data:
                punct_pivot_df = pd.DataFrame.from_dict(punct_by_name_data, orient='index').fillna(0).astype(int)
                seen_book_ids_for_punct = set()
                for book_counts_for_name in punct_by_name_data.values():
                    seen_book_ids_for_punct.update(book_counts_for_name.keys())
                ordered_book_cols_punct = [b for b in BOOK_ORDER if b in seen_book_ids_for_punct]
                for book_col in ordered_book_cols_punct:
                    if book_col not in punct_pivot_df.columns: punct_pivot_df[book_col] = 0
                if ordered_book_cols_punct: punct_pivot_df = punct_pivot_df[ordered_book_cols_punct]
                punct_pivot_df = punct_pivot_df.sort_index()
                punct_pivot_df.to_excel(writer, sheet_name="Punctuation_By_Book", index=True, index_label="UnicodeName")
            else:
                pd.DataFrame().to_excel(writer, sheet_name="Punctuation_By_Book", index=False)

            extreme_words_data = []
            if project_results["AllWordsInProject"]:
                unique_words = sorted(list(set(project_results["AllWordsInProject"])), key=lambda w: (len(w), w))
                shortest = unique_words[:num_extreme_words]
                longest = unique_words[-num_extreme_words:]
                for w in shortest: extreme_words_data.append({"Type": "Shortest", "Word": w, "Length": len(w)})
                for w in longest: extreme_words_data.append({"Type": "Longest", "Word": w, "Length": len(w)})
            extreme_df = pd.DataFrame(extreme_words_data if extreme_words_data else [], columns=["Type", "Word", "Length"])
            extreme_df.to_excel(writer, sheet_name="Word_Extremes_Project", index=False)

            summary_data_for_sheet = {**meta_df_data, **{k: [v] for k,v in project_summary_aggregates.items()}}
            expected_summary_cols = meta_cols + list(project_summary_aggregates.keys())
            for col in expected_summary_cols:
                if col not in summary_data_for_sheet: summary_data_for_sheet[col] = [""]
            pd.DataFrame(summary_data_for_sheet).to_excel(writer, sheet_name="Project_Summary_Data", index=False)

        # print(f"Detailed report generated: {output_path} (PID: {os.getpid()})")
        return True
    except Exception as e:
        # print(f"Error generating detailed report for {project_name} (PID: {os.getpid()}): {e}")
        # This error should be caught by the worker and returned.
        # If generate_detailed_project_report is called directly, this error handling is still useful.
        # For the worker, the worker's try-except will catch this.
        raise # Re-raise for the worker to catch and report

def collate_master_summary_report(main_output_folder_str, details_output_folder_override_str, num_extreme_words, sfm_exclude_list=None):
    print("\nCollating master summary report from detailed project files...")
    
    detailed_reports_folder_str = details_output_folder_override_str if details_output_folder_override_str else main_output_folder_str
    detailed_reports_folder = Path(detailed_reports_folder_str)

    if not detailed_reports_folder.exists():
        print(f"Error: Folder for detailed reports '{detailed_reports_folder}' not found. Cannot collate summary.")
        return

    detail_files = list(detailed_reports_folder.glob("*_details.xlsx"))

    if not detail_files:
        print(f"No detailed project reports (*_details.xlsx) found in '{detailed_reports_folder}'. Cannot generate master summary.")
        return

    print(f"Found {len(detail_files)} detailed project reports to collate.")

    summary_list = []
    for detail_file_path in tqdm(detail_files, desc="Collating summaries"):
        try:
            project_summary_df = pd.read_excel(detail_file_path, sheet_name="Project_Summary_Data")
            if not project_summary_df.empty:
                project_entry = project_summary_df.iloc[0].to_dict()
                master_summary_entry = {}
                for key, value in project_entry.items():
                    if isinstance(key, str) and key.endswith("_Project"):
                        master_summary_entry[key.replace("_Project", "_Summary")] = value
                    else:
                        master_summary_entry[key] = value
                master_summary_entry["PathToDetailedReport"] = str(detail_file_path.resolve())
                summary_list.append(master_summary_entry)
            else:
                # print(f"Warning: 'Project_Summary_Data' sheet in {detail_file_path} is empty. Skipping.")
                pass # Less verbose
        except Exception as e:
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
    summary_column_order = [
        "ProjectName", "ProcessingStatus", "ErrorMessage", "DateAnalyzed",
        "TotalBooksProcessed", "LanguageCode", 
        "DetectedScript", "ScriptDirection", "HasCustomSty",
        "TotalUniqueSFMMarkers_Summary", "TotalSFMMarkerInstances_Summary", "TopNCommonSFMMarkers_Summary",
        "TotalUniquePunctuationChars_Summary", "TotalPunctuationInstances_Summary", "TopNCommonPunctuation_Summary",
        f"{num_extreme_words}_ShortestWords_Summary", f"{num_extreme_words}_LongestWords_Summary",
        "PathToDetailedReport", "ProjectFolderPath"
    ]
    for col in summary_column_order:
        if col not in summary_df.columns:
            summary_df[col] = "" 

    summary_df = summary_df[summary_column_order]

    main_output_folder = Path(main_output_folder_str)
    summary_xlsx_path = main_output_folder / "project_analysis_summary.xlsx"
    summary_csv_path = main_output_folder / "project_analysis_summary.csv"

    try:
        summary_df.to_excel(summary_xlsx_path, index=False, engine='openpyxl')
        print(f"Master summary report generated: {summary_xlsx_path}")
        summary_df.to_csv(summary_csv_path, index=False)
        print(f"Master summary CSV generated: {summary_csv_path}")
    except Exception as e:
        print(f"Error generating master summary report: {e}")

# --- Multiprocessing Worker ---
def process_project_task(project_path_str, n_words_arg, book_filter_arg, details_output_folder_str, force_arg):
    project_name = Path(project_path_str).name
    try:
        # print(f"Worker {os.getpid()} starting on {project_name}") # Debug
        current_project_data = analyze_project_data(project_path_str, n_words_arg, book_filter_arg)
        
        # Check status from analysis before attempting report generation
        if current_project_data.get("ProcessingStatus") == "Error" and not force_arg:
             # If analysis itself errored, and we are not forcing, return that error.
             return {"project_name": project_name, "status": "analysis_error_not_forced", "error_message": current_project_data.get("ErrorMessage", "Analysis failed"), "processing_status_detail": current_project_data.get("ProcessingStatus")}

        # If analysis was "Success" or "Warning", or if "Error" but force_arg is True, proceed to report generation.
        # generate_detailed_project_report will raise an exception on failure, which is caught below.
        report_generated = generate_detailed_project_report(current_project_data, details_output_folder_str, n_words_arg)
        
        # If generate_detailed_project_report succeeded (returned True, or didn't raise exception that was re-raised)
        return {"project_name": project_name, "status": "success", "processing_status_detail": current_project_data.get("ProcessingStatus"), "error_message": current_project_data.get("ErrorMessage","")}

    except Exception as e:
        # This catches errors from analyze_project_data if it raises, or from generate_detailed_project_report
        # print(f"Worker {os.getpid()} exception on {project_name}: {e}") # Debug
        return {"project_name": project_name, "status": "worker_exception", "error_message": str(e), "traceback": traceback.format_exc()}

def worker_wrapper(args_tuple):
    return process_project_task(*args_tuple)

# --- Main Execution (Multiprocessing) ---
def main_mp():
    load_dotenv()

    default_projects_folder = os.getenv("PROJECTS_FOLDER")
    default_output_folder = os.getenv("OUTPUT_FOLDER")
    default_details_output_folder = os.getenv("DETAILS_OUTPUT_FOLDER")
    process_n_projects_env = os.getenv("PROCESS_N_PROJECTS")
    book_filter_env = os.getenv("BOOK_FILTER")
    num_workers_env = os.getenv("NUM_WORKERS")

    parser = argparse.ArgumentParser(description="Analyze Paratext project folders in parallel using sil-machine.")
    parser.add_argument("projects_folder", nargs="?", default=default_projects_folder, help="Path to Paratext projects folder.")
    parser.add_argument("--output_folder", default=default_output_folder, help="Path for main reports.")
    parser.add_argument("--details_output_folder", default=default_details_output_folder, help="Separate path for detailed reports.")
    parser.add_argument("--force", action="store_true", help="Force reprocessing.")
    parser.add_argument("--n_words", type=int, default=N_WORDS, help=f"Number of shortest/longest words (default: {N_WORDS}).")
    parser.add_argument("--exclude_sfm_summary", type=str, default="", help="SFM markers to exclude from summary (comma-separated).")
    parser.add_argument("--process_n_projects", type=int, help="Limit projects to process.")
    parser.add_argument("--book_filter", type=str, help="Books to process (e.g., GEN,PSA,MAT).")
    parser.add_argument("--num_workers", type=int, help="Number of worker processes (default: CPU count).")

    args = parser.parse_args()

    if not args.projects_folder:
        print("Error: Projects folder not specified.")
        return
    if not args.output_folder:
        print("Error: Output folder not specified.")
        return

    main_output_folder_path = Path(args.output_folder)
    main_output_folder_path.mkdir(parents=True, exist_ok=True)
    
    details_output_folder_path = main_output_folder_path
    if args.details_output_folder:
        details_output_folder_path = Path(args.details_output_folder)
        details_output_folder_path.mkdir(parents=True, exist_ok=True)
        print(f"Detailed reports will be saved in: {details_output_folder_path}")
    
    sfm_exclusion_list_for_summary = [marker.strip() for marker in args.exclude_sfm_summary.split(',') if marker.strip()]

    limit_n_projects = None
    if args.process_n_projects is not None: limit_n_projects = args.process_n_projects
    elif process_n_projects_env:
        try: limit_n_projects = int(process_n_projects_env)
        except ValueError: print(f"Warning: Invalid PROCESS_N_PROJECTS in .env: '{process_n_projects_env}'.")
    if limit_n_projects is not None and limit_n_projects <= 0: limit_n_projects = None

    active_book_filter = None
    book_filter_source = args.book_filter if args.book_filter else book_filter_env
    if book_filter_source:
        active_book_filter = {book_id.strip().upper() for book_id in book_filter_source.split(',') if book_id.strip()}

    num_workers = os.cpu_count()
    if args.num_workers is not None: num_workers = args.num_workers
    elif num_workers_env:
        try: num_workers = int(num_workers_env)
        except ValueError: print(f"Warning: Invalid NUM_WORKERS in .env: '{num_workers_env}'. Using CPU count.")
    if num_workers <= 0: num_workers = 1
    print(f"Using {num_workers} worker processes.")

    print(f"Scanning for projects in: {args.projects_folder}")
    # Initial scan limit can be different from final processing limit.
    # For simplicity, pass the same limit_n_projects for scan, or None if no limit.
    all_project_paths = get_project_paths(args.projects_folder, limit_n_projects, active_book_filter)

    if not all_project_paths:
        print("No Paratext projects found or none met initial scan criteria.")
        return

    actual_projects_for_pool = []
    projects_skipped_existing = 0
    for proj_path in all_project_paths:
        if limit_n_projects is not None and len(actual_projects_for_pool) >= limit_n_projects:
            print(f"Reached processing limit of {limit_n_projects} projects before submitting to pool.")
            break
        
        project_name = proj_path.name
        detailed_report_path = details_output_folder_path / f"{project_name}_details.xlsx"

        if not args.force and detailed_report_path.exists():
            # print(f"Detailed report for {project_name} already exists. Skipping analysis (use --force to override).")
            projects_skipped_existing +=1
            continue
        actual_projects_for_pool.append(proj_path)
    
    if projects_skipped_existing > 0:
        print(f"{projects_skipped_existing} project(s) skipped as detailed reports already exist (use --force to override).")

    if not actual_projects_for_pool:
        print("No projects to process after filtering for existing reports and limits.")
        collate_master_summary_report(str(main_output_folder_path), str(details_output_folder_path) if args.details_output_folder else None, args.n_words, sfm_exclusion_list_for_summary)
        return

    print(f"Preparing to process {len(actual_projects_for_pool)} project(s) in parallel.")

    task_args_list = [
        (str(proj_path), args.n_words, active_book_filter, str(details_output_folder_path), args.force)
        for proj_path in actual_projects_for_pool
    ]

    # Adjust num_workers if there are fewer tasks than potential workers
    effective_num_workers = min(num_workers, len(task_args_list))
    if effective_num_workers == 0 and len(task_args_list) > 0 : effective_num_workers = 1 # Ensure at least one worker if tasks exist
    elif len(task_args_list) == 0: effective_num_workers = 0 # No tasks, no workers

    projects_processed_successfully_count = 0
    projects_with_errors_count = 0

    if task_args_list and effective_num_workers > 0:
        with multiprocessing.Pool(processes=effective_num_workers) as pool:
            # Using tqdm with imap_unordered
            # Wrap pool.imap_unordered with tqdm for a progress bar
            results_iterator = pool.imap_unordered(worker_wrapper, task_args_list)
            
            for result in tqdm(results_iterator, total=len(task_args_list), desc="Analyzing projects"):
                if result.get("status") == "success":
                    projects_processed_successfully_count += 1
                    # Optional: print success message or status detail
                    # print(f"Project {result.get('project_name')} processed with status: {result.get('processing_status_detail')}. Message: {result.get('error_message')}")
                else:
                    projects_with_errors_count += 1
                    print(f"Error processing project {result.get('project_name', 'Unknown')}:")
                    print(f"  Status: {result.get('status')}")
                    print(f"  Message: {result.get('error_message')}")
                    if "traceback" in result: # For worker_exception
                        print(f"  Traceback: \n{result.get('traceback')}")
    
    collate_master_summary_report(str(main_output_folder_path), str(details_output_folder_path) if args.details_output_folder else None, args.n_words, sfm_exclusion_list_for_summary)

    print(f"\n--- Processing Summary ---")
    print(f"Total projects submitted for parallel processing: {len(task_args_list)}")
    print(f"Successfully processed and reports generated: {projects_processed_successfully_count}")
    print(f"Projects with errors during processing: {projects_with_errors_count}")
    if projects_skipped_existing > 0:
         print(f"Projects skipped (report existed, no --force): {projects_skipped_existing}")

if __name__ == "__main__":
    # This guard is crucial for multiprocessing on Windows and good practice elsewhere.
    multiprocessing.freeze_support() # For PyInstaller/cx_Freeze if used
    main_mp()