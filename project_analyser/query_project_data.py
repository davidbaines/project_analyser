#!/usr/bin/env python3

import argparse
import os
import sys
from datetime import datetime
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm
import unicodedata
import regex # For Unicode properties. Needs: pip install regex

# Constants for accessing verse count data from detailed project reports
BOOK_STATS_SHEET_NAME = "Book_Stats" # Assumed sheet name in _details.xlsx
BOOK_STATS_ID_COL = "BookCode"       # Assumed book identifier column in Book_Stats
BOOK_STATS_VERSE_COUNT_COL = "VerseCount" # Assumed verse count column in Book_Stats


def is_quotation_mark(name_or_char_str: str) -> bool:
    """
    Determines if the given string (either a character or its Unicode name)
    represents a quotation mark.
    Relies on Unicode properties and a list of known names/keywords.
    """
    if not isinstance(name_or_char_str, str):
        return False

    char_obj = None
    if len(name_or_char_str) == 1: # If the 'name' is actually the character itself
        char_obj = name_or_char_str
    else:
        try:
            char_obj = unicodedata.lookup(name_or_char_str) # Try to look up by official Unicode name
        except KeyError:
            pass # Character not found by name, will proceed to name-based string checks

    if char_obj:
        # 1. Check Unicode Quotation_Mark binary property
        if regex.fullmatch(r"\p{Quotation_Mark}", char_obj):
            return True
        # 2. Check General Categories: Pi, Pf (initial/final quotes)
        #    Ps, Pe (open/close punctuation, often used for quotes like CJK brackets)
        category = unicodedata.category(char_obj)
        if category in ('Pi', 'Pf', 'Ps', 'Pe'):
            return True
        # 3. Specific character checks for common quote-like chars not caught above
        if char_obj in ("'", "`") and unicodedata.category(char_obj) in ('Po', 'Sk'):
            return True

    # 4. Fallback/Augmentation: Check the original name string for keywords and known exact names.
    # This is crucial if char_obj couldn't be determined, or for names that are
    # descriptive of quotes but the char itself has a general category (e.g., Po).
    name_upper = name_or_char_str.upper()
    
    quote_keywords = [
        "QUOTATION", "QUOTE", "APOSTROPHE", "GUILLEMET", "SPEECH MARK",
        "TURNED COMMA", "CORNER BRACKET", "ANGLE BRACKET" 
    ]
    if any(keyword in name_upper for keyword in quote_keywords):
        return True

    known_quote_names_exact = [
        "GRAVE ACCENT", "ACUTE ACCENT", "MODIFIER LETTER APOSTROPHE",
        "MODIFIER LETTER TURNED COMMA", "MODIFIER LETTER REVERSED COMMA",
        "LEFT SINGLE QUOTATION MARK", "RIGHT SINGLE QUOTATION MARK",
        "LEFT DOUBLE QUOTATION MARK", "RIGHT DOUBLE QUOTATION MARK",
        "SINGLE HIGH-REVERSED-9 QUOTATION MARK", "DOUBLE HIGH-REVERSED-9 QUOTATION MARK",
        "LEFT-POINTING DOUBLE ANGLE QUOTATION MARK", "RIGHT-POINTING DOUBLE ANGLE QUOTATION MARK",
        "SINGLE LEFT-POINTING ANGLE QUOTATION MARK", "SINGLE RIGHT-POINTING ANGLE QUOTATION MARK",
        "LEFT CORNER BRACKET", "RIGHT CORNER BRACKET",
        "LEFT WHITE CORNER BRACKET", "RIGHT WHITE CORNER BRACKET",
        "PRESENTATION FORM FOR VERTICAL LEFT WHITE CORNER BRACKET",
        "PRESENTATION FORM FOR VERTICAL RIGHT WHITE CORNER BRACKET",
    ]
    if name_upper in (name.upper() for name in known_quote_names_exact):
        return True
        
    return False

def query_marker_usage(summary_df, target_markers, top_n_projects=5, top_n_books_per_project=3):
    """
    Finds projects and books with the highest usage of specified SFM markers.
    Returns two lists of dictionaries: one for top projects, one for top books.
    """

    project_marker_totals = Counter()
    project_book_marker_details = defaultdict(lambda: defaultdict(Counter)) # {proj: {book: {marker: count}}}

    # Data collection lists
    top_projects_data = []
    top_books_data = []
    for index, row in tqdm(summary_df.iterrows(), total=summary_df.shape[0], desc="Processing projects for marker query"):
        project_name = row["ProjectName"]
        detailed_report_path_str = row.get("PathToDetailedReport", "")
        if not detailed_report_path_str:
            print(f"Warning: No detailed report path for project {project_name}. Skipping.")
            continue
        
        detailed_report_path = Path(detailed_report_path_str)

        if not detailed_report_path.exists():
            print(f"Warning: Detailed report for {project_name} not found at {detailed_report_path}. Skipping.")
            continue

        try:
            # The SFM_Markers_By_Book sheet is pivoted: SFMMarker is index, BookIDs are columns
            sfm_df = pd.read_excel(detailed_report_path, sheet_name="SFM_Markers_By_Book", index_col="SFMMarker")
            if sfm_df.empty:
                continue

            for marker in target_markers:
                if marker in sfm_df.index:
                    marker_row = sfm_df.loc[marker]
                    project_total_for_marker = marker_row.sum()
                    project_marker_totals[project_name] += project_total_for_marker
                    
                    for book_id, count in marker_row.items(): # book_id can be non-string from column header
                        if count > 0:
                            project_book_marker_details[project_name][str(book_id)][marker] += count # Ensure book_id is string
        except Exception as e:
            print(f"Warning: Could not process SFM markers for {project_name} from {detailed_report_path}: {e}")
            continue
            
    if not project_marker_totals:
        # print("No projects found with the specified markers.") # Handled in main
        return [], []

    # print("\nTop Projects by Total Target Marker Count:") # Replaced by DataFrame output
    for project, total_count in project_marker_totals.most_common(top_n_projects):
        # print(f"  {project}: {total_count} occurrences") # Replaced by DataFrame output
        top_projects_data.append({"Project": project, "Total Target Marker Occurrences": total_count})
        
        if project in project_book_marker_details:
            # print(f"    Top books in {project}:") # Replaced by DataFrame output
            
            book_totals_in_project = Counter()
            for book_id, marker_counts in project_book_marker_details[project].items():
                for marker in target_markers: # Ensure we only sum target markers
                    book_totals_in_project[book_id] += marker_counts.get(marker, 0)

            for book, book_count in book_totals_in_project.most_common(top_n_books_per_project):
                current_book_entry = {
                    "Project": project,
                    "Book": book, # Already string due to str(book_id) above
                    "Total": book_count
                }
                for tm in target_markers:
                    sanitized_marker_col_name = tm.replace("\\", "")
                    actual_count = project_book_marker_details[project][book].get(tm, 0)
                    current_book_entry[sanitized_marker_col_name] = actual_count
                top_books_data.append(current_book_entry)
    
    return top_projects_data, top_books_data


def query_quotation_punctuation(summary_df, top_n_projects=5, top_n_books_per_project=3):
    """
    Finds projects and books with the highest usage of punctuation containing "QUOTATION" in its name.
    Returns two lists of dictionaries: one for top projects, one for top books.
    """
    top_projects_data = []
    top_books_data = []

    # print("\n--- Query: Top usage of 'QUOTATION' punctuation ---") # Moved to main

    project_quot_punct_totals = Counter()
    project_scripts = {}  # To store DetectedScript for relevant projects
    project_book_quot_punct_details = defaultdict(lambda: defaultdict(Counter)) # {proj: {book: {punct_name: count}}}
    project_book_verse_counts = defaultdict(lambda: defaultdict(int)) # {proj: {book: verse_count}}

    # --- Step 1: Gather all quotation and verse count data per project/book ---

    for index, row in tqdm(summary_df.iterrows(), total=summary_df.shape[0], desc="Processing projects for punctuation query"):
        project_name = row["ProjectName"]
        detailed_report_path_str = row.get("PathToDetailedReport", "")
        if not detailed_report_path_str:
            continue
        
        detailed_report_path = Path(detailed_report_path_str)
        if not detailed_report_path.exists():
            continue

        # Attempt to load verse counts for this project
        try:
            book_stats_df = pd.read_excel(detailed_report_path, sheet_name=BOOK_STATS_SHEET_NAME)
            if not book_stats_df.empty and BOOK_STATS_ID_COL in book_stats_df and BOOK_STATS_VERSE_COUNT_COL in book_stats_df:
                for _, stat_row in book_stats_df.iterrows():
                    book_code = str(stat_row[BOOK_STATS_ID_COL]) # Ensure book_code is string
                    verse_count = stat_row[BOOK_STATS_VERSE_COUNT_COL]
                    if pd.notna(verse_count) and verse_count > 0 : # Ensure verse_count is valid
                        project_book_verse_counts[project_name][book_code] = int(verse_count)
        except Exception as e:
            print(f"Warning: Could not load or process '{BOOK_STATS_SHEET_NAME}' for {project_name} from {detailed_report_path}: {e}")

        try:
            # Punctuation_By_Book sheet is pivoted: UnicodeName is index, BookIDs are columns
            punct_df = pd.read_excel(detailed_report_path, sheet_name="Punctuation_By_Book", index_col="UnicodeName")
            if punct_df.empty:
                continue
            
            quotation_punctuation_names = [
                name for name in punct_df.index if is_quotation_mark(name)
            ]

            if not quotation_punctuation_names:
                continue

            # If we've reached here, the project *might* have relevant quotation marks.
            # Get the script name for this project.
            current_project_script = row.get("DetectedScript", "Unknown")
            project_contributed_quotes = False

            for punct_name in quotation_punctuation_names:
                punct_row = punct_df.loc[punct_name]
                project_total_for_punct = punct_row.sum()
                
                if project_total_for_punct > 0:
                    project_quot_punct_totals[project_name] += project_total_for_punct
                    project_contributed_quotes = True # Mark that this project contributed

                    for book_id, count in punct_row.items(): # book_id can be non-string
                        if count > 0:
                            project_book_quot_punct_details[project_name][str(book_id)][punct_name] += count # Ensure book_id is string
            
            if project_contributed_quotes: # If any quote type contributed to totals for this project
                project_scripts[project_name] = current_project_script
        except Exception as e:
            print(f"Warning: Could not process punctuation for {project_name} from {detailed_report_path}: {e}")
            continue

    if not project_quot_punct_totals:
        # print("No projects found with 'QUOTATION' punctuation.") # Handled in main
        return [], []
    # print("\nTop Projects by Total 'QUOTATION' Punctuation Count:") # Replaced by DataFrame output
    
    # --- Step 2: Process gathered data to find top projects and top books within them ---
    for project, total_count in project_quot_punct_totals.most_common(top_n_projects):
        top_projects_data.append({
            "Project": project,
            "DetectedScript": project_scripts.get(project, "Unknown"),
            "Total 'QUOTATION' Punctuation Occurrences": total_count
        })
        
        if project in project_book_quot_punct_details: # If this project has book-level details
            project_all_books_data_list = [] # Stores (book_id, total_quotes, verse_count, density, individual_punct_counts)

            for book_id_str, individual_punct_counts in project_book_quot_punct_details[project].items():
                total_book_quotes = sum(individual_punct_counts.values())
                # Get verse count, default to 0 if not found for the book_id_str
                verse_count = project_book_verse_counts[project].get(book_id_str, 0)

                density = 0.0
                if verse_count > 0:
                    density = total_book_quotes / verse_count
                elif total_book_quotes > 0: # verse_count is 0 but quotes exist
                    density = float('inf') 
                # else: total_book_quotes is 0 and verse_count is 0 (or not found), density remains 0.0

                project_all_books_data_list.append(
                    (book_id_str, total_book_quotes, verse_count, density, individual_punct_counts)
                )

            # Select top N by count
            top_by_count_books = sorted(project_all_books_data_list, key=lambda x: x[1], reverse=True)[:top_n_books_per_project]
            
            # Select top N by density
            top_by_density_books = sorted(project_all_books_data_list, key=lambda x: x[3], reverse=True)[:top_n_books_per_project]

            # Combine and get unique books (book_id is at index 0 of the tuple)
            # We need to store the full tuple to reconstruct, so we use a dict keyed by book_id
            combined_books_dict = {}
            for book_data_tuple in top_by_count_books + top_by_density_books:
                book_id_for_dict = book_data_tuple[0] # book_id_str
                if book_id_for_dict not in combined_books_dict:
                     combined_books_dict[book_id_for_dict] = book_data_tuple

            # Now create the output entries for these selected books
            for book_id_key, book_data_tuple_val in combined_books_dict.items():
                _book_id, _total_quotes, _verse_count, _density, _punct_details = book_data_tuple_val
                
                current_book_entry = {
                    "Project": project,
                    "DetectedScript": project_scripts.get(project, "Unknown"),
                    "Book": _book_id, # Already string
                    "No_verses": _verse_count,
                    "QM_ratio_by_book": _density if _density != float('inf') else "Inf", # More Excel-friendly
                    "Total Book 'QUOTATION' Punctuation Occurrences": _total_quotes
                }
                # Add individual punctuation counts for this book
                for punct_name_detail, count_detail in _punct_details.items():
                    if count_detail > 0: 
                         current_book_entry[punct_name_detail + "_Count"] = count_detail
                top_books_data.append(current_book_entry)
    
    return top_projects_data, top_books_data

def main():
    load_dotenv()
    default_output_folder = os.getenv("OUTPUT_FOLDER")
    env_query_markers = os.getenv("QUERY_MARKERS") # Read QUERY_MARKERS from .env

    parser = argparse.ArgumentParser(description="Query analyzed Paratext project data.")
    parser.add_argument(
        "--output_folder", default=default_output_folder,
        help="Path to the folder where the 'project_analysis_summary.xlsx' is located (overrides .env OUTPUT_FOLDER)."
    )
    parser.add_argument(
        "--query_markers", type=str, default=None, # Default to None to distinguish from empty string argument
        help=(
            "Comma-separated list of SFM markers to query for (e.g., p,q,q1 or \\p,\\q,\\q1). "
            "Command-line input takes precedence over any markers specified in the .env file."
        )
    )
    parser.add_argument(
        "--quotes", action="store_true",
        help="Query for projects/books with high usage of 'QUOTATION' punctuation."
    )
    parser.add_argument(
        "--top_n_projects", type=int, default=500,
        help="Number of top projects to display for queries."
    )
    parser.add_argument(
        "--top_n_books", type=int, default=3,
        help="Number of top books per project to display for queries."
    )
    args = parser.parse_args()

    if not args.output_folder:
        print("Error: Output folder not specified via argument or .env file (OUTPUT_FOLDER).")
        sys.exit(1)

    summary_file_path = Path(args.output_folder) / "project_analysis_summary.xlsx"
    if not summary_file_path.exists():
        print(f"Error: Summary file not found at {summary_file_path}")
        sys.exit(1)

    print(f"Loading summary data from {summary_file_path}...")
    try:
        summary_df = pd.read_excel(summary_file_path)
    except Exception as e:
        print(f"Error: Could not load summary file: {e}")
        sys.exit(1)
        
    if summary_df.empty:
        print("Summary file is empty. No data to query.")
        sys.exit(0)

    # Initialize results and marker list
    marker_projects_df, marker_books_df = None, None
    quotes_projects_df, quotes_books_df = None, None
    
    actual_target_markers_list = [] # Will hold the final list of markers if valid ones are found

    # Determine markers to process from CLI or .env
    markers_input_str = None
    markers_source_description = ""

    if args.query_markers is not None: # CLI has highest precedence
        markers_input_str = args.query_markers
        markers_source_description = "from command-line argument"
    elif env_query_markers is not None: # .env is the fallback
        markers_input_str = env_query_markers
        markers_source_description = "from .env file (QUERY_MARKERS)"

    if markers_input_str is not None: # If any source provided a string (even if empty)
        # Split and strip, remove empty strings from the input
        raw_markers_from_input_temp = [m.strip() for m in markers_input_str.split(',') if m.strip()]
        
        # Normalize markers: ensure they start with '\'
        normalized_markers_from_input = []
        for m_raw in raw_markers_from_input_temp:
            if not m_raw.startswith('\\'):
                normalized_markers_from_input.append(f'\\{m_raw}')
            else:
                normalized_markers_from_input.append(m_raw)
 
        
        if normalized_markers_from_input:
            # Ensure uniqueness while preserving the order of first appearance
            unique_target_markers = list(dict.fromkeys(normalized_markers_from_input))
            
            if len(unique_target_markers) < len(normalized_markers_from_input):
                print(f"Note: Duplicate markers were found in the input {markers_source_description}. "
                      f"Using unique list: {', '.join(unique_target_markers)}")
            
            actual_target_markers_list = unique_target_markers # This list will be used for the query
            
            # User-facing message about the source of markers
            source_display = markers_source_description.replace("from ", "")
            print(f"\n--- Query: Top usage of markers ({source_display}): {', '.join(actual_target_markers_list)} ---")
            
            marker_projects_data, marker_books_data = query_marker_usage(
                summary_df, actual_target_markers_list, args.top_n_projects, args.top_n_books
            )
            if marker_projects_data:
                marker_projects_df = pd.DataFrame(marker_projects_data)
            if marker_books_data:
                marker_books_df = pd.DataFrame(marker_books_data)
                marker_count_cols = [
                    col for col in marker_books_df.columns 
                    if col.endswith("_Count") and col not in ["Project", "Book", "Total Book Target Marker Occurrences"]
                ]
                if marker_count_cols:
                     marker_books_df[marker_count_cols] = marker_books_df[marker_count_cols].fillna(0).astype(int)
        else:
            # Markers were specified (CLI or .env) but resulted in an empty list after parsing
            print(f"No valid markers specified {markers_source_description} (e.g., input was empty or just commas).")
            # actual_target_markers_list remains empty, marker query is skipped

    if args.quotes:
        print("\n--- Query: Top usage of 'QUOTATION' punctuation ---")
        quotes_projects_data, quotes_books_data = query_quotation_punctuation(
            summary_df, args.top_n_projects, args.top_n_books
        )
        if quotes_projects_data:
            quotes_projects_df = pd.DataFrame(quotes_projects_data)
            if not quotes_projects_df.empty:
                # Define desired column order
                proj_summary_cols = ["Project", "DetectedScript", "Total 'QUOTATION' Punctuation Occurrences"]
                # Ensure all expected columns are present and in order
                present_cols = [col for col in proj_summary_cols if col in quotes_projects_df.columns]
                quotes_projects_df = quotes_projects_df[present_cols]

        if quotes_books_data:
            quotes_books_df = pd.DataFrame(quotes_books_data)
            if not quotes_books_df.empty:
                # Define desired column order for base columns
                book_detail_base_cols = [
                    "Project", "DetectedScript", "Book", 
                    "No_verses", 
                    "QM_ratio_by_book",
                    "Total Book 'QUOTATION' Punctuation Occurrences"
                ]
                
                # Get all other columns (specific punctuation counts) and sort them for consistency
                other_punct_cols = sorted([
                    col for col in quotes_books_df.columns 
                    if col not in book_detail_base_cols
                ])
                
                final_book_detail_cols = book_detail_base_cols + other_punct_cols
                
                # Ensure all expected columns are present and in order
                present_cols = [col for col in final_book_detail_cols if col in quotes_books_df.columns]
                quotes_books_df = quotes_books_df[present_cols]

                # Ensure all specific punctuation count columns are integer and NaNs are 0
                punct_count_cols = [
                    col for col in other_punct_cols # Only operate on the dynamic _Count columns
                    if col.endswith("_Count") 
                ]
                if punct_count_cols:
                    quotes_books_df[punct_count_cols] = quotes_books_df[punct_count_cols].fillna(0).astype(int)
                if "No_verses" in quotes_books_df.columns:
                    quotes_books_df["No_verses"] = quotes_books_df["No_verses"].fillna(0).astype(int)
                if "QM_ratio_by_book" in quotes_books_df.columns:
                    # Replace "Inf" with actual infinity for potential numeric sort, then back if needed, or handle as object
                    quotes_books_df["QM_ratio_by_book"] = quotes_books_df["QM_ratio_by_book"].replace(float('inf'), "Inf")

    # --- Filename construction and Excel writing ---
    valid_marker_query_defined = bool(actual_target_markers_list)
    quotes_query_attempted = args.quotes

    output_file_parts = []
    if valid_marker_query_defined:
        now = datetime.now()
        timestamp_part = now.strftime("_%Y_%m_%d_%H_%S") # Format: _YYYY_MM_DD_HH_SS
        output_file_parts.append(f"Markers{timestamp_part}")
    
    if quotes_query_attempted:
        output_file_parts.append("Quotes")

    if not valid_marker_query_defined and not quotes_query_attempted:
        print("No query specified. Use --query_markers (or set QUERY_MARKERS in .env) or use --quotes.")
    elif not output_file_parts: # This case implies a query was attempted but resulted in no valid parameters for filename
        print("No valid query parameters to form an output filename.")
    else:
        data_frames_to_write = {}
        if marker_projects_df is not None and not marker_projects_df.empty:
            data_frames_to_write["Marker_Projects_Summary"] = marker_projects_df
        if marker_books_df is not None and not marker_books_df.empty:
            data_frames_to_write["Marker_Books_Details"] = marker_books_df
        if quotes_projects_df is not None and not quotes_projects_df.empty:
            data_frames_to_write["Quotes_Projects_Summary"] = quotes_projects_df
        if quotes_books_df is not None and not quotes_books_df.empty:
            data_frames_to_write["Quotes_Books_Details"] = quotes_books_df

        if not data_frames_to_write:
            print("Queries were run, but no data was found to save to an Excel file.")
            if valid_marker_query_defined and (marker_projects_df is None or marker_projects_df.empty) and (marker_books_df is None or marker_books_df.empty):
                print(f"  - No data found for markers: {', '.join(actual_target_markers_list)}")
            if quotes_query_attempted and (quotes_projects_df is None or quotes_projects_df.empty) and (quotes_books_df is None or quotes_books_df.empty):
                print(f"  - No data found for 'QUOTATION' punctuation.")
        else:
            output_filename_stem = "_".join(output_file_parts) + "_Query_Results.xlsx"
            output_file_path = Path(args.output_folder) / output_filename_stem
            try:
                with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
                    for sheet_name, df_to_write in data_frames_to_write.items():
                        df_to_write.to_excel(writer, sheet_name=sheet_name, index=False)
                
                print(f"\nQuery results saved to {output_file_path}")
                if "Marker_Projects_Summary" in data_frames_to_write or "Marker_Books_Details" in data_frames_to_write :
                    print(f"  Marker data sheets: Marker_Projects_Summary, Marker_Books_Details (if data present)")
                if "Quotes_Projects_Summary" in data_frames_to_write or "Quotes_Books_Details" in data_frames_to_write:
                     print(f"  Quotation data sheets: Quotes_Projects_Summary, Quotes_Books_Details (if data present)")

            except Exception as e:
                print(f"Error writing Excel file to {output_file_path}: {e}")


if __name__ == "__main__":
    main()
