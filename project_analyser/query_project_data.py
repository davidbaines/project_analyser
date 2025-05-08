#!/usr/bin/env python3

import argparse
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm


def query_marker_usage(summary_df, target_markers, top_n_projects=5, top_n_books_per_project=3):
    """
    Finds projects and books with the highest usage of specified SFM markers.
    """
    print(f"\n--- Query: Top usage of markers: {', '.join(target_markers)} ---")

    project_marker_totals = Counter()
    project_book_marker_details = defaultdict(lambda: defaultdict(Counter))

    for index, row in tqdm(summary_df.iterrows(), total=summary_df.shape[0], desc="Processing projects for marker query"):
        project_name = row["ProjectName"]
        detailed_report_path_str = row.get("PathToDetailedReport", "")
        if not detailed_report_path_str:
            # print(f"Warning: No detailed report path for project {project_name}. Skipping.")
            continue
        
        detailed_report_path = Path(detailed_report_path_str)

        if not detailed_report_path.exists():
            # print(f"Warning: Detailed report for {project_name} not found at {detailed_report_path}. Skipping.")
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
                    
                    for book_id, count in marker_row.items():
                        if count > 0:
                            project_book_marker_details[project_name][book_id][marker] += count
        except Exception as e:
            print(f"Warning: Could not process SFM markers for {project_name} from {detailed_report_path}: {e}")
            continue
            
    if not project_marker_totals:
        print("No projects found with the specified markers.")
        return

    print("\nTop Projects by Total Target Marker Count:")
    for project, total_count in project_marker_totals.most_common(top_n_projects):
        print(f"  {project}: {total_count} occurrences")
        if project in project_book_marker_details:
            print(f"    Top books in {project}:")
            
            # Sum target markers per book for this project
            book_totals_in_project = Counter()
            for book_id, marker_counts in project_book_marker_details[project].items():
                for marker in target_markers: # Ensure we only sum target markers
                    book_totals_in_project[book_id] += marker_counts.get(marker, 0)

            for book, book_count in book_totals_in_project.most_common(top_n_books_per_project):
                detail_counts = ", ".join(f"{m}: {project_book_marker_details[project][book].get(m,0)}" for m in target_markers if project_book_marker_details[project][book].get(m,0) > 0)
                print(f"      {book}: {book_count} (Details: {detail_counts})")


def query_quotation_punctuation(summary_df, top_n_projects=5, top_n_books_per_project=3):
    """
    Finds projects and books with the highest usage of punctuation containing "QUOTATION" in its name.
    """
    print("\n--- Query: Top usage of 'QUOTATION' punctuation ---")

    project_quot_punct_totals = Counter()
    project_book_quot_punct_details = defaultdict(lambda: defaultdict(Counter)) # {proj: {book: {punct_name: count}}}

    for index, row in tqdm(summary_df.iterrows(), total=summary_df.shape[0], desc="Processing projects for punctuation query"):
        project_name = row["ProjectName"]
        detailed_report_path_str = row.get("PathToDetailedReport", "")
        if not detailed_report_path_str:
            continue
        
        detailed_report_path = Path(detailed_report_path_str)
        if not detailed_report_path.exists():
            continue

        try:
            # Punctuation_By_Book sheet is pivoted: UnicodeName is index, BookIDs are columns
            punct_df = pd.read_excel(detailed_report_path, sheet_name="Punctuation_By_Book", index_col="UnicodeName")
            if punct_df.empty:
                continue
            
            quotation_punctuation_names = [name for name in punct_df.index if isinstance(name, str) and "QUOTATION" in name.upper()]

            if not quotation_punctuation_names:
                continue

            for punct_name in quotation_punctuation_names:
                punct_row = punct_df.loc[punct_name]
                project_total_for_punct = punct_row.sum()
                project_quot_punct_totals[project_name] += project_total_for_punct

                for book_id, count in punct_row.items():
                    if count > 0:
                        project_book_quot_punct_details[project_name][book_id][punct_name] += count
        except Exception as e:
            print(f"Warning: Could not process punctuation for {project_name} from {detailed_report_path}: {e}")
            continue

    if not project_quot_punct_totals:
        print("No projects found with 'QUOTATION' punctuation.")
        return

    print("\nTop Projects by Total 'QUOTATION' Punctuation Count:")
    for project, total_count in project_quot_punct_totals.most_common(top_n_projects):
        print(f"  {project}: {total_count} occurrences")
        if project in project_book_quot_punct_details:
            print(f"    Top books in {project}:")
            
            book_totals_in_project = Counter()
            for book_id, punct_name_counts in project_book_quot_punct_details[project].items():
                 book_totals_in_project[book_id] = sum(punct_name_counts.values())


            for book, book_count in book_totals_in_project.most_common(top_n_books_per_project):
                detail_counts = ", ".join(f"'{name.replace('QUOTATION MARK', 'QM')}': {count}" 
                                          for name, count in project_book_quot_punct_details[project][book].items() if count > 0)
                print(f"      {book}: {book_count} (Details: {detail_counts})")


def main():
    load_dotenv()
    default_output_folder = os.getenv("OUTPUT_FOLDER")

    parser = argparse.ArgumentParser(description="Query analyzed Paratext project data.")
    parser.add_argument(
        "--output_folder", default=default_output_folder,
        help="Path to the folder where the 'project_analysis_summary.xlsx' is located (overrides .env OUTPUT_FOLDER)."
    )
    parser.add_argument(
        "--query_markers", type=str,
        help="Comma-separated list of SFM markers to query for (e.g., \p,\q,\q1)."
    )
    parser.add_argument(
        "--query_quotation_punctuation", action="store_true",
        help="Query for projects/books with high usage of 'QUOTATION' punctuation."
    )
    parser.add_argument(
        "--top_n_projects", type=int, default=5,
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

    if args.query_markers:
        target_markers_list = [m.strip() for m in args.query_markers.split(',') if m.strip()]
        if target_markers_list:
            query_marker_usage(summary_df, target_markers_list, args.top_n_projects, args.top_n_books)
        else:
            print("No markers specified for --query_markers.")

    if args.query_quotation_punctuation:
        query_quotation_punctuation(summary_df, args.top_n_projects, args.top_n_books)

    if not args.query_markers and not args.query_quotation_punctuation:
        print("No query specified. Use --query_markers or --query_quotation_punctuation.")

if __name__ == "__main__":
    main()
