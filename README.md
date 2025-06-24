# Project Analyser

## Overview

This project provides tools for analyzing Paratext USFM/SFM scripture projects, focusing on extracting statistics such as SFM marker usage, punctuation, word forms, and verse counts. The main script, `project_analyser_mp.py`, is designed for efficient, parallel processing of multiple Paratext projects and outputs detailed Excel reports and a master summary.

## Main Script

### `project_analyser_mp.py`

#### Features

- **Parallel Processing:** Analyzes multiple Paratext projects in parallel for speed.
- **Flexible Filtering:** Supports filtering by book codes and limiting the number of projects processed.
- **Detailed Reports:** Generates per-project Excel reports with SFM marker, punctuation, word, and verse statistics.
- **Master Summary:** Collates all project reports into a master summary (Excel and CSV).
- **Script and Language Detection:** Attempts to detect script and language direction from project settings.
- **Robustness:** Handles missing or malformed projects gracefully and logs warnings/errors.

#### Usage

```sh
poetry run python project_analyser/project_analyser_mp.py <projects_folder> [--output_folder <output>] [--details_output_folder <details>] [--force] [--n_words N] [--exclude_sfm_summary MARKERS] [--process_n_projects N] [--book_filter BOOKS] [--num_workers N]
```

- `<projects_folder>`: Path to the folder containing Paratext projects.
- `--output_folder`: Path for main summary reports (default: from `.env` or required).
- `--details_output_folder`: Path for detailed per-project reports (optional).
- `--force`: Force reprocessing even if reports exist.
- `--n_words`: Number of shortest/longest words to report (default: 10).
- `--exclude_sfm_summary`: Comma-separated SFM markers to exclude from summary.
- `--process_n_projects`: Limit the number of projects to process.
- `--book_filter`: Comma-separated list of book codes to include (e.g., `GEN,PSA,MAT`).
- `--num_workers`: Number of parallel worker processes (default: CPU count).

#### Example

```sh
poetry run python project_analyser/project_analyser_mp.py F:/Corpora/test_projects --output_folder F:/Corpora/test_projects/output --details_output_folder F:/Corpora/test_projects/project_details --n_words 10 --book_filter GEN,EXO
```

## Testing

- The `tests/test_sfm_counts.py` script provides regression and equivalence tests for SFM/USFM counting logic, ensuring consistency and correctness of the main script's output.

## Requirements

- Python 3.8+
- [sil-machine](https://github.com/sillsdev/sil-machine)
- pandas, tqdm, dotenv, unicodedataplus, openpyxl

Install dependencies with:

```sh
poetry install
```

## License

See LICENSE file for details.
