import sys
import hashlib
import csv
from pathlib import Path

# Add the parent directory (project_analyser) to sys.path for module import
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from project_analyser_mp import UsfmStylesheet, count_usfm_content

def sha256_file(filepath):
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            h.update(chunk)
    return h.hexdigest()

def count_sfm_file(sfm_path, stylesheet_file):
    with open(sfm_path, "r", encoding="utf-8-sig") as f:
        content = f.read()
    stylesheet = UsfmStylesheet(str(stylesheet_file))
    marker_counts, punctuation_counts, word_counts = count_usfm_content(content, stylesheet)
    return marker_counts, punctuation_counts, word_counts

def write_counts_csv(csv_path, marker_counts, punctuation_counts, word_counts, verified=False):
    with open(csv_path, "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["type", "item", "count", "verified"])
        for k, v in marker_counts.items():
            writer.writerow(["sfm_marker", k, v, "TRUE" if verified else "FALSE"])
        for k, v in punctuation_counts.items():
            writer.writerow(["punctuation", k, v, "TRUE" if verified else "FALSE"])
        for k, v in word_counts.items():
            writer.writerow(["word", k, v, "TRUE" if verified else "FALSE"])

def is_verified_csv(csv_path):
    with open(csv_path, newline='', encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("verified", "").strip().upper() != "TRUE":
                return False
    return True

def compare_counts_csv(csv_path, marker_counts, punctuation_counts, word_counts):
    """Compare the generated counts with the verified CSV. Returns a list of mismatches."""
    mismatches = []
    # Build dicts for comparison
    expected = {"sfm_marker": {}, "punctuation": {}, "word": {}}
    with open(csv_path, newline='', encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            typ = row["type"]
            item = row["item"]
            count = int(row["count"])
            expected[typ][item] = count
    # Compare each type
    for typ, actual in [("sfm_marker", marker_counts), ("punctuation", punctuation_counts), ("word", word_counts)]:
        # Check for missing or mismatched counts
        for k, v in actual.items():
            if k not in expected[typ]:
                mismatches.append(f"{typ}:{k} missing in verified counts (actual={v})")
            elif expected[typ][k] != v:
                mismatches.append(f"{typ}:{k} count mismatch: actual={v}, verified={expected[typ][k]}")
        # Check for extra items in verified
        for k in expected[typ]:
            if k not in actual:
                mismatches.append(f"{typ}:{k} missing in actual counts (verified={expected[typ][k]})")
    return mismatches

def compare_counts_dicts(label, a, b):
    mismatches = []
    for k, v in a.items():
        if k not in b:
            mismatches.append(f"{label}:{k} missing in B (A={v})")
        elif b[k] != v:
            mismatches.append(f"{label}:{k} mismatch: A={v}, B={b[k]}")
    for k, v in b.items():
        if k not in a:
            mismatches.append(f"{label}:{k} missing in A (B={v})")
    return mismatches

def main():
    test_dir = Path(__file__).parent / "data"
    stylesheet_file = Path("path/to/usfm.sty")  # Update this path as needed

    summary = []
    for sfm_file in test_dir.glob("*.sfm"):
        base = sfm_file.stem
        counts_file = test_dir / f"{base}_counts.csv"
        counts_hash_file = test_dir / f"{base}_counts.csv.sha256"
        sfm_hash_file = test_dir / f"{base}.sfm.sha256"

        # Always calculate and save SFM file hash if missing
        sfm_hash = sha256_file(sfm_file)
        if not sfm_hash_file.exists():
            sfm_hash_file.write_text(sfm_hash)
            print(f"Saved SFM hash for {sfm_file.name} -> {sfm_hash_file.name}")
        else:
            saved_sfm_hash = sfm_hash_file.read_text().strip()
            if sfm_hash != saved_sfm_hash:
                print(f"WARNING: {sfm_file.name} has changed since last verification!")
                summary.append((sfm_file.name, "sfm_changed"))
            else:
                print(f"{sfm_file.name}: SFM file unchanged.")

        # 1. Generate counts CSV if missing
        if not counts_file.exists():
            print(f"Generating counts for {sfm_file.name}")
            marker_counts, punctuation_counts, word_counts = count_sfm_file(sfm_file, stylesheet_file)
            write_counts_csv(counts_file, marker_counts, punctuation_counts, word_counts, verified=False)
            print(f"Please manually verify and edit {counts_file} as needed, then set 'verified' to TRUE before hashing.")
            summary.append((sfm_file.name, "counts_generated"))
            continue  # Wait for manual verification before hashing

        # 2. Generate hash if missing and CSV is verified
        if counts_file.exists() and not counts_hash_file.exists():
            if is_verified_csv(counts_file):
                print(f"Hashing verified counts for {counts_file.name}")
                hash_val = sha256_file(counts_file)
                counts_hash_file.write_text(hash_val)
                print(f"Hash saved to {counts_hash_file.name}")
            else:
                print(f"{counts_file.name} is not marked as verified. Please set all 'verified' fields to TRUE.")
            continue

        # 3. If both counts and hash exist, treat as golden and verify
        if counts_file.exists() and counts_hash_file.exists():
            counts_hash = sha256_file(counts_file)
            golden_hash = counts_hash_file.read_text().strip()
            if counts_hash == golden_hash:
                print(f"{counts_file.name}: counts CSV hash matches golden reference.")
            else:
                print(f"WARNING: {counts_file.name} counts CSV hash does NOT match golden reference!")
                summary.append((sfm_file.name, "counts_changed"))

            # Also compare actual counts to verified CSV for feedback
            marker_counts, punctuation_counts, word_counts = count_sfm_file(sfm_file, stylesheet_file)
            mismatches = compare_counts_csv(counts_file, marker_counts, punctuation_counts, word_counts)
            if not mismatches:
                print(f"{sfm_file.name}: All counts match verified counts.")
                summary.append((sfm_file.name, "all_match"))
            else:
                print(f"{sfm_file.name}: MISMATCHES FOUND:")
                for m in mismatches:
                    print("  " + m)
                summary.append((sfm_file.name, "counts_mismatch"))

        # After all previous checks, compare project_analyser_mp.py output to golden counts
        if counts_file.exists() and is_verified_csv(counts_file):
            # Get golden counts from CSV
            golden_marker, golden_punct, golden_word = {}, {}, {}
            with open(counts_file, newline='', encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    typ = row["type"]
                    item = row["item"]
                    count = int(row["count"])
                    if typ == "sfm_marker":
                        golden_marker[item] = count
                    elif typ == "punctuation":
                        golden_punct[item] = count
                    elif typ == "word":
                        golden_word[item] = count

            # Get counts from project_analyser_mp.py (same as count_sfm_file now)
            pa_marker, pa_punct, pa_word = count_sfm_file(sfm_file, stylesheet_file)

            mismatches = []
            mismatches += compare_counts_dicts("sfm_marker", golden_marker, pa_marker)
            mismatches += compare_counts_dicts("punctuation", golden_punct, pa_punct)
            mismatches += compare_counts_dicts("word", golden_word, pa_word)

            if not mismatches:
                print(f"{sfm_file.name}: project_analyser_mp.py output matches golden counts.")
                summary.append((sfm_file.name, "project_analyser_mp_match"))
            else:
                print(f"{sfm_file.name}: project_analyser_mp.py output MISMATCHES golden counts:")
                for m in mismatches:
                    print("  " + m)
                summary.append((sfm_file.name, "project_analyser_mp_mismatch"))

    # Print summary
    print("\n=== Test Summary ===")
    for fname, status in summary:
        print(f"{fname}: {status}")

if __name__ == "__main__":
    main()