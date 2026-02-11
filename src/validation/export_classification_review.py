"""
Export recall classification data to Excel for manual review.
Shows RecallID, 3-level classification, and source text used for classification.
"""
import pandas as pd
import re
from pathlib import Path


def clean_text_for_excel(text):
    """Remove illegal characters that Excel/openpyxl cannot handle."""
    if pd.isna(text):
        return text
    # Remove control characters (except tab, newline, carriage return)
    text = str(text)
    # Remove characters that openpyxl considers illegal (ASCII 0-8, 11-12, 14-31)
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', text)
    return text

# Paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
PARQUET_FILE = PROJECT_ROOT / "data" / "output" / "parquet" / "fact_recalls.parquet"
OUTPUT_FILE = PROJECT_ROOT / "data" / "output" / "classification" / "classification_review_v2.xlsx"

def export_classification_review():
    print("Loading fact_recalls.parquet...")
    df = pd.read_parquet(PARQUET_FILE)

    print(f"Total records: {len(df):,}")

    # Select relevant columns for review
    columns = [
        'RecallID',
        'Source',
        'RecallCategory',
        'RecallGroup',
        'RecallSubgroup',
        'ReasonForRecall'  # Source text used for classification
    ]

    review_df = df[columns].copy()

    # Clean text columns for Excel compatibility
    review_df['ReasonForRecall'] = review_df['ReasonForRecall'].apply(clean_text_for_excel)
    review_df['RecallID'] = review_df['RecallID'].apply(clean_text_for_excel)

    # Sort by classification to group similar items
    review_df = review_df.sort_values(['RecallCategory', 'RecallGroup', 'RecallSubgroup', 'Source'])

    # Statistics
    print("\n" + "=" * 50)
    print("CLASSIFICATION STATISTICS")
    print("=" * 50)

    # Level 1
    print("\nLevel 1 - RecallCategory:")
    for cat, count in review_df['RecallCategory'].value_counts().items():
        pct = 100 * count / len(review_df)
        print(f"  {cat}: {count:,} ({pct:.1f}%)")

    # Level 2
    print("\nLevel 2 - RecallGroup (Top 15):")
    for group, count in review_df['RecallGroup'].value_counts().head(15).items():
        pct = 100 * count / len(review_df)
        print(f"  {group}: {count:,} ({pct:.1f}%)")

    # Unknown records
    unknown_df = review_df[review_df['RecallCategory'] == 'Unknown']
    print(f"\nUnknown records: {len(unknown_df):,}")

    # Create output directory if needed
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    # Export to Excel with multiple sheets
    print(f"\nExporting to {OUTPUT_FILE}...")

    with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
        # Sheet 1: All records
        review_df.to_excel(writer, sheet_name='All Records', index=False)

        # Sheet 2: Unknown only (for review)
        unknown_df.to_excel(writer, sheet_name='Unknown Only', index=False)

        # Sheet 3: Summary statistics
        summary_data = []

        # Category counts
        for cat, count in review_df['RecallCategory'].value_counts().items():
            summary_data.append({
                'Level': 'Category',
                'Value': cat,
                'Count': count,
                'Percentage': f"{100*count/len(review_df):.1f}%"
            })

        # Group counts
        for group, count in review_df['RecallGroup'].value_counts().items():
            summary_data.append({
                'Level': 'Group',
                'Value': group,
                'Count': count,
                'Percentage': f"{100*count/len(review_df):.1f}%"
            })

        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name='Summary', index=False)

        # Sheet 4: By Source
        source_cat = review_df.groupby(['Source', 'RecallCategory']).size().unstack(fill_value=0)
        source_cat.to_excel(writer, sheet_name='By Source')

    print(f"\nDone! Excel file saved to:\n{OUTPUT_FILE}")
    print("\nSheets created:")
    print("  - All Records: Complete list sorted by classification")
    print("  - Unknown Only: Records with Unknown classification for review")
    print("  - Summary: Statistics by Category and Group")
    print("  - By Source: Cross-tabulation of Source vs Category")

if __name__ == "__main__":
    export_classification_review()
