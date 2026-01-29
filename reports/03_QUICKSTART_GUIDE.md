# Quick Start Guide: Using the Refined Ingestion Script

## Purpose

This guide shows you how to use `read_deals_v2_refined.R` to transform your raw SDC/Refinitiv M&A data into a clean, analysis-ready dataset that aligns with your research design requirements.

## Prerequisites

Before running the script, ensure:

1. Your raw deal data file (e.g., `Deals_v1.xlsx`) is in `data/raw/`
2. The schema configuration file exists at `config/data_schema.yaml`
3. All required R packages are installed (the script will alert you if any are missing)

## Running the Script

### Option 1: From RStudio

Open the script in RStudio and click "Source" or press Ctrl+Shift+S (Cmd+Shift+S on Mac). The script will automatically find your data file and process it.

### Option 2: From Command Line

Navigate to your project root directory and run:

```bash
Rscript src/10_ingest/read_deals_v2_refined.R
```

### Option 3: From R Console

```r
source("src/10_ingest/read_deals_v2_refined.R")
```

## What Happens During Processing

The script executes nine distinct steps, each with detailed logging:

**Step 1 (Loading):** Reads the first Excel file found in `data/raw/` and reports initial dimensions.

**Step 2 (Cleaning):** Removes completely empty rows and unnamed columns that are Excel artifacts.

**Step 3 (Deduplication):** Identifies and removes duplicate rows using deal_number as the primary key. You will see exactly how many duplicates were found and removed.

**Step 4 (Column Mapping):** Maps source column names to standardized schema names. This allows the script to work with different SDC exports that might use slightly different column labels.

**Step 5 (Type Conversion):** Converts columns to appropriate data types (dates to Date class, numeric strings to actual numbers, etc.). Warns you if any conversions introduce unexpected NA values.

**Step 6 (Derived Variables):** Creates new variables needed for your research:
- Outcome indicators (deal_completed, time_to_close, deal_outcome_terminal)
- Industry classifications (target_sic_primary, industry_broad)
- Standardized payment method categories (Cash, Stock, Mixed, Other)
- Fixed effects identifiers (year_announced, industry_year)
- Sample filter flags (one for each inclusion criterion from your research design)

**Step 7 (Quality Checks):** Runs validation rules from the schema to identify logical inconsistencies (e.g., completion date before announcement date).

**Step 8 (Saving):** Writes the cleaned dataset and all documentation files.

**Step 9 (Summary):** Generates comprehensive statistics suitable for inclusion in your thesis appendix.

## Expected Processing Time

For a dataset of around 10,000-15,000 deals, expect processing to complete in 30-60 seconds on a typical laptop.

## Output Files

After successful execution, you will find the following files:

### Primary Output
`data/interim/deals_ingested.rds` — Your cleaned deal dataset (R binary format for fast loading)

### Documentation
`data/interim/deals_ingestion_summary.txt` — Human-readable summary with full statistics
`data/interim/deals_ingestion_summary.json` — Machine-readable summary for automation
`data/interim/deals_column_mapping.csv` — Record of how source columns were mapped to standard names
`data/interim/deals_ingestion_log.txt` — Complete processing log with timestamps

### Quality Reports (if issues found)
`data/interim/deals_quality_issues.csv` — List of quality check failures (only created if checks fail)

## Verifying Success

Run the test script to verify everything worked correctly:

```r
source("tests/test_ingestion_refined.R")
```

This will check:
- Output file exists and can be loaded
- All required variables are present
- Outcome variables have valid values
- Sample filter flags have reasonable distributions
- Documentation files were created

You should see a message saying "All tests completed successfully!" at the end.

## Understanding the Output Dataset

The cleaned dataset (`deals_ingested.rds`) contains several types of variables:

**Core identifiers** like deal_id, target_name, and target_ticker allow you to link deals to other data sources.

**Date variables** like announce_date, complete_date, and withdrawn_date are all in R Date format for easy arithmetic and comparison.

**Outcome variables** provide everything you need for your econometric models. The variable deal_completed is binary (1 for completed, 0 for withdrawn, NA for other statuses). The variable time_to_close measures days from announcement to terminal outcome and is suitable for survival analysis.

**Control variables** like payment_method_clean and deal_value_eur_th are ready to use in regressions without additional cleaning.

**Sample filter flags** help you implement the sample restrictions from your research design. Each flag is a logical variable (TRUE/FALSE/NA) indicating whether a deal meets a specific criterion. For example, flag_terminal_outcome is TRUE only for deals with completed or withdrawn status.

**Fixed effect identifiers** like industry_year let you easily add industry-by-year fixed effects to your regression specifications.

## Important: No Rows Are Excluded Yet

The refined script creates filter flags but does not actually drop any rows. This is intentional. It allows you to:

1. See the full distribution of your data before applying restrictions
2. Document exactly what gets excluded at each step of your sample cascade
3. Run robustness checks with alternative filter combinations
4. Include comprehensive sample construction tables in your thesis

Actual row exclusion will happen in a subsequent "sample restriction" script that applies the flags in a documented cascade.

## Common Issues and Solutions

### Issue: "Schema file not found"
**Solution:** Make sure `config/data_schema.yaml` exists. If you haven't created it yet, use the version that came with the original read_deals_v2.R script.

### Issue: "No .xlsx files found in data/raw"
**Solution:** Place your SDC/Refinitiv export file in the `data/raw/` folder. The script automatically finds the first Excel file there.

### Issue: Many date parsing warnings
**Solution:** This is usually harmless. The script tries multiple date formats and warns when some fail. Check the output log to see how many dates were successfully parsed.

### Issue: High duplicate count
**Solution:** This is common with SDC data that includes updated deal records. The script handles this by keeping the first occurrence of each deal_number. Review the deduplication section of the log to verify the rule makes sense for your data.

## Next Steps After Ingestion

Once you have successfully ingested the data, you can:

### 1. Examine the Summary Statistics
Open `data/interim/deals_ingestion_summary.txt` to see comprehensive statistics about your dataset including the time period covered, outcome distributions, and industry breakdowns.

### 2. Apply Sample Restrictions
Develop a sample restriction script that applies the filter flags in sequence and logs exactly what gets excluded at each step. This will create your final analytical sample.

### 3. Match Deals to 10-K Filings
Use the target_ticker field to look up CIK identifiers and then identify the pre-announcement 10-K filing for each deal. This is necessary before you can construct NLP indices.

### 4. Verify Data Quality
Spot check a few deals manually to ensure the cleaning worked as expected. Look at deals across different time periods and industries.

## Customization

If you need to modify the script for your specific SDC export format:

**Column names:** Add alternative column name patterns to `config/data_schema.yaml` under the source_names for each variable.

**Date formats:** Add additional date format strings to the schema's processing.date_formats list if your dates use an unusual format.

**Industry classification:** Modify the industry_broad logic in Step 6B if you want to use a different industry grouping system.

**Payment method categories:** Adjust the regex patterns in payment_method_clean if your data uses different payment terminology.

All customizations should be made either in the schema configuration file or clearly commented in the script so they are documented for reproducibility.

## Getting Help

If you encounter issues:

1. Check the processing log at `data/interim/deals_ingestion_log.txt` for detailed error messages
2. Review the refinements documentation at `reports/02_INGESTION_REFINEMENTS.md` for technical details
3. Run the test script to identify specific problems
4. Verify that your raw data file matches the expected SDC/Refinitiv format

## Summary

The refined ingestion script transforms messy SDC deal data into a clean, well-documented dataset that is ready for the next stages of your research pipeline. By creating filter flags instead of immediately dropping rows, it gives you full control over sample construction while maintaining complete transparency about what data you started with and what criteria you applied. The comprehensive logging and documentation outputs ensure that every step of your data preparation can be explained and defended in your thesis.
