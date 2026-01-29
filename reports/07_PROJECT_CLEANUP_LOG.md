# Project Cleanup Guide and File Inventory

**Last Updated:** 2026-01-27  
**Status:** After CIK matching phase, before filing identification

---

## Current Pipeline Status

✅ **COMPLETE:** Ingestion, Sample Restriction, CIK Matching (verified bulk only)  
🔄 **IN PROGRESS:** Filing Identification  
⏸️ **DEFERRED:** Historical ticker resolution (requires different approach)

---

## Directory Structure: Keep vs. Delete

### `/src/` - SOURCE CODE (All scripts to KEEP)

#### **PRODUCTION SCRIPTS (actively used in pipeline):**

**`/src/10_ingest/`**
- ✅ **KEEP:** `read_deals_v2_refined.R` - Main ingestion script (production)
- ✅ **KEEP:** `revert_to_verified_matches.R` - CIK matching with verified bulk matches only
- ⚠️ **ARCHIVE:** `map_tickers_to_cik.R` - Original draft (superseded, keep for reference)
- ⚠️ **ARCHIVE:** `map_tickers_to_cik_refined.R` - First refinement (superseded, keep for reference)
- ⚠️ **ARCHIVE:** `test_historical_ticker_resolution.R` - Failed EDGAR search approach (keep for documentation)

**`/src/20_clean/`**
- ✅ **KEEP:** `apply_sample_restrictions.R` - Sample restriction cascade (production)

**`/src/30_nlp/`, `/src/40_merge/`, `/src/50_models/`, `/src/90_utils/`**
- Currently empty, will be populated in next phases

#### **RECOMMENDATION FOR `/src/10_ingest/` cleanup:**

Create an archive folder for superseded scripts:
```
/src/10_ingest/
  ├── read_deals_v2_refined.R              [PRODUCTION]
  ├── revert_to_verified_matches.R         [PRODUCTION]
  └── _archive/
      ├── map_tickers_to_cik.R             [Original draft]
      ├── map_tickers_to_cik_refined.R     [First attempt]
      └── test_historical_ticker_resolution.R [Failed EDGAR search]
```

---

### `/data/` - DATA FILES

#### **`/data/raw/`**
- ✅ **KEEP:** `Deals_v1.xlsx` - Original SDC export (never delete)

#### **`/data/interim/` - INTERMEDIATE FILES**

**PRODUCTION PIPELINE OUTPUTS (KEEP):**
- ✅ `deals_ingested.rds` (10,131 deals after ingestion)
- ✅ `deals_sample_restricted.rds` (8,986 deals after restrictions)
- ✅ `deals_with_cik_verified.rds` (1,238 deals with verified CIK matches)
- ✅ `sample_restriction_flowchart.txt`
- ✅ `sample_restriction_log.txt`
- ✅ `sample_restriction_summary.json`

**SUPPORTING FILES (KEEP):**
- ✅ `deals_ingestion_summary.txt`
- ✅ `deals_ingestion_summary.json`
- ✅ `deals_column_mapping.csv`
- ✅ `deals_quality_issues.csv`
- ✅ `deals_ingestion_log.txt`

**FAILED/SUPERSEDED OUTPUTS (DELETE OR ARCHIVE):**
- ❌ **DELETE:** `deals_with_cik.rds` - Original bulk-only matches (superseded by verified version)
- ❌ **DELETE:** `deals_with_cik_enhanced.rds` - Contains incorrect EDGAR search matches
- ❌ **DELETE:** `cik_mapping_log.txt` - Log from original script (superseded)
- ❌ **DELETE:** `cik_mapping_summary.json` - Summary from original script (superseded)
- ❌ **DELETE:** `ticker_cik_lookup.csv` - From original script (superseded)
- ⚠️ **ARCHIVE:** `ticker_cik_cache.rds` - Contains incorrect EDGAR matches, keep for analysis of what went wrong
- ⚠️ **ARCHIVE:** `ticker_resolution_test_log.txt` - Documents failed approach
- ⚠️ **ARCHIVE:** `ticker_resolution_report.txt` - Shows incorrect matches, useful for documentation

**OLD EXPLORATORY FILES (if they exist - DELETE):**
- ❌ `deals_clean.csv` - Pre-refinement format
- ❌ `filings_text.csv` - Old text extraction attempts
- ❌ Any files with "test", "temp", "old" in the name

#### **`/data/processed/`**
- ❌ **DELETE:** `analysis_ready.csv` - Old preliminary analysis (predates refined pipeline)
- Keep empty for future final analytical dataset

---

### `/output/` - ANALYSIS OUTPUTS
Currently empty - will contain tables and figures from econometric analysis

---

### `/config/` - CONFIGURATION
- ✅ **KEEP:** `data_schema.yaml` - Column definitions and validation rules

---

### `/reports/` - DOCUMENTATION

**CURRENT DOCUMENTATION (KEEP ALL):**
- ✅ `01_PROJECT_OVERVIEW.md`
- ✅ `02_INGESTION_REFINEMENTS.md`
- ✅ `03_QUICKSTART_GUIDE.md`
- ✅ `04_SAMPLE_RESTRICTION_GUIDE.md`
- ✅ `05_CIK_MAPPING_GUIDE.md`
- 📝 **ADD:** `06_HISTORICAL_TICKER_CHALLENGE.md` (see below)
- 📝 **ADD:** `07_PROJECT_CLEANUP_LOG.md` (this document)

---

### `/tests/`
- ✅ **KEEP:** `test_ingestion_refined.R` - Unit tests for ingestion

---

## Recommended Cleanup Actions

### IMMEDIATE (Do Now):

```r
# Delete incorrect/superseded files
file.remove("data/interim/deals_with_cik_enhanced.rds")
file.remove("data/interim/cik_mapping_log.txt")
file.remove("data/interim/cik_mapping_summary.json")
file.remove("data/interim/ticker_cik_lookup.csv")

# Delete old analysis file if exists
if (file.exists("data/processed/analysis_ready.csv")) {
  file.remove("data/processed/analysis_ready.csv")
}
```

### ORGANIZE (Create Archive):

```r
# Create archive directories
dir.create("src/10_ingest/_archive", recursive = TRUE, showWarnings = FALSE)
dir.create("data/interim/_archive", recursive = TRUE, showWarnings = FALSE)

# Move superseded scripts to archive
file.rename(
  "src/10_ingest/map_tickers_to_cik.R",
  "src/10_ingest/_archive/map_tickers_to_cik.R"
)
file.rename(
  "src/10_ingest/map_tickers_to_cik_refined.R",
  "src/10_ingest/_archive/map_tickers_to_cik_refined.R"
)
file.rename(
  "src/10_ingest/test_historical_ticker_resolution.R",
  "src/10_ingest/_archive/test_historical_ticker_resolution.R"
)

# Move failed experiment outputs to archive
file.rename(
  "data/interim/ticker_cik_cache.rds",
  "data/interim/_archive/ticker_cik_cache.rds"
)
file.rename(
  "data/interim/ticker_resolution_test_log.txt",
  "data/interim/_archive/ticker_resolution_test_log.txt"
)
file.rename(
  "data/interim/ticker_resolution_report.txt",
  "data/interim/_archive/ticker_resolution_report.txt"
)
```

---

## Current Pipeline Data Flow

**VERIFIED CLEAN PIPELINE:**

```
RAW DATA
  └─ Deals_v1.xlsx (10,131 rows)
       │
       ↓ [read_deals_v2_refined.R]
       │
INGESTED
  └─ deals_ingested.rds (10,131 deals, deduplicated, typed, with flags)
       │
       ↓ [apply_sample_restrictions.R]
       │
RESTRICTED
  └─ deals_sample_restricted.rds (8,986 deals after research design filters)
       │
       ↓ [revert_to_verified_matches.R]
       │
CIK MATCHED
  └─ deals_with_cik_verified.rds (1,238 deals with verified CIK identifiers)
       │
       ↓ [NEXT: filing identification]
       │
FILING IDENTIFIED
  └─ deals_with_filings.rds (TBD: deals matched to specific 10-K filings)
```

**FILES AT EACH STAGE:**
- Ingestion: 1 RDS + 6 supporting docs
- Restriction: 1 RDS + 3 supporting docs  
- CIK matching: 1 RDS (verified clean)
- Total: 3 RDS files in sequence + documentation

---

## Files Summary Statistics

**KEEP (Production Pipeline):**
- Scripts: 2 (ingestion + restriction) + 1 (CIK matching) = 3 active
- Data: 3 RDS files (ingested → restricted → CIK matched)
- Documentation: 5 markdown files
- Supporting: 10 log/summary files

**ARCHIVE (Reference Only):**
- Scripts: 3 (superseded CIK matching attempts)
- Data: 3 files (failed EDGAR search outputs)

**DELETE (Incorrect/Obsolete):**
- 4 files with incorrect data or superseded logs

---

## Key Principles for Future Cleanup

1. **Never delete raw data** (`Deals_v1.xlsx`)
2. **Keep all production pipeline RDS files** (represent stages)
3. **Archive, don't delete, experimental scripts** (useful for documentation)
4. **Delete files with incorrect data** (like enhanced dataset with wrong matches)
5. **Maintain documentation** explaining what was tried and why it failed
6. **One file per pipeline stage** (avoid proliferation of intermediate versions)

---

## Historical Ticker Resolution: Deferred Challenge

**Problem:** 85.7% of deals have historical tickers not in current SEC registry  
**Failed Approach:** EDGAR company search returns wrong matches (see archived test results)  
**Current Solution:** Use 1,238 verified bulk matches for pipeline development  
**Future Options:** See `06_HISTORICAL_TICKER_CHALLENGE.md` for detailed analysis

**Decision:** Proceed with verified matches, revisit historical resolution after:
1. Complete pipeline is working end-to-end
2. Final SDC export characteristics are known
3. Advisor provides guidance on sample composition priorities

---

## Next Steps

1. ✅ Cleanup: Run the deletion/archiving commands above
2. ✅ Verify: Confirm only 3 RDS files remain in `/data/interim/` (not archived)
3. ✅ Document: Create `06_HISTORICAL_TICKER_CHALLENGE.md` 
4. 🔄 Develop: Filing identification script for 1,238 verified matches
5. 🔄 Continue: Text extraction → NLP → Econometrics

---

**Last Review Date:** 2026-01-27  
**Pipeline Status:** Ready for Filing Identification Phase  
**Working Sample:** 1,238 deals with verified CIK matches
