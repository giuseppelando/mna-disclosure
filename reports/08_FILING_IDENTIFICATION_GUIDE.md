# Filing Identification Guide

**Phase:** 3 of 7 (Data Linkage → Content Retrieval)  
**Purpose:** Match each M&A deal to the specific 10-K filing that represents pre-announcement disclosure  
**Status:** Ready to execute

---

## Overview

The filing identification phase bridges your M&A deals data (with CIK identifiers) to specific EDGAR documents ready for text extraction. For each deal, we query EDGAR to retrieve the target company's complete filing history, then apply research design timing rules to select the single 10-K filing that best represents the disclosure environment that existed before the deal was announced.

This is a **critical juncture** in your pipeline because the quality of filing selection directly determines whether your NLP measures truly capture pre-deal disclosure or whether they're contaminated by anticipation effects, post-announcement updates, or temporal misalignment.

---

## Research Design Logic

### Selection Rules (Applied Sequentially)

1. **Form Type Filter**: Include only "10-K" forms (original annual reports)
   - Excludes: 10-Q (quarterly), 10-K/A (amendments), 8-K (current reports)
   - Rationale: 10-K provides comprehensive annual disclosure; amendments may reflect post-deal corrections

2. **Pre-Announcement Constraint**: Filing date must be < announcement date
   - Ensures temporal ordering (disclosure precedes deal)
   - Prevents forward-looking bias

3. **Minimum Lag Buffer**: Filing date must be ≤ (announcement date - 90 days)
   - Reduces anticipation contamination
   - 90 days ≈ one quarter, standard in literature
   - Configurable for robustness checks (60/120 days)

4. **Recency Principle**: Among qualifying filings, select the most recent
   - Most recent filing best reflects current state
   - Avoids using stale information when fresher data exists

### Why These Rules Matter

**Pre-announcement boundary**: Using filings dated after the announcement would violate the temporal logic of your research design. Post-announcement filings may contain deal-related disclosures, MD&A updates referencing the transaction, or risk factor modifications driven by the deal itself.

**Minimum lag buffer**: Even pre-announcement filings could be contaminated if filed during active deal negotiations. A 90-day buffer ensures the disclosure reflects routine reporting rather than deal-specific positioning. Literature precedent: Feldman et al. (2010) use 60-90 days; Hoberg & Phillips (2010) use similar windows.

**No amendments**: 10-K/A filings are amendments that correct or update original 10-Ks. These may reflect post-deal restatements or clarifications and should not be used to measure pre-deal disclosure quality.

---

## Implementation Architecture

### Module 1: EDGAR Submissions Query (`query_edgar_submissions.R`)

**Purpose**: Robust, cached access to SEC EDGAR company filing histories

**Key Function**: `query_edgar_submissions(cik, ...)`
- Queries SEC `/submissions/CIK{cik}.json` endpoint
- Returns complete filing history with dates, form types, URLs
- Implements caching to avoid redundant API calls
- Rate limiting: ~9 requests/second (SEC max = 10/sec)
- Error handling: Retries with exponential backoff

**Caching System**:
- First query: Downloads from EDGAR, saves to `data/interim/edgar_submissions_cache/CIK{cik}.json`
- Subsequent queries: Instant load from cache
- Cache persists across runs (reusable for robustness checks)

**Why SEC Submissions API**:
- Official structured data (JSON format)
- Complete filing history in single request
- Rate-limit friendly (1 request per company, not per filing)
- Maintained by SEC (stable, reliable)

### Module 2: Filing Selection Logic (`select_qualifying_filing.R`)

**Purpose**: Apply research design rules to select one 10-K per deal

**Key Function**: `select_qualifying_filing(filing_history, announcement_date, minimum_lag_days, ...)`

Returns:
```r
list(
  status = "matched",  # or "no_10k_filings", "no_qualifying_filing"
  filing = <single-row data frame>,  # Selected 10-K
  n_candidates = 25,  # Total 10-Ks available
  n_qualifying = 8,   # 10-Ks meeting timing rules
  days_before_announcement = 147,  # Actual lag
  cutoff_date = <date>  # Earliest allowable filing date
)
```

**Edge Cases Handled**:
- No 10-K filings exist → status = "no_10k_filings"
- All 10-Ks after announcement → status = "no_qualifying_filing"
- All 10-Ks within minimum lag window → status = "no_qualifying_filing"
- Multiple 10-Ks on same date → selects first, flags for review

### Module 3: Orchestration (`identify_preannouncement_filings.R`)

**Purpose**: Process all 1,238 deals systematically

**Workflow**:
1. Load deals with CIK identifiers
2. Query EDGAR for filing histories (with progress bar)
3. Apply selection logic to each deal
4. Compute match statistics
5. Generate diagnostic visualizations
6. Save comprehensive outputs

**Configuration Parameters** (top of script, easily adjustable):
```r
MINIMUM_LAG_DAYS <- 90     # Temporal buffer
ALLOW_AMENDMENTS <- FALSE  # Use original 10-K only
```

---

## Expected Results

### Match Rate Projection

**Anticipated**: 85-95% of 1,238 CIK-matched deals will successfully match to qualifying 10-Ks

**Why Not 100%**:
- Recent deals (2024-2025): Company may not have filed 10-K yet within lag window
- Young companies: Went public after deal announcement (no pre-deal 10-K exists)
- Filing gaps: Extensions, delays, or deregistration before deal
- Edge cases: Very short time from IPO to M&A deal

**Expected matched count**: 1,050-1,175 deals with filing URLs ready for text extraction

### Output Files

**1. Main Dataset**: `data/interim/deals_with_filings.rds`
- Contains all original deal variables
- Plus filing selection results:
  - `filing_date`, `filing_accession_number`, `filing_url`
  - `filing_selection_status`, `days_before_announcement`
  - `n_candidate_filings`, `n_qualifying_filings`

**2. Processing Log**: `data/interim/filing_identification_log.txt`
- Timestamped record of all operations
- EDGAR query results
- Selection statistics
- Edge case documentation

**3. Summary Statistics**: `data/interim/filing_identification_summary.json`
- Machine-readable match statistics
- Status breakdown
- Timing distributions
- Configuration parameters used

**4. Diagnostic Plots**: `output/filing_identification_diagnostics.pdf`
- Match status distribution (bar chart)
- Days before announcement histogram
- Filing date vs. announcement date scatter
- Temporal distribution of matched deals

### Quality Checks Embedded in Diagnostics

**Plot 1: Match Status** - Should show ~90% "matched", small % each of edge cases  
**Plot 2: Days Distribution** - Should show no deals with lag < 90 days (minimum enforced)  
**Plot 3: Date Scatter** - All points below red line (validates lag constraint)  
**Plot 4: Temporal Coverage** - Check for gaps in match coverage by year

---

## Running the Script

### Prerequisites

Ensure you have completed:
- ✅ Ingestion (`deals_ingested.rds`)
- ✅ Sample restriction (`deals_sample_restricted.rds`)
- ✅ CIK matching (`deals_with_cik_verified.rds`)

### Execution

```r
source("src/30_filings/identify_preannouncement_filings.R")
```

### What to Expect

**Phase 1: EDGAR Queries** (2-5 minutes)
- Progress bar shows CIK query progress
- First run: Downloads filing histories from EDGAR
- Subsequent runs: Loads from cache (near-instant)

**Phase 2: Filing Selection** (<1 minute)
- Applies timing rules to each deal
- No network calls (works on cached data)

**Phase 3: Output Generation** (<1 minute)
- Saves datasets
- Generates diagnostic plots

**Total Runtime**: 3-7 minutes first run, <2 minutes cached runs

### Interpreting Output

Console summary shows:
```
Deals with CIK: 1238
Matched to 10-K: 1127 (91.0%)

Match status breakdown:
  matched: 1127 deals (91.0%)
  no_qualifying_filing: 89 deals (7.2%)
  no_10k_filings: 22 deals (1.8%)
```

**Good indicators**:
- Match rate 85-95%
- Median days before announcement > minimum lag (e.g., 147 days when lag = 90)
- No deals violating minimum lag constraint
- Temporal coverage matches CIK-matched sample distribution

---

## Robustness and Sensitivity

### Easy Parameter Changes

The script is designed for effortless robustness checks:

**Test 60-day minimum lag**:
```r
# Edit line 47 of identify_preannouncement_filings.R
MINIMUM_LAG_DAYS <- 60  # Was 90

# Rerun (uses cache, very fast)
source("src/30_filings/identify_preannouncement_filings.R")

# Rename output to preserve
file.rename("data/interim/deals_with_filings.rds", 
            "data/interim/deals_with_filings_lag60.rds")
```

**Test 120-day minimum lag**: Same process, set to 120

**Allow amendments** (not recommended but testable):
```r
ALLOW_AMENDMENTS <- TRUE  # Was FALSE
```

### Comparing Robustness Checks

After running multiple specifications:
```r
# Load different versions
main <- readRDS("data/interim/deals_with_filings.rds")
lag60 <- readRDS("data/interim/deals_with_filings_lag60.rds")
lag120 <- readRDS("data/interim/deals_with_filings_lag120.rds")

# Compare match rates
data.frame(
  specification = c("Lag 60", "Lag 90 (main)", "Lag 120"),
  n_matched = c(
    sum(lag60$filing_selection_status == "matched"),
    sum(main$filing_selection_status == "matched"),
    sum(lag120$filing_selection_status == "matched")
  )
)

# Check overlap of matched deals
main_matched_ids <- main$deal_id[main$filing_selection_status == "matched"]
lag60_matched_ids <- lag60$deal_id[lag60$filing_selection_status == "matched"]

# How many matched in both?
length(intersect(main_matched_ids, lag60_matched_ids))
```

---

## Integration with Full Pipeline

### Upstream Dependencies

**Requires**:
- `deals_with_cik_verified.rds` (from CIK matching phase)
- Columns needed: `target_cik`, `announce_date`, `deal_id` (or similar identifier)

### Downstream Usage

**Feeds into**:
- Text Extraction (Phase 4): Downloads HTML from `filing_url`
- Section Parsing (Phase 4): Extracts MD&A and Risk Factors
- NLP Processing (Phase 5): Computes disclosure indices

**Key columns produced**:
- `filing_url`: Direct link to 10-K HTML on EDGAR
- `filing_accession_number`: Unique EDGAR identifier
- `filing_date`: For temporal controls in regressions

---

## Troubleshooting Common Issues

### Issue: Low Match Rate (<80%)

**Possible causes**:
1. Recent deals dominate sample (not enough time for 10-K filings)
2. Many young companies (went public recently, limited filing history)
3. EDGAR query failures

**Diagnostics**:
- Check temporal distribution: Are unmatched deals clustered in 2024-2025?
- Check status breakdown: Is "no_10k_filings" high?
- Review log for query failures

**Solutions**:
- If recent deals: Normal, proceed with matched sample
- If query failures: Check internet connection, SEC website status
- If systematic gaps: May need to adjust minimum lag downward (60 days)

### Issue: Script Hangs During EDGAR Queries

**Cause**: Network issues or SEC API slowness

**Solution**:
1. Check cache directory: Already-queried CIKs will be reused on restart
2. Interrupt and restart: Cache prevents redundant queries
3. Reduce batch size: Uncomment debug mode to process subset first

### Issue: Diagnostic Plots Look Wrong

**Examples of problems**:
- Points above red line in date scatter → minimum lag not enforced (BUG)
- Match rate much lower than expected → check status breakdown
- Temporal gaps → certain years have no matched deals

**Action**: Review log file for specific deals causing issues

---

## Technical Notes

### EDGAR Submissions API Response Structure

```json
{
  "cik": "0000320193",
  "name": "Apple Inc.",
  "filings": {
    "recent": {
      "form": ["10-K", "10-Q", "8-K", ...],
      "filingDate": ["2024-11-01", "2024-08-02", ...],
      "accessionNumber": ["0000320193-24-000123", ...],
      "primaryDocument": ["aapl-20240928.htm", ...]
    }
  }
}
```

**We extract**:
- `form` → `form_type`
- `filingDate` → `filing_date` (converted to Date class)
- `accessionNumber` → `accession_number`
- Construct URL: `https://www.sec.gov/Archives/edgar/data/{CIK}/{ACCESSION_NO_DASHES}/{PRIMARY_DOC}`

### Rate Limiting Implementation

SEC guidelines: Maximum 10 requests per second

Our implementation:
- 0.11 seconds between requests = ~9 req/sec (safe margin)
- Tracks last query time globally
- Waits if needed before next query
- Applies only to NEW queries (cache hits are instant)

### Caching Benefits

**First run**:
- 1,238 unique CIKs × 0.11 sec = ~137 seconds just for rate limiting
- Plus network latency → ~3-5 minutes total

**Subsequent runs with cache**:
- All 1,238 queries load from disk instantly
- Entire EDGAR query phase: <5 seconds
- Enables rapid robustness testing

**Cache persistence**:
- Files remain until manually deleted
- Safe to share cache across runs
- Can archive cache for reproducibility

---

## Next Steps After Filing Identification

Once filing identification completes successfully with good match rates:

1. **Review Diagnostics**: Open PDF, verify plots look reasonable
2. **Check Sample**: Load output RDS, examine matched deals
3. **Validate URLs**: Spot-check a few `filing_url` values by visiting in browser
4. **Confirm Ready**: Verify match rate meets expectations (85%+)

Then proceed to **Text Extraction Phase** where you will:
- Download HTML for each `filing_url`
- Parse EDGAR HTML structure
- Extract MD&A and Risk Factors sections
- Clean and store section text

---

## References

**Research Design Timing Rules**:
- Feldman, R., Govindaraj, S., Livnat, J., & Segal, B. (2010). "Management's tone change, post earnings announcement drift and accruals." *Review of Accounting Studies*
- Loughran, T., & McDonald, B. (2016). "Textual analysis in accounting and finance: A survey." *Journal of Accounting Research*

**SEC EDGAR Documentation**:
- [SEC API Documentation](https://www.sec.gov/edgar/sec-api-documentation)
- [EDGAR Filing Retrieval](https://www.sec.gov/os/accessing-edgar-data)
- [Rate Limiting Guidelines](https://www.sec.gov/os/accessing-edgar-data)

---

**Status**: Documentation complete. Script ready to execute.  
**Expected Runtime**: 3-7 minutes first run, <2 minutes cached runs  
**Expected Output**: ~1,050-1,175 deals with filing matches (85-95% of 1,238)
