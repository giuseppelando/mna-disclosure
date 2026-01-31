# Post-Ingestion Pipeline (Phase 20)

## Overview

This folder contains the **post-ingestion pipeline** for the M&A disclosure project. It transforms the raw ingested deal data into a research-ready dataset with SEC filing information.

## Execution Map

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     POST-INGESTION PIPELINE                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  INPUT: data/interim/deals_ingested.rds                                 │
│         (from src/10_ingest/ingest_deals_final.R)                       │
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │ STEP 1: 20_sample_apply.R                                         │  │
│  │         Apply research design sample restrictions                 │  │
│  │         - Terminal outcome filter (completed/withdrawn)           │  │
│  │         - Required identifiers (deal_id, ticker, dates)           │  │
│  │         - Time period bounds (2006-2023)                          │  │
│  │         OUTPUT: data/interim/deals_sample.rds                     │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                              ↓                                          │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │ STEP 2: 21_cik_resolve.R                                          │  │
│  │         Multi-strategy CIK resolution                             │  │
│  │         - SEC bulk ticker file (current tickers)                  │  │
│  │         - Historical ticker database (delisted companies)         │  │
│  │         - Company name fuzzy matching (fallback)                  │  │
│  │         OUTPUT: data/interim/deals_with_cik.rds                   │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                              ↓                                          │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │ STEP 3: 22_filing_identify.R                                      │  │
│  │         Identify pre-announcement 10-K filings                    │  │
│  │         - Query SEC EDGAR submissions API                         │  │
│  │         - Apply research design timing constraints                │  │
│  │         - Select qualifying 10-K per deal                         │  │
│  │         OUTPUT: data/interim/deals_with_filing.rds                │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│  FINAL: data/interim/deals_ready_for_download.csv                       │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

## Quick Start

**Option 1: Run entire pipeline**
```r
source("src/20_resolve/00_run_pipeline.R")
```

**Option 2: Run step by step**
```r
source("src/20_resolve/20_sample_apply.R")   # Step 1
source("src/20_resolve/21_cik_resolve.R")    # Step 2
source("src/20_resolve/22_filing_identify.R") # Step 3
```

## Prerequisites

1. **Ingestion must be complete:**
   ```r
   source("src/10_ingest/ingest_deals_final.R")
   ```
   
2. **Required output:** `data/interim/deals_ingested.rds`

## Output Files

| File | Description |
|------|-------------|
| `deals_sample.rds` | Sample after research design filters |
| `deals_with_cik.rds` | Deals with resolved CIK numbers |
| `deals_with_filing.rds` | Deals with filing identification |
| `deals_ready_for_download.csv` | CSV of deals ready for filing download |
| `cik_unmatched_deals.csv` | Deals where CIK could not be resolved |
| `filing_selection_details.csv` | Detailed filing selection reasoning |

## Configuration

### Filing Selection Parameters (in `22_filing_identify.R`)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `min_lag_days` | 30 | Minimum days between filing and announcement |
| `max_lookback_days` | 450 | Maximum days to search for filings |
| `target_forms` | 10-K, 10-K/A, 20-F, 20-F/A | Form types to consider |
| `prefer_original` | TRUE | Prefer original over amended filings |

### Sample Restrictions (in `20_sample_apply.R`)

| Filter | Description |
|--------|-------------|
| Terminal outcome | Only completed or withdrawn deals |
| Required identifiers | Must have deal_id, target_name, ticker |
| Time period | 2006-2023 (Item 1A mandatory from 2005) |

## Diagnostic Outputs

Each step produces a log file:
- `data/interim/sample_apply_log.txt`
- `data/interim/cik_resolve_log.txt`
- `data/interim/filing_identify_log.txt`

And a JSON summary:
- `data/interim/sample_apply_summary.json`
- `data/interim/cik_resolve_summary.json`
- `data/interim/filing_identify_summary.json`

## Research Design Alignment

This pipeline implements the following research design requirements:

1. **Routine disclosure only**: Uses 10-K filings (not deal-specific documents)
2. **Pre-announcement timing**: Filing must precede deal announcement
3. **Minimum lag**: Configurable buffer to avoid contamination
4. **Section availability**: MD&A and Item 1A present (post-2005)

## Troubleshooting

**Low CIK match rate (<50%)**
- Build historical ticker database: `source("src/15_historical_cik/build_historical_ticker_database.R")`
- Check ticker quality in source data
- Consider manual lookup for critical deals

**No filings found**
- Check SEC API connectivity
- Verify CIK format (should be 10-digit zero-padded)
- Review timing constraints

**API rate limiting**
- Default delay is 0.15s between requests
- Increase if experiencing 429 errors

## Next Steps

After this pipeline completes, proceed to:
```r
source("src/30_download/30_download_filings.R")  # Download 10-K HTML files
```
