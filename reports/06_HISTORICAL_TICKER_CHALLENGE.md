# Historical Ticker Resolution Challenge

**Date:** 2026-01-27  
**Status:** Deferred pending pipeline completion and final data characteristics  
**Current Approach:** Using verified bulk matches only (1,238 deals, 13.8%)

---

## The Challenge

Your draft M&A dataset contains 8,986 deals after sample restrictions, but only 1,238 (13.8%) match to CIK identifiers using the current SEC company tickers file. The remaining 85.7% of deals have ticker symbols that no longer appear in the contemporary SEC registry because those companies have been acquired, merged, or delisted.

**Example unmatched tickers:**
- TWX (Time Warner) - acquired by AT&T
- AET (Aetna) - acquired by CVS Health  
- CELG (Celgene) - acquired by Bristol-Myers Squibb
- AGN (Allergan) - acquired by AbbVie
- VMW (VMware) - acquired by Broadcom

These are major, well-documented M&A deals involving large public companies that definitely have 10-K filing histories in EDGAR. The challenge is mapping their historical ticker symbols to their CIK identifiers.

---

## Why This Matters

**For your research:**
- Larger sample = more statistical power
- Historical deals = longer time series for time-based controls
- Major M&A transactions = economically important events

**For completeness:**
- The unmatched deals include many landmark transactions
- Your research design focuses on completed deals (which means delisted targets)
- Industry consolidation means many targets no longer exist independently

---

## What We Tried: EDGAR Company Search

**Approach:** Query EDGAR's company search endpoint for each unmatched ticker individually.

**Implementation:** `test_historical_ticker_resolution.R` (archived)

**Results:** 
- Queried 5,780 unmatched tickers
- Found 482 "matches" (8.3% success rate)
- **CRITICAL FAILURE:** Matches were systematically incorrect

**Example incorrect matches:**
- ATVI → "ATVISO LTD" (should be Activision Blizzard)
- RTN → "QUANTITATIVE ALPHA TRADING INC." (should be Raytheon)
- HNZ → "HNZLLQ GLOBAL Ltd." (should be Heinz)

**Why it failed:**
EDGAR's company search performs **text search across company names** rather than **ticker symbol lookup**. When searching for "ATVI", it finds companies whose names contain those letters, not companies that traded under that ticker. This produces systematically wrong matches that would corrupt any downstream analysis.

**Decision:** Immediately reverted to verified bulk matches only.

---

## Alternative Approaches (Not Yet Implemented)

### Option 1: SEC Historical Index Files ⭐ (Most Reliable)

**Method:**
1. Download SEC quarterly index files for your time period (1997-2025)
2. Parse master index files to extract CIK-ticker pairs from actual filings
3. Build comprehensive historical mapping from filing metadata
4. Match your deals against this historical database

**Advantages:**
- Authoritative source (actual filing records)
- Captures tickers exactly as they existed when companies filed
- No third-party dependencies
- Perfect alignment with 10-K availability (mapping only includes filers)

**Disadvantages:**
- Requires downloading and parsing many large index files (gigabytes)
- Complex implementation (parsing SEC index format)
- Time-intensive first run (hours to build comprehensive mapping)
- Requires careful handling of ticker changes over time

**Implementation complexity:** High (2-3 days development)

**Reliability:** Highest (authoritative SEC data)

---

### Option 2: Company Name Matching

**Method:**
1. Use target company names from your deals data (not just tickers)
2. Query EDGAR company search by company name
3. Retrieve CIK for name matches
4. Verify ticker alignment as validation check

**Advantages:**
- Company names more stable than tickers
- EDGAR search works better with names than tickers
- Can implement fuzzy matching for variations

**Disadvantages:**
- Company names have variations (Inc. vs Incorporated, legal name changes)
- Requires sophisticated string matching (Levenshtein distance, etc.)
- More false positives to manually review
- Some companies have very generic names (conflicts)

**Implementation complexity:** Medium (1-2 days development)

**Reliability:** Medium (requires validation/review)

---

### Option 3: SEC Submissions API with Ticker History

**Method:**
1. Download current list of all CIKs from SEC
2. For each CIK, query `/submissions/` endpoint for company metadata
3. Extract `tickers` field which contains historical ticker information
4. Build comprehensive ticker→CIK mapping across all companies' histories

**Advantages:**
- Official SEC data
- Includes ticker history (not just current ticker)
- Programmatic access (no file parsing)

**Disadvantages:**
- Requires ~10,000 API calls to SEC (one per CIK)
- Must respect rate limits (slow: several hours)
- Not all submissions include complete ticker history
- Need to cache results to avoid repeated queries

**Implementation complexity:** Medium (1 day development + overnight processing)

**Reliability:** High (official data but not exhaustive)

---

### Option 4: Third-Party Historical Ticker Databases

**Services available:**
- **OpenFIGI** (Bloomberg's open identifier system) - Free API
- **WRDS** (Wharton Research Data Services) - Requires institutional access
- **Quandl/Nasdaq Data Link** - Commercial API
- **CRSP** (via university access) - Comprehensive stock database

**Advantages:**
- Pre-built comprehensive mappings
- Includes corporate actions (mergers, ticker changes)
- Professional data quality
- Fast (simple API lookup)

**Disadvantages:**
- Requires external accounts/API keys
- Some require institutional access or fees
- Introduces third-party dependency
- May have gaps for smaller companies
- Less control over data provenance

**Implementation complexity:** Low (if access available)

**Reliability:** High (professional services)

---

## Recommended Strategy (For Future Implementation)

**Phase 1: Complete Pipeline with Verified Matches (Current)**
- Use 1,238 verified bulk matches
- Develop filing identification → text extraction → NLP → econometrics
- Validate methods work end-to-end
- Present working prototype to advisor

**Phase 2: Assess Final Data Needs**
- Receive final SDC export (likely better match rate if properly filtered)
- Understand advisor's preferences on sample composition
- Determine if historical resolution is necessary given final data characteristics

**Phase 3: Implement Historical Resolution (If Needed)**

**Recommended approach order:**
1. **First try:** SEC Submissions API with ticker history (overnight process, official data)
2. **If insufficient:** SEC Historical Index Files (most authoritative, more complex)
3. **Supplement:** Company name matching for remaining unmatched
4. **Last resort:** Third-party service (if institutional access available)

**Implementation plan:**
```
1. Implement SEC Submissions approach (cache results)
2. Evaluate coverage improvement
3. If still <60% matched, add index file parsing
4. Manual review of sample of matches for quality control
5. Document provenance of each CIK mapping source
```

---

## Impact on Current Analysis

**With 1,238 verified matches:**
- ✅ Sufficient for methods development
- ✅ Spans 29 years (1997-2025)
- ✅ Covers 10+ major industries
- ✅ Includes completed and withdrawn deals
- ✅ Represents real M&A activity

**Limitations to acknowledge:**
- ⚠️ Sample limited to companies still filing with SEC
- ⚠️ May underrepresent older deals (more delistings)
- ⚠️ Excludes major historical M&A transactions
- ⚠️ Cannot study full population of announced deals

**For thesis documentation:**
"Our analytical sample is restricted to target firms with active SEC registration at the time of analysis, which enables reliable matching to 10-K filings but excludes acquired companies that were subsequently delisted. This selection may underrepresent deals from earlier in the sample period where target companies have since been acquired."

---

## Technical Notes for Future Implementation

### SEC Submissions API Structure

The `/submissions/` endpoint returns JSON with structure:
```json
{
  "cik": "0000320193",
  "name": "Apple Inc.",
  "tickers": ["AAPL"],
  "exchanges": ["Nasdaq"],
  "fiscalYearEnd": "0930",
  "filings": {
    "recent": { ... },
    "files": [ ... ]
  }
}
```

The `tickers` array should contain historical tickers, but not all submissions have complete history.

### Index File Format

SEC master index files (e.g., `master.idx`) have format:
```
CIK|Company Name|Form Type|Date Filed|Filename
1000045|NICHOLAS FINANCIAL INC|10-Q|2020-11-16|edgar/data/1000045/0001193125-20-298273.txt
```

Can be filtered to 10-K forms and parsed to build ticker mappings.

### Rate Limiting Requirements

SEC guidelines specify:
- Maximum 10 requests per second
- Use descriptive User-Agent header
- Implement exponential backoff for errors
- Consider overnight processing for bulk operations

---

## Decision Log

**2026-01-27:** Attempted EDGAR company search approach, discovered systematic incorrect matches, reverted to verified bulk matches only. Deferred historical resolution until after pipeline completion and final data assessment.

**Future decision point:** After presenting working pipeline to advisor and receiving final SDC export characteristics.

---

## References for Future Work

- [SEC EDGAR Submissions API](https://www.sec.gov/edgar/sec-api-documentation)
- [SEC Master Index Files](https://www.sec.gov/Archives/edgar/full-index/)
- [SEC Rate Limiting Guidelines](https://www.sec.gov/os/accessing-edgar-data)
- [OpenFIGI API Documentation](https://www.openfigi.com/api)

---

**Status:** Challenge documented and understood. Proceeding with verified matches. Revisit after pipeline complete and final data received.
