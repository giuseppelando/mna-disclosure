# Literature-Aligned Filing Selection: Implementation Notes

**Date:** 2026-01-27 (Revised)  
**Key Change:** Minimum lag 90→30 days + fallback mechanism

---

## What Changed and Why

### Original Implementation (Too Conservative)
- Minimum lag: **90 days** (roughly one quarter)
- No fallback: Excluded deals when most recent 10-K within window
- Result: Would unnecessarily exclude many deals

### Literature-Aligned Implementation (Current)
- Minimum lag: **30 days** (conservative availability buffer)
- Fallback mechanism: Use prior 10-K when most recent too close
- Result: Preserves sample size while maintaining timing discipline

---

## Literature Standards (Empirical M&A-Disclosure)

### Consensus Practices

1. **Most recent pre-announcement 10-K** (baseline)
   - Latest Form 10-K with filing date < announcement date
   - Best represents target's information environment during bidder screening

2. **Short availability buffer: 10-30 days**
   - Purpose: Ensure filing publicly observable and processable
   - NOT about market efficiency or strict exogeneity
   - 30 days = "most conservative and widely accepted choice"

3. **Fallback to prior 10-K when most recent too close**
   - If most recent 10-K falls within exclusion window, use prior fiscal year
   - Preserves sample size vs. dropping observations
   - Standard practice in well-designed studies

4. **Never use post-announcement filings**
   - Universal rule (contamination by deal-related incentives)

5. **Robustness via alternative specifications**
   - Test 10 vs. 20 vs. 30 days
   - Document primary vs. fallback usage
   - Show results stable across specifications

### Rationale

> "The objective is not to claim strict exogeneity, but to ensure that the
> textual measures reflect routine disclosure produced outside the public deal
> window and plausibly used by acquirers in screening and preliminary valuation."

The buffer is an **identification safeguard**, not a behavioral assumption.

---

## Implementation Details

### Algorithm

```
For each deal:
  1. Get all 10-K filings before announcement date
  2. Sort by filing date (most recent first)
  
  3. Check most recent 10-K:
     If filing_date <= (announcement_date - 30 days):
       → Use most recent (PRIMARY SELECTION)
       → Status: "matched_primary"
     
  4. Else (most recent too close):
     Check if prior 10-K exists:
       If yes:
         → Use second-most-recent (FALLBACK)
         → Status: "matched_fallback"
       If no:
         → Mark unmatched
         → Status: "no_qualifying_filing"
```

### Configuration (src/30_filings/identify_preannouncement_filings.R)

```r
MINIMUM_LAG_DAYS <- 30        # Literature standard
ENABLE_FALLBACK <- TRUE       # Use prior 10-K when needed
ALLOW_AMENDMENTS <- FALSE     # Original 10-K only
```

### Output Fields Added

**New diagnostic fields:**
- `filing_selection_status`: "matched_primary" / "matched_fallback" / "no_10k_filings" / "no_qualifying_filing"
- `filing_selection_method`: "most_recent" / "prior_year_fallback"
- `used_fallback`: TRUE/FALSE flag
- `most_recent_10k_date`: Date of most recent 10-K (for diagnostics)

These enable:
- Tracking which deals used fallback
- Robustness checks comparing primary vs. fallback subsamples
- Transparent reporting in thesis methodology

---

## Expected Impact on Match Rates

### Before (90-day minimum, no fallback)
- **Projected**: 85-90% match rate
- Many deals excluded unnecessarily

### After (30-day minimum + fallback)
- **Projected**: 92-97% match rate
- Fewer exclusions via fallback mechanism
- Sample preservation while maintaining timing discipline

### Breakdown Expected

Out of 1,238 CIK-matched deals:
- ~1,050-1,100 **Primary** matches (most recent 10-K meets 30-day buffer)
- ~50-100 **Fallback** matches (used prior 10-K)
- ~40-100 **Unmatched** (no qualifying 10-K available)

Total matched: ~1,140-1,200 (92-97%)

---

## Robustness Testing Strategy

### Specification 1: 10-Day Minimum (Liberal)
```r
MINIMUM_LAG_DAYS <- 10
```
- Most liberal specification
- Maximizes sample size
- Tests sensitivity to availability assumption

### Specification 2: 30-Day Minimum (Main)
```r
MINIMUM_LAG_DAYS <- 30
```
- Literature-standard conservative buffer
- PRIMARY SPECIFICATION for thesis
- Balances sample size and timing discipline

### Specification 3: No Fallback (Restrictive)
```r
MINIMUM_LAG_DAYS <- 30
ENABLE_FALLBACK <- FALSE
```
- Excludes deals when most recent 10-K too close
- Tests whether fallback drives results
- More conservative sample composition

### How to Run Robustness Checks

1. Edit configuration in main script
2. Rerun (fast: uses EDGAR cache)
3. Rename output to preserve:
   ```r
   file.rename(
     "data/interim/deals_with_filings.rds",
     "data/interim/deals_with_filings_lag10.rds"
   )
   ```
4. Repeat for other specifications
5. Compare match rates and overlap

---

## Thesis Documentation Template

### Methodology Section Text

> "Following standard practice in the M&A-disclosure literature (Loughran & McDonald, 2016; Hoberg & Phillips, 2010), we measure target disclosure quality using the most recent Form 10-K filed at least 30 days before the deal announcement. The 30-day buffer ensures that the filing was publicly available and could plausibly be processed by potential acquirers during preliminary valuation, while avoiding mechanical contamination from last-minute or deal-related disclosure (Feldman et al., 2010).
>
> When the most recent 10-K falls within the 30-day exclusion window, we use the prior fiscal year's 10-K as a fallback to preserve sample size while maintaining timing discipline. This approach is consistent with prevailing standards for selecting pre-announcement routine disclosure in settings where strict exogeneity is not claimed but temporal ordering is essential for construct validity."

### Robustness Section Text

> "Our main results use a 30-day minimum lag, which represents the conservative standard in the literature. In untabulated robustness checks, we verify that our findings are not sensitive to this specification by re-estimating all models using 10-day and 50-day minimum lags. [Results are qualitatively similar and available upon request / shown in Appendix Table X]. We also confirm that our results hold when we exclude observations using the fallback mechanism (i.e., restricting to deals where the most recent 10-K meets the timing requirement), though this reduces the sample by approximately [X]%."

---

## References

Key papers establishing these conventions:

- Loughran, T., & McDonald, B. (2016). Textual analysis in accounting and finance: A survey. *Journal of Accounting Research*, 54(4), 1187-1230.

- Feldman, R., Govindaraj, S., Livnat, J., & Segal, B. (2010). Management's tone change, post earnings announcement drift and accruals. *Review of Accounting Studies*, 15(4), 915-953.

- Hoberg, G., & Phillips, G. (2010). Product market synergies and competition in mergers and acquisitions: A text-based analysis. *Review of Financial Studies*, 23(10), 3773-3811.

---

## Next Actions

✅ **Implementation complete** - Scripts updated with literature-aligned logic  
✅ **Ready to run** - Execute `identify_preannouncement_filings.R`  
✅ **Documentation current** - Approach now matches empirical standards  

Expected runtime: 3-7 minutes (first run with EDGAR queries)  
Expected output: ~1,140-1,200 matched deals (92-97% of 1,238 CIK-matched)

---

**Status**: Ready for execution with literature-aligned parameters
