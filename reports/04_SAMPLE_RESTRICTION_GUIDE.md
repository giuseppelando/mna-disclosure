# Sample Restriction Script: Technical Documentation

## Overview

The sample restriction script (`apply_sample_restrictions.R`) is the critical bridge between data ingestion and analysis. It takes the full ingested dataset with all deals and sample filter flags, applies the research design inclusion criteria in a documented cascade, and produces the final analytical sample ready for CIK matching, filing retrieval, and econometric analysis.

## Current Project Status

Based on investigation of your project folder, here is where you stand right now:

### What Is Working
Your ingestion pipeline is production-ready. The refined `read_deals_v2_refined.R` script successfully processes your current Deals_v1.xlsx file, removing 4,332 duplicates to produce a clean dataset of 10,131 deals with proper outcome variables, industry classifications, payment method standardization, and comprehensive sample filter flags. The ingestion produces detailed logs and summary statistics suitable for inclusion in your thesis methodology chapter.

### What Exists But Is Disconnected
You have several scripts from earlier exploratory work including CIK mapping (`map_tickers_to_cik.R`), filing retrieval (`get_filings_v2.R`), and some preliminary analysis outputs. However, these scripts operate on older versions of the data that do not benefit from the standardized schema and research-design-aligned variable construction we just implemented. The `deals_with_cik.csv` file has only 2,701 rows compared to your current 10,131 deals, indicating it was created before proper deduplication. The column names in that file use the old raw format rather than the standardized schema names.

### The Critical Gap
The missing piece is the sample restriction step that should sit between ingestion and everything else. Without this step, you cannot properly document your sample construction cascade, demonstrate compliance with the research design decision log, or maintain a clean audit trail showing exactly what gets excluded and why. This script fills that gap.

## Why This Script Matters

Think of the research pipeline as a series of quality control gates. Ingestion brings everything in and creates flags for potential issues. Sample restriction is the first major quality gate where you make explicit decisions about what belongs in your analysis and what does not. By implementing this as a separate, well-documented script rather than scattering filter logic across multiple downstream scripts, you achieve three important goals.

First, you create a single source of truth for sample definition. Every downstream script that needs analysis-ready data uses the output of this script, ensuring consistency. If you later decide to adjust your sample criteria for robustness checks, you modify this one script rather than hunting through multiple files.

Second, you generate thesis-ready documentation automatically. The cascade flowchart, detailed logs, and summary statistics produced by this script can be directly cited in your methodology chapter when explaining sample construction. Reviewers can see exactly how many deals you started with, what criteria you applied, how many deals each criterion excluded, and what your final sample looks like.

Third, you enable easy robustness testing. The configuration section at the top of the script allows you to toggle filters on and off by changing a single boolean flag. Want to see if your results are sensitive to the US listing requirement? Set `us_listed: enabled = FALSE` and re-run. Want to test different time period definitions? Adjust the date parameters in the time period filter. This flexibility is crucial for rigorous empirical research.

## Script Architecture

The script is structured around a cascade design pattern where filters are applied sequentially and each step is documented. Let me explain how the major components work together.

### Configuration Section
At the top of the script, you will find the RESTRICTIONS list which defines all sample filters in the order they should be applied. Each restriction has five key elements. The enabled flag determines whether the filter is applied (set to FALSE to skip it for robustness checks). The description provides a human-readable explanation of what the filter does. The flag field specifies which column from the ingested dataset contains the boolean flag for this criterion. The design_reference cites the specific decision log entry from your Sample Blueprint ruleset. The rationale explains why this filter matters for your research design.

This configuration-driven approach means you can easily adjust the sample definition without modifying any core logic. All your decisions are explicit and documented at the top of the file where they are easy to review and modify.

### Cascade Application Logic
The apply_filter helper function implements the core cascade logic. For each enabled restriction, it counts how many deals pass the filter, applies the filter to create a smaller dataset, calculates how many deals were excluded and what percentage of the original sample remains, logs all these statistics, and records the step in the cascade tracking structure for later reporting.

The cascade proceeds step by step, with each filter operating on the output of the previous filter. This sequential application ensures that exclusion counts do not double-count deals that fail multiple criteria. The order matters because later filters can only exclude deals that survived earlier filters.

### Custom Filter Support
Some restrictions are straightforward flag-based filters where you simply check if a boolean column is TRUE. Others require custom logic, like the time period filter which needs to compare dates against configured boundaries. The script supports both patterns. Flag-based filters use the standard apply_filter function. Custom filters have their logic implemented inline with the same logging and tracking structure.

This hybrid approach keeps the script flexible enough to handle both simple and complex restrictions while maintaining consistency in how results are documented.

### Output Structure
The script produces four complementary outputs that serve different purposes. The restricted dataset (`deals_sample_restricted.rds`) is the primary output that downstream scripts will use. It contains only the deals that passed all enabled filters and is ready for CIK matching and filing retrieval.

The cascade log (`sample_restriction_log.txt`) provides a complete timestamped record of every step in the restriction process including how many deals passed each filter and what percentage of the original sample remains. The summary JSON (`sample_restriction_summary.json`) contains the same information in machine-readable format for programmatic access by downstream scripts or analysis notebooks. The flowchart (`sample_restriction_flowchart.txt`) provides a visual representation of the cascade suitable for inclusion in your thesis appendix showing the flow from starting sample through each filter to the final analytical sample.

Together, these outputs ensure that your sample construction is fully documented from multiple perspectives suitable for different audiences and uses.

## How to Use This Script

### Running on Your Current Data
To test the script on your current Deals_v1 data, simply run it after completing the ingestion step. From RStudio, open the script and click Source. From the command line, execute `Rscript src/20_clean/apply_sample_restrictions.R`. From the R console, run `source("src/20_clean/apply_sample_restrictions.R")`.

The script will automatically find your ingested dataset, apply the configured restrictions, and produce all output files. Watch the console output to see the cascade unfold in real time.

### Configuring for Your Final Dataset
When you receive your final SDC export, you may need to adjust the configuration depending on the characteristics of that data. The key decisions involve whether certain filters should be enabled or disabled.

The US listing filter (`us_listed`) is currently set to FALSE based on the assumption that your final SDC export will be pre-filtered to US targets. If your final export includes non-US targets, set this to TRUE to ensure you are analyzing only US deals where Form 10-K is available. You can verify whether this is needed by checking the target exchange field in your final data to see if any non-US exchanges appear.

The time period filter (`time_period`) is disabled by default because the appropriate time range depends on when SEC mandated consistent 10-K section structure. You should enable this and configure the dates once you determine the appropriate period for your analysis based on the advice in your research design document about ensuring MD&A and Risk Factors sections are consistently available and parsable.

### Robustness Checks
The configuration-driven design makes robustness checks straightforward. To test sensitivity to a particular criterion, simply set its enabled flag to FALSE, rerun the script, and compare the final sample size and characteristics to the baseline. Document which alternatives you tested and report the results in your thesis.

For example, you might want to check whether your results are robust to relaxing the control transfer criterion or to including deals with "Completed Assumed" status. Make the configuration change, rerun the restriction script, and then rerun your econometric analyses on the alternative sample.

## Integration with the Full Pipeline

This sample restriction script is designed to fit seamlessly into a complete processing pipeline. The upstream dependency is the refined ingestion script which must be run first to produce `deals_ingested.rds` with all the necessary sample filter flags. The downstream consumers will be the CIK matching script (which you already have in draft form as `map_tickers_to_cik.R` but which should be updated to work with the standardized schema names), the filing identification script (to be developed, which identifies the pre-announcement 10-K for each deal), and ultimately the NLP and econometric analysis scripts.

By inserting this explicit sample restriction step between ingestion and downstream processing, you ensure that all your analyses operate on a consistently defined analytical sample. No downstream script needs to implement its own filtering logic or make ad hoc exclusion decisions. Everything flows from the single source of truth produced by this restriction script.

## When You Get Your Final Dataset

When your final SDC export arrives, the workflow should be straightforward. First, place the new Excel file in `data/raw/` (replacing or renaming the current Deals_v1.xlsx). Second, run the ingestion script to produce the updated `deals_ingested.rds`. Third, review the configuration section of this restriction script to ensure all filters are set appropriately for your final data characteristics. Fourth, run this restriction script to produce `deals_sample_restricted.rds`. Fifth, proceed to CIK matching, filing retrieval, and analysis knowing that your sample is properly constructed and documented.

The beauty of this approach is that every step is deterministic and reproducible. If you later need to revise something, you can simply adjust the relevant script, rerun from that point forward, and all downstream outputs will update automatically. There is no manual intervention, no ad hoc decisions scattered across multiple files, and no ambiguity about how the sample was constructed.

## Current Sample Expectations

Based on the ingested data statistics, here is what to expect when you run this script on your current Deals_v1 data. You start with 10,131 deals after deduplication during ingestion. With the default configuration where only the four core restrictions are enabled (target identifiable, announcement date, terminal outcome, control transfer) plus the two quality filters (date logic and negative values), you should end up with approximately 8,500 to 9,000 deals in your restricted sample.

The largest exclusion will come from the terminal outcome filter because about 860 deals have "Completed Assumed" or "Other" status which cannot be used for completion probability analysis. The control transfer filter should exclude very few deals (less than 2 percent) because most M&A transactions in your data are genuine acquisitions. The announcement date filter will exclude roughly 360 deals that have missing announcement dates. The quality filters should exclude only a handful of deals with logical inconsistencies or obvious data errors.

If you enable the US listing filter, expect to exclude roughly 45 percent of the remaining sample because a substantial portion of your current data involves non-US targets or targets where exchange information is missing. Whether you should enable this filter depends on whether your final SDC export will be pre-filtered to US listings or whether you need to enforce this criterion yourself.

## Summary

This sample restriction script provides the critical linkage between ingestion and analysis by implementing the research design sample criteria in a transparent, configurable, well-documented manner. It produces thesis-ready documentation of your sample construction cascade while maintaining the flexibility needed for robustness checks. When combined with your refined ingestion script, it establishes a solid foundation for all downstream processing that will serve you well both for your current draft analysis and for your final results once the complete SDC dataset arrives.
