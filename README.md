# Improving-SaaS-Conversion-Funnel-From-Signup-to-Paid-Users (In-progress)
From Signup → Paid: A Revenue Intelligence Project

Not just a data cleanup exercise, a business problem with a dollar sign on it.

## Table of Contents
- [The Business Problem](#the-business-problem)
- [Data Quality](#data-quality)
- [Funnel Breakdown](#funnel-breakdown)
- [Top 3 Recommendations](#top-3-recommendations)
- [Tech Stack](#tech-stack)
- [How I Used AI](#how-i-used-ai)
- [Dashboard Snapshots](#dashboard-snapshots)


## The Business Problem
A SaaS company has 5,000 signups but only 7.8% ever become paying customers. This project diagnoses exactly where and why users are dropping off, quantifies the revenue impact, and delivers three specific, actionable recommendations.

## Data Quality
**8 Defects Fixed Before Analysis**

| # | Defect | Records affected | How detected | Fix |
|---|--------|------------------|--------------|-----|
| 1 | Orphaned payments | 87,831 payer IDs (63% of 139,144) with no event trail | SQL anti-join: payments → events on user_id | Payments rebuilt from payment_success / payment_failed events only; 0 orphans, 20,458 rows |
| 2 | Broken funnel hierarchy | 26,876 onboarding_complete users (85.6%) had no signup event; 26,830 trial users (85.8%) had no onboarding_complete | Subset check between consecutive stages (only 14.4% / 14.2% overlap) | Events regenerated as a nested sequence: each stage requires the prior one, timestamps strictly increasing |
| 3 | Inconsistent payment_status | 41,669 rows with case/whitespace variants + 10,819 unusable (1,996 `unknown`, 8,823 null) = 52,488 (23.9%) | `GROUP BY payment_status` showed 10 variants of 3 values | Normalised to success / failed / refunded; `pending` and unresolvable rows removed in rebuild |
| 4 | Missing funnel stages | 4 of 7 event types absent (onboarding_start, payment_initiated, payment_success, payment_failed) | `SELECT DISTINCT event_type` vs funnel spec | All 7 stages present; 574,813 events |
| 5 | No stage drop-off | onboarding_complete = 99.9%, trial_start = 99.5% of signups (31,440 / 31,403 / 31,277) | Stage-to-stage ratio check | Rebuilt attrition: 100% → 77.9% → 40.4% → 24.3% → 9.3% → 7.3% |
| 6 | Users with no events | 138,675 users (63.0%) | `LEFT JOIN users → events WHERE event IS NULL` | Every user has a signup event timestamped at `users.signup_date` (220,000 / 220,000) |
| 7 | Payments cardinality | 220,000 rows for 139,144 distinct payers (80,856 surplus rows), mirroring the users table 1:1 | `COUNT(*)` vs `COUNT(DISTINCT user_id)`; `HAVING COUNT(*) > 1` | One row per payment attempt, tied to its event |
| 8 | Missing values | users: 31,263 cells (country 13,430 / device 8,926 / source 8,907); sessions: 1,547 durations (1.8%); payments: 2,980 amounts | `isnull().sum()` per column | Users: imputed by sampling the observed distribution (limitation: dilutes segment signal). Session nulls kept, excluded from averages, not imputed. Amount nulls removed in rebuild |


## Funnel Breakdown

| Stage                | Users | Conversion % | Drop-off |
|---------------------|-------|--------------|----------|
| Signup              | 5,000 | 100%         | —        |
| Onboarding Start    | 3,930 | 78.6%        | 21.4%    |
| Onboarding Complete | 2,073 | 41.5%        | 47.3%    |
| Trial Start         | 1,301 | 26.0%        | 37.2%    |
| Payment Initiated   | 506   | 10.1%        | 61.1%    |
| Payment Success     | 392   | 7.8%         | 22.5%    |

### Key Insights : 
- Major drop at **Payment Initiation (61%)** → Indicates strong purchase friction
- Significant loss during **Onboarding Completion (47.3%)** → UX or complexity issue
- 21.4% users never start onboarding → Weak activation or unclear value proposition

## Tech Stack
SQL, Python (pandas), Power BI, Excel, Claude

## How I Used AI 
- **Used for:** profiling the tables, drafting DuckDB/pandas validation checks, stress-testing funnel hypotheses, and reviewing my Power BI pages as a recruiter would.
- **Validation:** every number was reconciled across pandas, DuckDB SQL and Power BI DAX before use (see `docs/reconciliation.md`).
- **What Claude got wrong:**
  1. Its first funnel numbers and dashboard text came from a 5,000-user sample it generated itself, not my 220K tables. My own EDA didn't reconcile, so I re-profiled from source and found the 8 defects above.
  2. It labelled the ~$105K recovery estimate "annual". I recomputed it (+10% initiations × 78.8% success × $65.48 avg ≈ $105.6K) and found it spans the ~25-month window.
  3. It reported a 2.45× activation lift measured on the earlier sample, not on the rebuilt data. I removed it.
## Dashboard Snapshots
### Executive Summary :

<img width="1295" height="723" alt="Screenshot 2026-05-15 000217" src="https://github.com/user-attachments/assets/5230d075-084e-4baa-9d85-5ed06f8c426e" />


