# AI-Assisted Analytical Workflow

## Workflow: AI Proposes, SQL Disposes

This project uses AI as an analytical assistant, not as a source of truth.

Every AI-generated suggestion, query, or finding must be independently validated using SQL, Python, or Power BI before being accepted.

### 1. Profile

Provide the AI with:

* Database schemas.
* Data types and column descriptions.
* `describe()` outputs.
* `value_counts()` results.
* Relevant sample records.

**Objective:** Identify and rank potential data-quality defects by business impact.

### 2. Validate

Ask AI to generate detection queries and specify expected results.

Execute the queries independently using DuckDB and compare actual results against expected results.

**Objective:** Ensure that AI-generated SQL produces verifiable results.

### 3. Hypothesize

Before examining the results, document:

* Hypothesis.
* Metric being evaluated.
* Statistical or analytical test.
* Expected outcome.
* Conditions that would falsify the hypothesis.

**Objective:** Avoid confirmation bias and unsupported conclusions.

### 4. Reconcile

Calculate important metrics independently using:

* Python (Pandas).
* SQL (DuckDB).
* Power BI (DAX).

Record the results and investigate discrepancies.

**Objective:** Establish consistency across analytical tools.

### 5. Adversarial Review

Ask AI to challenge the findings by investigating:

* Survivorship bias.
* Reverse causality.
* Join fan-out.
* Time-window bias.
* Imputation-related distortions.

**Objective:** Identify alternative explanations and analytical weaknesses.

### 6. Convert Findings into Business Decisions

Every significant finding should translate into:

**Finding → Business Decision → Owner → Success Metric → Expected Impact**

Include assumptions, limitations, and potential risks associated with the recommendation.

**Objective:** Ensure that analysis leads to actionable business outcomes.


## AI Prompt Library

### 1. Defect Audit

**Purpose:** Identify data-quality issues before analysis.

Prompt:

> Act as a data auditor. Given the database schemas, describe() outputs, and value_counts() below, identify potential data-quality defects ranked by their potential revenue impact.
>
> For each defect, provide:
>
> * Description of the issue.
> * Detection query using DuckDB.
> * Expected row count or validation condition.
> * Potential business consequence.
> * What an unexpected result would indicate.

### 2. Key Integrity

**Purpose:** Validate primary and foreign key relationships.

Prompt:

> Given these four tables and their respective keys, write anti-join and subset validation queries for every relationship.
>
> Return a table containing:
>
> * Validation check.
> * SQL query.
> * Expected result.
> * Consequence if the validation fails.

### 3. Funnel Validity

**Purpose:** Verify the logical consistency of funnel stages.

Prompt:

> Write SQL queries to validate that each funnel stage is a subset of the preceding stage.
>
> Also verify that event timestamps follow the correct chronological order.
>
> Return the number of violating users for each stage pair and explain what each violation indicates.

### 4. Falsifiable Hypothesis

**Purpose:** Prevent confirmation bias.

Prompt:

> Hypothesis: [Insert hypothesis].
>
> Define:
>
> * Primary metric.
> * Analytical or statistical test.
> * Sample-size considerations.
> * Assumptions.
> * Expected result.
> * Specific result that would disprove the hypothesis.
>
> Do not interpret or assume the outcome until I provide the actual results.

### 5. Adversarial Review

**Purpose:** Challenge analytical conclusions.

Prompt:

> Here is my analytical finding and the SQL query supporting it.
>
> Identify five possible ways this finding could be misleading or an analytical artifact.
>
> Investigate:
>
> 1. Survivorship bias.
> 2. Reverse causality.
> 3. Join fan-out.
> 4. Time-window bias.
> 5. Imputation-related distortions.
>
> Provide a specific validation test for each possibility.

### 6. Join Fan-Out Guard

**Purpose:** Detect unintended row multiplication during joins.

Prompt:

> Review the following SQL query for potential row multiplication caused by joins.
>
> Identify relationships that could produce duplicate records.
>
> Add COUNT(*) and distinct-key assertions before and after each join.
>
> Explain which joins could inflate revenue, customer counts, or conversion metrics.

### 7. Revenue Impact Sizing

**Purpose:** Translate conversion improvements into estimated financial impact.

Prompt:

> Estimate the revenue impact of a 10% relative improvement at [Insert funnel stage].
>
> Include:
>
> * Current baseline.
> * Expected additional conversions.
> * Revenue per conversion.
> * Low, base, and high scenarios.
> * Every underlying assumption.
> * Whether the estimate represents an annual impact or the entire analysis period.
>
> Clearly distinguish estimated impact from observed revenue.

### 8. Cross-Tool Reconciliation

**Purpose:** Investigate inconsistencies between SQL and Power BI.

Prompt:

> My SQL calculation returns [A], whereas my Power BI DAX measure returns [B].
>
> Identify potential reasons for this discrepancy, ranked by likelihood.
>
> Investigate:
>
> * Filter context.
> * Relationship direction.
> * Active and inactive relationships.
> * Duplicate records.
> * Date filters.
> * DISTINCTCOUNT versus COUNT.
> * Row context versus filter context.
>
> Provide a validation test for each possibility.

### 9. Finding to Business Action

**Purpose:** Convert analytical insights into recommendations.

Prompt:

> Convert each of the following analytical findings into a business recommendation.
>
> Use this structure:
>
> * Finding.
> * Recommended decision.
> * Responsible business owner.
> * Metric to improve.
> * Expected impact range.
> * Assumptions.
> * Risk if the recommendation is incorrect.

### 10. Recruiter Review

**Purpose:** Evaluate the analytical project from an employer's perspective.

Prompt:

> Act as a senior hiring manager recruiting for a Data Analyst position at [Company].
>
> Review this GitHub project against the following job description.
>
> Identify:
>
> 1. Technical credibility risks.
> 2. Analytical methodology weaknesses.
> 3. Unsupported business claims.
> 4. Documentation issues.
> 5. Missing industry-relevant skills.
>
> Rank the problems by severity and suggest specific improvements.

---

## Prompt Execution Log

Every important AI interaction should be documented.

| Prompt          | AI Output Summary                  | Validation Method                          | Decision |
| --------------- | ---------------------------------- | ------------------------------------------ | -------- |
| Defect Audit    | Identified missing customer IDs    | DuckDB NULL checks                         | Accepted |
| Funnel Validity | Generated stage validation queries | SQL execution                              | Modified |
| Revenue Impact  | Estimated potential revenue uplift | Recalculated using actual conversion rates | Rejected |

**Decision definitions:**

* Accepted: AI output was validated and retained.
* Modified: AI output required corrections before use.
* Rejected: AI output was inaccurate, unsupported, or irrelevant.

---

## Analytical Principles

1. AI-generated SQL is not automatically correct.
2. Every important metric must have a clearly defined calculation.
3. Business conclusions must be supported by validated evidence.
4. Correlation must not automatically be interpreted as causation.
5. Estimated financial impact must be separated from observed financial results.
6. Assumptions, limitations, and validation steps must be documented.
7. Final analytical decisions remain the responsibility of the analyst.
