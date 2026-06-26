# Genie Space — Metric View Instructions

> Paste this into the **Instructions** field when configuring the
> `mv_clickstream_analytics` Genie Space in Databricks.

---

This space answers questions about the FlexHire marketplace search funnel —
how job seekers search, what they see, and what they click.

## Key Concepts

| Term | Definition |
|---|---|
| **Search** | One search session by a visitor |
| **Impression** | A job listing shown to a visitor in search results |
| **Click** | A visitor clicking on a job listing from search results |
| **CTR** | Click-through rate = clicks ÷ impressions × 100 |
| **Sublocation** | Where a job appeared — `search_results` or `featured_jobs` |

## Available Metrics

- Impressions, clicks, and CTR — by job, category, search query, sublocation, and date
- Time to click — how long a visitor took to click after seeing a job
- Search sessions — unique searches made by visitors
- Client portfolio — how each client's jobs perform in search

## Notes

- CTR is calculated at impression level — one impression results in zero or one click
- Bot traffic is filtered out at the pipeline level before this data is available
- All timestamps are UTC

---

## Sample Questions

### Funnel & Traffic
- How many impressions and clicks happened this week?
- What is the overall click-through rate?
- How has daily traffic trended over the past 30 days?
- What hour of the day sees the most search activity?

### Job Performance
- Which jobs have the most impressions?
- Which jobs have the highest click-through rate?
- Which jobs appear at the top positions most often?
- Show me jobs with high impressions but low CTR.

### Search Queries
- Which search queries drive the most clicks?
- What are the most common searches with zero clicks?
- Which search terms have the highest CTR?

### Categories & Budget
- Which job categories have the highest CTR?
- How does average budget compare across categories?
- Which category gets the most impressions per active job?

### Placement (Sublocation)
- Do featured jobs outperform organic search results in CTR?
- What is the average position for featured jobs vs search results?
- How much faster do visitors click on featured jobs?

### Position Bias
- How does CTR change as position increases?
- What is the average time to click for jobs in position 1 vs position 10?

### Client Performance
- Which clients have the highest click-through rate on their job listings?
- Which clients have the most jobs appearing in search results?
- How does CTR vary across clients in the same category?
- Which clients have the highest total spend and how does that correlate with CTR?

### Freelancer Behaviour
- Which countries have the highest CTR on job listings?
- Do top-rated freelancers have a higher CTR than non-rated ones?
- Which primary skills are most associated with high-CTR searches?
