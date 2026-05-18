# Data quality challenges in the raw data

| Dataset | Issue | How silver handles it |
|---------|-------|-----------------------|
| `impression_events` / `click_events` | Null `event_id` | `expect_or_drop` removes the row |
| `click_events` | `time_to_click_secs` contains `"instant"`, `"fast"`, and negatives | `regexp_extract` -> null for non-numeric; `expect` warns on negatives |
| `click_events` | Duplicate `event_id` rows | `dropDuplicates(["event_id"])` |
| `click_events` | Future-dated `event_ts` | `expect` warning |
| `click_events` | Null `opening_uid` (organic browse clicks) | Allowed - kept as null |
| `job_openings` | `budget_amount` = `"negotiable"` (text) | `regexp_extract` -> null |
| `job_openings` | Negative `budget_amount` | `expect` warning |
| `job_openings` | Null `title` | `expect_or_drop` removes the row |
| `job_openings` | Future `posted_at` | `expect` warning |
| `clients` | `total_spend_usd` = `"negotiable"` or `"not disclosed"` | `regexp_extract` -> null |
| `clients` | Null `avg_rating` for new accounts | Kept as null |
| `clients` | Future `member_since` date | `expect` warning |
| `freelancers` | `hourly_rate` = `"negotiable"` | `regexp_extract` -> null |
| `freelancers` | `job_success_score` = `"N/A"` for new accounts | `regexp_extract` -> null |
| `freelancers` | Negative `job_success_score` | `expect` warning |
| All | Bot traffic (`is_bot = True`) | `expect_or_drop` removes bots in silver |
