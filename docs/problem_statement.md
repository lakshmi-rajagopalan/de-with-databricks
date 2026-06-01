# Problem Statement

## The Company

FlexHire is a freelance job marketplace. Clients post jobs, freelancers apply. In between, the platform's best-match algorithm decides which jobs to show, in what order, to which visitors — and whether any of them click.

The best-match algorithm is the primary acquisition channel. If it works, freelancers find roles and clients hire. If it doesn't, both churn.

---

## The Problem

The head of product has three open questions she cannot answer:

**1. Is the best-match algorithm surfacing the right jobs?**
Every day, thousands of job listings are shown to visitors. Most are ignored. A small number get clicked. Nobody knows whether the ones getting clicked are the right ones — or whether the ones being ignored are actually better matches that just happen to be ranked lower.

**2. Are featured listings worth the premium?**
Clients can pay to have their jobs appear in a featured placement above the organic results. The sales team sells this as "guaranteed visibility." But nobody has measured whether featured placements actually drive more clicks, or whether clients are paying for impressions that go nowhere.

**3. Which clients are at risk?**
Some clients post jobs, get plenty of impressions, and receive zero clicks. That's a signal — either their job posts are poorly written, their budgets are uncompetitive, or the platform is showing them to the wrong audience. The account management team has no way to identify these clients before they cancel.

---

## The Data

The platform captures every interaction a visitor has with the best-match algorithm. You have access to one day of this data.

**Search sessions** — every time a visitor submits a query, a record is created capturing the query text, how results were ranked, and which page of results they were on.

**Impressions** — every time a job listing was shown to a visitor, a record is created capturing which listing, at which position on the page, and in which search session.

**Clicks** — every time a visitor clicked a listing, a record is created capturing which listing they clicked, how long after the impression it took, and the position it appeared at.

Alongside the event data, you have the underlying entity records: job postings with their categories and budgets, client company profiles, and freelancer profiles linked to visitor activity.

---

## What Good Looks Like

The head of product should be able to open a dashboard and immediately see:

- Click-through rate by job category, by search query, by position on the page
- A direct comparison of featured vs organic listing performance
- A list of clients ranked by impression volume with low CTR — her at-risk account list

The account management team should be able to ask plain-English questions about client performance without waiting for a data analyst to write a query.

---

## Your Task

The data exists. The questions are clear. Nothing has been built yet.

You will figure out how.
