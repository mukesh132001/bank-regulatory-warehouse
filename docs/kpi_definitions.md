# KPI Definitions

Different teams can get different numbers from the same data if a KPI is not
defined precisely. Every KPI in this project has one written definition.

## Active customer

| Definition | Rule | Used by | Model column |
|---|---|---|---|
| **Active (any activity)** | At least one transaction of any kind in the month, including system-posted interest | Operations (accounts that moved) | `active_customers_any_activity` |
| **Active (customer-initiated)** | At least one transaction the customer made themselves (card, ATM, transfer, deposit, bill payment, salary) | Business / marketing / regulatory reporting | `active_customers_customer_initiated` |

**Why they differ:** savings accounts receive monthly interest from the bank's
system even when the customer does nothing. Under the first definition, a
customer who has not touched their account in a year still looks "active."

**Recommended for regulatory reporting:** customer-initiated.

## Dormant customer

No **customer-initiated** transaction in the 90 days before the reporting date.
Customers who have never transacted are measured from their join date, so new
customers are not flagged as dormant.

System transactions (interest) are excluded. The column
`would_be_missed_by_naive_rule` in `mart_dormant_customers` shows how many dormant
customers a naive "no transactions of any kind" rule would have missed.

## Reporting date

All point-in-time KPIs use the dbt variable `reporting_date` (default `2026-06-30`).
