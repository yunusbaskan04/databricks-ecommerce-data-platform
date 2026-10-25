# 0003. Revenue definition for Gold metrics

- **Status:** Accepted
- **Date:** 2026-10-25
- **Decided by:** Yunus Başkan

## Context
The dashboard's headline number is "Total Revenue". Today it is
`sum(price)` over **every** order item, whatever the order's status. Olist has
three money columns and eight order statuses, so "revenue" is a business choice,
not a technical one:

| Column | Grain | Meaning |
|---|---|---|
| `price` | item | product price paid for the item |
| `freight_value` | item | shipping charged for the item |
| `payment_value` | payment | what the customer paid, incl. installment interest and vouchers |

Statuses (approx. order counts): `delivered` 96.5k, `shipped` 1.1k,
`canceled` 0.6k, `unavailable` 0.6k, `invoiced` 0.3k, `processing` 0.3k,
`created` 5, `approved` 2.

Two questions:
1. **Which statuses count?** A canceled order is not money earned.
2. **Which amount?** Is it merchandise only, merchandise + shipping, or cash collected?

## Options — statuses
### A. All statuses (today)
- Pros: matches the current dashboard.
- Cons: counts canceled/unavailable orders as revenue, which overstates it.

### B. Exclude `canceled` and `unavailable`
- Pros: "booked" revenue: everything sold and not lost; standard for e-commerce
  GMV reporting.
- Cons: in-flight orders (`shipped`, `processing`) may still be canceled later.

### C. Only `delivered`
- Pros: most conservative, "realized" revenue.
- Cons: the latest days always look low because recent orders are still in transit.

## Options — amount
### 1. `price` (merchandise value)
- Pros: item grain, so it splits cleanly by category/product; matches today.
- Cons: ignores shipping income.

### 2. `price + freight_value` (GMV incl. shipping)
- Pros: what the customer was charged for the order lines; still item grain.
- Cons: freight is pass-through for many marketplaces, so not "our" revenue.

### 3. `payment_value` (cash collected)
- Pros: matches the money that actually came in.
- Cons: order grain only. It cannot be split by category, so category and daily
  numbers would use different definitions.

## Decision
**Revenue = `sum(price)` over items of orders whose status is not `canceled`
or `unavailable`** (statuses option B, amount option 1).

## Rationale
Booked revenue counts what was sold and not lost, which avoids overstating
revenue with canceled orders, without making the most recent days look empty the
way "delivered only" would. `price` lives at item grain, so the same definition
works for daily, category and product breakdowns. Freight is reported as a
separate metric instead of being mixed into revenue.

## Consequences
- `REVENUE_EXCLUDED_STATUSES` in `src/olist_platform/gold.py` is the single
  place the rule lives; tests pin it.
- Gold item-level tables (`gold_daily_sales`, `gold_category_sales`) apply the
  filter and expose `total_revenue` and `total_freight`.
- `gold_order_status` still shows **all** statuses (order counts and payments
  collected), since its purpose is to show the cancellation share.
- The dashboard's "Total Revenue" drops compared to the old all-status number;
  this is intended.
