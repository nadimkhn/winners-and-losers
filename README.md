# US Stock Market Daily Winners & Losers

### Summary

This project demonstrates an end-to-end data pipeline and dashboard for monitoring daily US stock market activity.

### Core features:

- Automated data ingestion from the Polygon.io REST API, scheduled to pull daily aggregate market data
- In-database transformation using PostgreSQL, where all key calculations - daily percent change, moving averages, advancers/decliners ratios are handled via SQL for maximum performance and reproducibility
- Transformed data is then exported via Python to Google Sheets, which acts as the data source for the final visualization layer
- Looker Studio dashboard built on top of the Google Sheet to present dynamic, interactive visualizations and KPIs related to market performance

This project showcases skills across API integration, data engineering, analytical SQL, and business intelligence tools, with a lean and modular workflow.

### Technologies Used

- Python: requests, pandas, psycopg2
- PostgreSQL for transformation and storage
- Polygon.io API for financial market data
- Google Sheets as a final delivery layer for dashboard connectivity
- Looker Studio for dashboarding and reporting

### Demo Dashboard

[View the Looker Studio Dashboard ->](https://lookerstudio.google.com/reporting/943a9115-1031-49d1-a6ee-d2cf838a7afe)

Includes:

- Daily market return scorecard (dynamic to selected time intervals)
- Top 5 gainers and losers by % change
- Market sentiment based on Advanced & Decline Ratio
- Tornado chart with average daily % change by industry to assess sector performance
- Superimposed Time-Series Chart showing Close, MA-03, and MA-05

### Additional Notes

- All metric calculations are SQL-based, enabling a lean and scalable backend
- Google Sheet updates with each script run, maintaining a clean decoupling of ETL and dashboard layers
- Dashboard visuals are filterable by time and ticker, with calculated fields adjusting accordingly