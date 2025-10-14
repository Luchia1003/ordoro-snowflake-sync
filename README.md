# ordoro-snowflake-sync
Automated job to sync Ordoro API inventory data into Snowflake

This scheduled job fetches inventory and product data from the **Ordoro API**
and writes it into **Snowflake** tables:

- `INVENTORY_PRODUCT_LEVEL`
- `INVENTORY_WAREHOUSE_LEVEL`

## Deployment

This repo is configured for Render’s **Cron Job** service.
It installs dependencies, then runs `ordoro_to_snowflake.py` on a daily schedule.

### Environment Variables
Set the following on Render:

| Variable | Description |
|-----------|--------------|
| `ORDORO_CLIENT_ID` | Ordoro API credential |
| `ORDORO_CLIENT_SECRET` | Ordoro API credential |
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier |
| `SNOWFLAKE_USER` | Snowflake username |
| `SNOWFLAKE_PASSWORD` | Snowflake password |
| `SNOWFLAKE_ROLE` | Optional: `ACCOUNTADMIN` |
| `SNOWFLAKE_WAREHOUSE` | e.g., `COMPUTE_WH` |
| `SNOWFLAKE_DATABASE` | e.g., `SKU_PROFIT_PROJECT` |
| `SNOWFLAKE_SCHEMA` | e.g., `ORDORO_API` |

## Manual Run (local)
```bash
pip install -r requirements.txt
python ordoro_to_snowflake.py
