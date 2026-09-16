# GridPulse EU — Step 1: ingestion infra

Week 1 of the roadmap: get real ENTSO-E data landing daily in an ADLS Gen2 "bronze"
container, via an Azure Function on your pay-as-you-go subscription.

## Do this first, today — it's the long pole

ENTSO-E approval typically takes up to **3 business days**, and nothing else here
is blocked on it, so kick it off before anything else:

1. Register at <https://transparency.entsoe.eu> → **Sign In → Register**, then verify
   your account via the confirmation email.
2. Email **transparency@entsoe.eu** with subject `RESTful API access` and your
   registered email address in the body.
3. Once approved (you'll get an email), log in → **My Account** → generate a
   **Web API security token**. Treat it like a password — never commit it to git.

## While you wait: deploy the infrastructure

Prerequisites: [Azure CLI](https://learn.microsoft.com/cli/azure/install-azure-cli)
logged in against your pay-as-you-go subscription (`az login`), and
[Azure Functions Core Tools v4](https://learn.microsoft.com/azure/azure-functions/functions-run-local)
plus Python 3.11 for the next step.

```bash
az group create --name rg-gridpulse --location westeurope

az deployment group create \
  --resource-group rg-gridpulse \
  --template-file infra/main.bicep \
  --parameters alertEmail=you@example.com monthlyBudgetAmount=20 entsoeToken=pending
```

If you don't have the ENTSO-E token yet, deploy with `entsoeToken=pending` as above —
redeploying later with the real token is idempotent and takes under two minutes:

```bash
az deployment group create \
  --resource-group rg-gridpulse \
  --template-file infra/main.bicep \
  --parameters alertEmail=you@example.com monthlyBudgetAmount=20 entsoeToken=YOUR_REAL_TOKEN
```

This creates:

- a **Linux consumption-plan Function App** (Python 3.11) — pay only per execution
- a **separate ADLS Gen2 storage account** with `bronze` / `silver` / `gold` / `export`
  containers (kept separate from the Function's own runtime storage)
- **Application Insights** for logs and monitoring
- a **system-assigned managed identity** on the Function App, granted
  `Storage Blob Data Contributor` on the data lake — no connection strings or
  storage keys in the ingestion code
- a **monthly budget alert** at 80% and 100% of `monthlyBudgetAmount`, emailed
  to `alertEmail`

Note the `dataLakeAccountUrl` and `functionAppName` from the deployment output —
you'll need them next.

## Deploy the function code

```bash
cd ingestion
func azure functionapp publish <functionAppName-from-deployment-output> --python
```

Then set the two values the code needs (skip `ENTSOE_TOKEN` if you already passed
the real one into the Bicep deployment above):

```bash
az functionapp config appsettings set \
  --name <functionAppName> --resource-group rg-gridpulse \
  --settings ENTSOE_TOKEN=YOUR_REAL_TOKEN
```

## Test it

```bash
az functionapp function invoke \
  --name <functionAppName> --resource-group rg-gridpulse \
  --function-name entsoe_daily_ingest
```

Then check the `bronze` container (Azure Storage Explorer, or Portal → your data
lake account → Containers → bronze) for files like:

```
entsoe/prices/dt=2026-09-10/NL.xml
entsoe/generation/dt=2026-09-10/BE.xml
```

If a country/day shows nothing, check Application Insights → Logs for the
warning/error the function logged — it fails per-country, not all-or-nothing.

## What's deliberately deferred (say so in the README when you write it up)

- `ENTSOE_TOKEN` sits in a Function App setting, not Key Vault — reasonable for a
  sandbox you'll tear down within weeks; call out Key Vault as the production fix.
- No retry/backoff beyond a per-country try/except — fine at this scale, worth
  mentioning as a known gap if asked in an interview.
- The zip-vs-XML branch in `_land()` is defensive; you'll find out which shape
  ENTSO-E actually returns once you're parsing real files in Databricks (Week 2).

## Cost control

Storage costs here are near-zero. The only real cost driver in the whole GridPulse
build is the Databricks cluster you'll add in Week 2 — keep it as a separate,
explicit step with auto-terminate configured from the moment you create it, not
something that inherits from this deployment.

**Next:** once this is landing raw files daily, move to Week 2 in the roadmap —
the Azure Databricks Bronze → Silver → Gold transform layer.
