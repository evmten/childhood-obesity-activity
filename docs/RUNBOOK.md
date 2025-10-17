
# Runbook — Local & (Optional) ADLS

## Prerequisites
- Python 3.10+
- `pip install pandas adlfs pyarrow`
- (Optional) ADLS Gen2 container with raw files present

## Secrets
- **Local/dev:** set SAS via env var `ADLS_SAS` (do not commit).
- **Cloud (future):** prefer Managed Identity; store secrets in Key Vault.

## Commands

### Dry run (validations only)
**PowerShell**
```powershell
$env:ADLS_SAS="?sv=..."
python .\scripts\transform_merge.py `
  --account childactivityobesity `
  --container activity-obesity-data `
  --dry-run
```
**Bash**
```bash
export ADLS_SAS='?sv=...'
python scripts/transform_merge.py \
  --account childactivityobesity \
  --container activity-obesity-data \
  --dry-run
```

## Full run (write processed/curated to ADLS, plus Parquet)
**PowerShell**
```powershell
$env:ADLS_SAS="?sv=..."
python .\scripts\transform_merge.py `
  --account childactivityobesity `
  --container activity-obesity-data `
  --write-parquet
```
**Bash**
```bash
export ADLS_SAS='?sv=...'
python scripts/transform_merge.py \
  --account childactivityobesity \
  --container activity-obesity-data \
  --write-parquet
```

## Local snapshot
**PowerShell**
```powershell
python .\scripts\transform_merge.py `
  --account childactivityobesity `
  --container activity-obesity-data `
  --write-local
