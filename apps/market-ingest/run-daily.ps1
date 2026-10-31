$ErrorActionPreference = 'Stop'
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$dataDir = Join-Path $projectRoot 'data'
New-Item -ItemType Directory -Force -Path $dataDir | Out-Null
$logPath = Join-Path $dataDir 'market-ingest.log'
& py -3 (Join-Path $PSScriptRoot 'update.py') --db (Join-Path $dataDir 'sp500.sqlite') *>> $logPath
exit $LASTEXITCODE
