$ErrorActionPreference = 'Stop'
$envPath = Join-Path $PSScriptRoot '.env'
$envValues = @{}

if (Test-Path $envPath) {
  Get-Content $envPath | ForEach-Object {
    if ($_ -match '^\s*#' -or [string]::IsNullOrWhiteSpace($_)) { return }
    $parts = $_ -split '=', 2
    if ($parts.Count -eq 2) {
      $envValues[$parts[0].Trim()] = $parts[1].Trim()
    }
  }
}

$project = if ($envValues.ContainsKey('GCP_PROJECT_ID')) { $envValues['GCP_PROJECT_ID'] } else { 'dataleaguenovaretail' }
$region = if ($envValues.ContainsKey('GCP_REGION')) { $envValues['GCP_REGION'] } else { 'us-south1' }
$topic = if ($envValues.ContainsKey('PUBSUB_TOPIC')) { $envValues['PUBSUB_TOPIC'] } else { 'eventos-realtime' }
$dataset = if ($envValues.ContainsKey('BQ_DATASET')) { $envValues['BQ_DATASET'] } else { 'nR_core_datasets' }
$templateBucket = 'gs://novaretail-dataflow-templates'
$templatePath = "$templateBucket/pubsub-to-bq-streaming.json"
$image = "gcr.io/$project/dataflow/pubsub-to-bq:latest"

Write-Host 'Configuring GCP project...'
gcloud config set project $project

Write-Host 'Creating template bucket...'
gsutil mb $templateBucket --project $project --quiet

Write-Host 'Building Dataflow Flex Template...'
gcloud dataflow flex-template build $templatePath `
  --image-gcr-path=$image `
  --sdk-language=PYTHON `
  --flex-template-base-image=PYTHON3 `
  --metadata-file='dataflow/metadata.json' `
  --py-path='.' `
  --env='FLEX_TEMPLATE_PYTHON_PY_MODULE=dataflow.streaming_pipeline' `
  --env='FLEX_TEMPLATE_PYTHON_PY_FILE=dataflow/streaming_pipeline.py' `
  --project=$project `
  --worker-region=$region

Write-Host 'Launching Dataflow job...'
gcloud dataflow flex-template run "streaming-events-$((Get-Date).ToString('yyyyMMdd-HHmmss'))" `
  --template-file-gcs-location=$templatePath `
  --parameters="project=$project,topic=$topic,output-table=${project}:${dataset}.bronze_events,deadletter-table=${project}:${dataset}.deadletter_events" `
  --region=$region
