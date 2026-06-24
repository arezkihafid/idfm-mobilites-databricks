<#
.SYNOPSIS
    Crée (ou met à jour) les Databricks Workflows idfm_referentiel et idfm_frequentation à partir des
    fichiers workflow_idfm_referentiel.json / workflow_idfm_frequentation.json, via l'API Jobs 2.1.

.DESCRIPTION
    Remplace le placeholder <REPO_PATH> dans chaque JSON par le chemin réel du Databricks Repo (cf.
    deploy_to_databricks_repo.ps1 -RepoPath), puis appelle POST /api/2.1/jobs/create.
    Si un job du même nom existe déjà (recherché via /api/2.1/jobs/list), utilise PATCH /reset à la place
    pour le mettre à jour plutôt que d'en créer un doublon.

    Rappel : <A_REMPLACER_node_type_id> et <A_REMPLACER_email> dans les JSON sont à adapter manuellement
    avant le premier déploiement (type d'instance disponible sur le workspace, adresse de notification).

.EXAMPLE
    $env:DATABRICKS_TOKEN = "dapiXXXXXXXX"
    .\deploy_workflows.ps1 -DatabricksHost "https://adb-xxxx.azuredatabricks.net" `
        -RepoPath "/Repos/arezki.hafid@entreprise.fr/idfm-mobilites-databricks"
#>

param(
    [string]$DatabricksHost = $env:DATABRICKS_HOST,
    [string]$DatabricksToken = $env:DATABRICKS_TOKEN,
    [Parameter(Mandatory = $true)] [string]$RepoPath
)

$ErrorActionPreference = "Stop"

if (-not $DatabricksHost) { throw "DatabricksHost manquant (paramètre -DatabricksHost ou variable d'env DATABRICKS_HOST)" }
if (-not $DatabricksToken) { throw "DatabricksToken manquant (paramètre -DatabricksToken ou variable d'env DATABRICKS_TOKEN)" }

$headers = @{ Authorization = "Bearer $DatabricksToken" }
$jobsApi = "$DatabricksHost/api/2.1/jobs"

$existingJobs = Invoke-RestMethod -Uri "$jobsApi/list?limit=100" -Headers $headers -Method Get

function Deploy-Job($jsonFile) {
    $raw = Get-Content -Path $jsonFile -Raw
    $raw = $raw.Replace("<REPO_PATH>", $RepoPath)

    if ($raw -match "<A_REMPLACER_") {
        Write-Warning "$jsonFile contient encore des placeholders <A_REMPLACER_*> non renseignés (node_type_id / email) — déploiement quand même tenté, à corriger si le job échoue."
    }

    $jobDef = $raw | ConvertFrom-Json
    $jobName = $jobDef.name

    $match = $null
    if ($existingJobs.jobs) {
        $match = $existingJobs.jobs | Where-Object { $_.settings.name -eq $jobName }
    }

    if ($match) {
        Write-Host "Job '$jobName' existant (id=$($match.job_id)) -> mise à jour (reset)"
        $body = @{ job_id = $match.job_id; new_settings = $jobDef } | ConvertTo-Json -Depth 20
        Invoke-RestMethod -Uri "$jobsApi/reset" -Headers $headers -Method Post -Body $body -ContentType "application/json"
        Write-Host "OK  $jobName mis à jour (job_id=$($match.job_id))"
    } else {
        Write-Host "Création du job '$jobName'..."
        $body = $jobDef | ConvertTo-Json -Depth 20
        $created = Invoke-RestMethod -Uri "$jobsApi/create" -Headers $headers -Method Post -Body $body -ContentType "application/json"
        Write-Host "OK  $jobName créé (job_id=$($created.job_id))"
    }
}

Deploy-Job (Join-Path $PSScriptRoot "workflow_idfm_referentiel.json")
Deploy-Job (Join-Path $PSScriptRoot "workflow_idfm_frequentation.json")

Write-Host "`nDéploiement des Workflows terminé. Penser à lancer manuellement un premier run de idfm_referentiel"
Write-Host "puis idfm_frequentation (chargement initial 2015-2025) avant de laisser tourner les planifications."
