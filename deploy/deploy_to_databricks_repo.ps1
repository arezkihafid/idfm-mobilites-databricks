<#
.SYNOPSIS
    Pousse le code local (dossier databricks/) vers un dépôt git, puis synchronise le Databricks Repo
    correspondant dans le workspace via l'API REST Repos.

.DESCRIPTION
    Étape 1 (git) : initialise le dépôt git local si nécessaire, commit, et push vers le remote fourni.
    Étape 2 (Databricks) : crée le Repo dans le workspace s'il n'existe pas encore (POST /api/2.0/repos),
    sinon le met à jour sur la dernière branche (PATCH /api/2.0/repos/{id}).

    Compatible GitHub, Azure DevOps et GitLab : c'est l'URL du remote qui détermine le provider, passé
    explicitement en paramètre (Databricks ne le déduit pas toujours correctement seul).

.PARAMETER GitRemoteUrl
    URL du dépôt git distant (ex: https://github.com/<org>/idfm-mobilites-databricks.git).
    Si le remote 'origin' n'existe pas encore localement, ce script l'ajoute.

.PARAMETER GitProvider
    Provider git pour l'API Databricks Repos. Valeurs attendues : gitHub, gitHubEnterprise, azureDevOpsServices,
    gitLab, gitLabEnterpriseEdition, bitbucketCloud, bitbucketServer.

.PARAMETER DatabricksHost
    URL du workspace Databricks (ex: https://adb-xxxx.azuredatabricks.net). Peut aussi être fourni via la
    variable d'environnement DATABRICKS_HOST.

.PARAMETER DatabricksToken
    Personal Access Token Databricks. Peut aussi être fourni via la variable d'environnement DATABRICKS_TOKEN
    (préférable à le passer en paramètre en clair sur la ligne de commande).

.PARAMETER RepoPath
    Chemin du Repo dans le Workspace Databricks (ex: /Repos/prenom.nom@entreprise.fr/idfm-mobilites-databricks).

.PARAMETER Branch
    Branche à synchroniser côté Databricks Repo (par défaut: main).

.EXAMPLE
    $env:DATABRICKS_TOKEN = "dapiXXXXXXXX"
    .\deploy_to_databricks_repo.ps1 `
        -GitRemoteUrl "https://github.com/mon-org/idfm-mobilites-databricks.git" `
        -GitProvider "gitHub" `
        -DatabricksHost "https://adb-xxxx.azuredatabricks.net" `
        -RepoPath "/Repos/arezki.hafid@entreprise.fr/idfm-mobilites-databricks"
#>

param(
    [Parameter(Mandatory = $true)] [string]$GitRemoteUrl,
    [Parameter(Mandatory = $true)] [string]$GitProvider,
    [string]$DatabricksHost = $env:DATABRICKS_HOST,
    [string]$DatabricksToken = $env:DATABRICKS_TOKEN,
    [Parameter(Mandatory = $true)] [string]$RepoPath,
    [string]$Branch = "main"
)

$ErrorActionPreference = "Stop"

if (-not $DatabricksHost) { throw "DatabricksHost manquant (paramètre -DatabricksHost ou variable d'env DATABRICKS_HOST)" }
if (-not $DatabricksToken) { throw "DatabricksToken manquant (paramètre -DatabricksToken ou variable d'env DATABRICKS_TOKEN)" }

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

# --- Étape 1 : git init / commit / push -------------------------------------------------

if (-not (Test-Path ".git")) {
    Write-Host "Initialisation du dépôt git local..."
    git init
    git checkout -b $Branch
}

$currentRemote = git remote get-url origin 2>$null
if (-not $currentRemote) {
    Write-Host "Ajout du remote 'origin' -> $GitRemoteUrl"
    git remote add origin $GitRemoteUrl
} elseif ($currentRemote -ne $GitRemoteUrl) {
    Write-Warning "Le remote 'origin' existant ($currentRemote) diffère de celui fourni ($GitRemoteUrl) — non modifié, vérifie manuellement."
}

# On ne committe que le code Databricks et la doc de cadrage, pas les données sources (volumineuses, non versionnées).
git add databricks/ Atelier_cadrage_questions_IDFM.xlsx 2>$null

$hasChanges = git status --porcelain
if ($hasChanges) {
    git commit -m "Déploiement notebooks IDFM Mobilités $(Get-Date -Format 'yyyy-MM-dd HH:mm')"
} else {
    Write-Host "Aucun changement à committer."
}

Write-Host "Push vers $GitRemoteUrl (branche $Branch)..."
git push -u origin $Branch

# --- Étape 2 : synchronisation du Databricks Repo ---------------------------------------

$headers = @{ Authorization = "Bearer $DatabricksToken" }
$baseUri = "$DatabricksHost/api/2.0/repos"

Write-Host "Recherche d'un Repo existant sur $RepoPath..."
$existing = Invoke-RestMethod -Uri "$baseUri`?path_prefix=$RepoPath" -Headers $headers -Method Get

$repoId = $null
if ($existing.repos) {
    $match = $existing.repos | Where-Object { $_.path -eq $RepoPath }
    if ($match) { $repoId = $match.id }
}

if ($repoId) {
    Write-Host "Repo existant (id=$repoId) -> mise à jour sur la branche $Branch"
    $body = @{ branch = $Branch } | ConvertTo-Json
    Invoke-RestMethod -Uri "$baseUri/$repoId" -Headers $headers -Method Patch -Body $body -ContentType "application/json"
} else {
    Write-Host "Aucun Repo existant -> création sur $RepoPath"
    $body = @{
        url      = $GitRemoteUrl
        provider = $GitProvider
        path     = $RepoPath
    } | ConvertTo-Json
    $created = Invoke-RestMethod -Uri $baseUri -Headers $headers -Method Post -Body $body -ContentType "application/json"
    Write-Host "Repo créé (id=$($created.id))"
}

Write-Host "Déploiement terminé. Notebooks disponibles sous $RepoPath/databricks/..."
