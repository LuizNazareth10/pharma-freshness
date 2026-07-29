# Executa uma DAG de ponta a ponta dentro do container, sem depender do agendador.
#
# `airflow dags test` roda as tarefas na ordem do grafo, no processo atual, e imprime tudo no
# terminal. E a forma mais rapida de validar uma DAG: nao exige despausar, nem esperar o
# horario agendado, nem caçar log no console web.

param(
    [ValidateSet("diario", "semanal")]
    [string]$Dag = "diario",

    [string]$Tarefa
)

$ErrorActionPreference = "Stop"

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $projectRoot

$dagId = "pipeline_farmacovigilancia_$Dag"

if ($Tarefa) {
    Write-Host "Testando a tarefa '$Tarefa' da DAG '$dagId'..." -ForegroundColor Cyan
    docker compose --profile orquestracao exec -T airflow-scheduler `
        airflow tasks test $dagId $Tarefa
} else {
    Write-Host "Executando a DAG '$dagId' inteira..." -ForegroundColor Cyan
    Write-Host "A ingestao real pode levar alguns minutos." -ForegroundColor Yellow
    docker compose --profile orquestracao exec -T airflow-scheduler `
        airflow dags test $dagId
}

if ($LASTEXITCODE -ne 0) { throw "A execucao falhou. Veja o log acima." }

Write-Host "`nExecucao concluida com sucesso." -ForegroundColor Green
