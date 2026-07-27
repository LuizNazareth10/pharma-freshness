# Fase 1 — Fundação (Dias 1–2)

Ao terminar este laboratório, você conseguirá explicar e demonstrar:

- como o Docker Compose transforma configuração em um container reproduzível;
- por que a API S3 (porta 9000) e o console MinIO (porta 9001) têm funções diferentes;
- como bucket, prefixo e objeto representam dados em object storage;
- como navegar manualmente nas páginas do DailyMed e do FAERS;
- por que schema, paginação, nulabilidade e rate limit precisam ser entendidos antes de escrever uma ingestão.

## Modelo mental do ambiente

```text
seu navegador ── http://localhost:9001 ──> console web MinIO
seu código     ── http://localhost:9000 ──> API compatível com S3
                                                  │
                                                  ▼
                                      volume Docker persistente
                                      pharma-freshness-minio-data
```

O container é o processo isolado que executa o MinIO. A imagem é o molde imutável desse processo. O volume guarda os objetos fora da camada descartável do container. Por isso recriar o container não apaga o bucket, mas remover o volume apaga.

## Dia 1 — MinIO com Docker

### 1. Pré-requisitos

No PowerShell, confirme:

```powershell
docker --version
docker compose version
docker info
```

Se `docker info` falhar, inicie o Docker Desktop e espere o engine ficar pronto.

O projeto usa `docker compose` (Compose v2). O antigo `docker-compose` com hífen expressa a mesma ideia, mas é a CLI legada.

### 2. Credenciais locais

O Compose lê variáveis do arquivo `.env`. Caso ele não exista:

```powershell
Copy-Item .env.example .env
```

As credenciais fornecidas são somente para a máquina local e `.env` está no `.gitignore`. Nunca reutilize essa senha ou publique `.env`. Para alterá-la, edite o arquivo antes da primeira inicialização.

### 3. Entenda o `docker-compose.yml`

O serviço principal contém:

- uma imagem MinIO fixada em uma versão, garantindo reprodutibilidade;
- o comando `server /data --console-address ":9001"`;
- mapeamento das portas 9000 (API) e 9001 (console);
- um volume nomeado montado em `/data`;
- um health check que só marca o serviço saudável quando a API responde;
- uma rede própria que será reutilizável pelas próximas fases.

Os serviços `minio-bootstrap` e `minio-verify` estão no profile `automation`. Eles **não** sobem no comando normal e, portanto, não tiram de você o exercício manual.

Valide a configuração resolvida sem iniciar containers:

```powershell
docker compose config
```

### 4. Inicie o MinIO

Comando didático direto:

```powershell
docker compose up -d minio
```

Ou o auxiliar, que também aguarda o health check:

```powershell
.\scripts\day1\Start-MinIO.ps1
```

O `up` cria/inicia os recursos; `-d` deixa o processo em segundo plano. Inspecione:

```powershell
docker compose ps
docker compose logs -f minio
```

Saia dos logs com `Ctrl+C`; isso não para o container.

### 5. Crie o bucket e envie o objeto pela interface

1. Abra <http://localhost:9001>.
2. Entre com `MINIO_ROOT_USER` e `MINIO_ROOT_PASSWORD` definidos em `.env`.
3. No menu **Buckets**, selecione **Create Bucket**.
4. Use exatamente `farmacovigilancia` e confirme.
5. Abra o bucket e crie a pasta `laboratorio` (em S3 ela é, tecnicamente, um prefixo).
6. Selecione **Upload** e envie `samples/minio/arquivo-teste.json` para esse prefixo.
7. Confirme que aparece como `laboratorio/arquivo-teste.json`; abra os detalhes e observe tamanho, data e ETag.

Um bucket é o contêiner lógico superior. Um objeto é composto por chave, bytes e metadados. A chave completa do teste é `laboratorio/arquivo-teste.json`. Object storage não possui diretórios reais: `/` é apenas parte do nome e a interface o apresenta como pasta.

### 6. Valide sem depender da interface

```powershell
.\scripts\day1\Test-MinIO.ps1
```

O teste verifica três coisas: endpoint saudável, bucket acessível e objeto existente. O cliente se conecta pela rede Docker a `http://minio:9000`; do Windows, você usa `http://localhost:9000`.

Se quiser reproduzir automaticamente o exercício manual (por exemplo, após limpar o volume):

```powershell
.\scripts\day1\Initialize-MinIO.ps1
.\scripts\day1\Test-MinIO.ps1
```

O bootstrap é idempotente: `mc mb --ignore-existing` aceita um bucket já existente, e o upload da mesma chave substitui o objeto de teste de forma determinística.

### 7. Parar, reiniciar e entender persistência

```powershell
docker compose stop minio       # para, preserva container e volume
docker compose start minio      # reinicia o mesmo container
docker compose down             # remove container/rede, preserva o volume nomeado
docker compose up -d minio      # recria; os objetos continuam no volume
```

Somente quando quiser apagar deliberadamente todos os objetos locais:

```powershell
docker compose down -v
```

`down -v` é destrutivo: remove `pharma-freshness-minio-data`, incluindo buckets e objetos.

## Dia 2 — Exploração manual das APIs

Este dia não grava nada no MinIO e não é uma ingestão. As respostas são pequenas amostras locais em `.local/api-samples/`, ignoradas pelo Git.

### 1. DailyMed no navegador

Abra:

```text
https://dailymed.nlm.nih.gov/dailymed/services/v2/spls.json?pagesize=5&page=1
```

Localize `data` e `metadata`. Depois troque `page=1` por `page=2` e observe `current_page`, `previous_page_url` e `next_page_url`.

Perguntas para responder:

- O identificador parece ser `setid` ou o título?
- `published_date` é uma data JSON ou texto?
- Como você sabe que existe outra página?

### 2. FAERS no navegador

Abra a primeira página:

```text
https://api.fda.gov/drug/event.json?limit=5&skip=0
```

Depois abra a segunda:

```text
https://api.fda.gov/drug/event.json?limit=5&skip=5
```

Compare `meta.results.skip`, `limit` e `total`. Explore `results[].patient.reaction` e `results[].patient.drug`: ambos são arrays, portanto um relato não equivale automaticamente a uma única combinação droga–reação.

### 3. Faça o laboratório reproduzível

O script executa somente quatro requisições: duas páginas de cada fonte. Ele guarda a resposta e cria `summary.json` com os números de paginação.

```powershell
.\scripts\day2\Explore-PharmaApis.ps1
```

Opções úteis:

```powershell
# Amostras um pouco maiores
.\scripts\day2\Explore-PharmaApis.ps1 -DailyMedPageSize 10 -FaersPageSize 10

# Com uma chave openFDA, sem registrá-la em arquivo
$OpenFdaKey = Read-Host 'API key openFDA'
.\scripts\day2\Explore-PharmaApis.ps1 -ApiKey $OpenFdaKey
Remove-Variable OpenFdaKey
```

Leia os JSONs gerados e confronte-os com [api-schema-notes.md](api-schema-notes.md). As notas separam tipo observado de contrato garantido.

### 4. Rate limiting com uso responsável

Em 27/07/2026, a documentação oficial do openFDA informa:

- sem chave: 240 requisições/minuto por IP e 1.000/dia por IP;
- com chave: 240 requisições/minuto por chave e 120.000/dia por chave.

Isso substitui a referência antiga de 40 requisições/minuto no documento de fundação. Limites podem mudar; confira a documentação oficial antes do experimento.

O teste não roda por padrão, pois consome uma parte relevante da cota. Quando decidir executá-lo, feche outros consumidores da API e rode uma única vez:

```powershell
.\scripts\day2\Test-OpenFdaRateLimit.ps1 -Requests 245 -Concurrency 20 -ConfirmTraffic
```

O script usa concorrência limitada porque chamadas sequenciais lentas talvez nunca ultrapassem 240/minuto. O resultado esperado é uma sequência de HTTP 200 seguida possivelmente de HTTP 429 (`Too Many Requests`). A posição exata pode variar por janela, IP compartilhado ou política do serviço. Registre também o header `Retry-After`, quando presente. Não aumente a carga repetidamente para “forçar” o erro.

Em um pipeline futuro, a reação correta a 429 será aguardar (`Retry-After`), aplicar backoff exponencial com jitter e tentar novamente de forma limitada — nunca um loop agressivo.

### 5. O que anotar antes de programar a ingestão

Para cada fonte, registre:

1. endpoint e cadência;
2. chave candidata e grão aparente;
3. mecanismo de paginação;
4. formato de cada data;
5. campos opcionais, nulos e arrays;
6. limites por chamada/minuto/dia;
7. decisões ainda não comprovadas.

O levantamento inicial está em [api-schema-notes.md](api-schema-notes.md). Ele deixa perguntas em aberto quando a amostra não é evidência suficiente.

## Critério de conclusão

Você concluiu a Fase 1 quando consegue, sem copiar definições:

- subir, inspecionar, parar e recriar o MinIO;
- explicar por que os dados sobrevivem a `docker compose down` mas não a `down -v`;
- localizar o objeto pela interface e pelo teste automatizado;
- avançar uma página em cada API;
- apontar pelo menos um campo nulo, uma data textual e um array aninhado;
- explicar o que seu código futuro deve fazer ao receber HTTP 429;
- dizer em uma frase: **“Ainda não construí ingestão; primeiro validei a infraestrutura e aprendi o formato e os limites das fontes.”**

## Solução de problemas

### Docker Engine indisponível

Sintoma: erro mencionando `dockerDesktopLinuxEngine` ou `npipe`.

Solução: inicie o Docker Desktop, espere o status “running” e execute `docker info`.

### Porta 9000 ou 9001 já está em uso

Descubra o processo:

```powershell
Get-NetTCPConnection -LocalPort 9000,9001 -ErrorAction SilentlyContinue
```

Ou altere `MINIO_API_PORT`/`MINIO_CONSOLE_PORT` em `.env`. Se usar outras portas, ajuste também as URLs acessadas pelo Windows; dentro da rede Docker, o MinIO continua em `minio:9000`.

### Container iniciou, mas está unhealthy

```powershell
docker compose ps
docker compose logs --tail 100 minio
```

Confira senha (mínimo aceito pelo MinIO), espaço em disco e conflitos de porta.

### `Test-MinIO.ps1` não encontra o objeto

Confirme nome e chave exatos: bucket `farmacovigilancia`, objeto `laboratorio/arquivo-teste.json`. Se você subiu o arquivo na raiz, mova/refaça o upload ou use o bootstrap automatizado.

## Referências oficiais

- [MinIO Object Store — documentação](https://docs.min.io/)
- [DailyMed REST API `/spls`](https://dailymed.nlm.nih.gov/dailymed/webservices-help/v2/spls_api.cfm)
- [openFDA — autenticação e limites](https://open.fda.gov/apis/authentication/)
- [openFDA drug event — uso do endpoint](https://open.fda.gov/apis/drug/event/how-to-use-the-endpoint/)
