# Data Vault com Apache Spark, dbt e Trino

Pipeline de dados moderno utilizando arquitetura **Data Vault 2.0** para modelagem de um dataset de e-commerce customer analytics. A ingestão é feita via **Apache Spark**, armazenamento em **Parquet** sobre **MinIO (S3)**, e as transformações Data Vault são orquestradas pelo **dbt** executando queries no **Trino**.

---

## Índice

- [Visão Geral da Arquitetura](#visão-geral-da-arquitetura)
- [Tecnologias](#tecnologias)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Pré-requisitos](#pré-requisitos)
- [Setup e Instalação](#setup-e-instalação)
- [Executando o Pipeline](#executando-o-pipeline)
- [Idempotência do Pipeline](#idempotência-do-pipeline)
- [Scripts de Reset](#scripts-de-reset)
- [Modelagem Data Vault](#modelagem-data-vault)
- [Detalhamento das Camadas](#detalhamento-das-camadas)
- [Dataset](#dataset)
- [Acessando os Dados](#acessando-os-dados)
- [Conectando via IDE (DataGrip, DBeaver, etc.)](#conectando-via-ide-datagrip-dbeaver-vs-code-etc)
- [Troubleshooting](#troubleshooting)

---

## Visão Geral da Arquitetura

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           PIPELINE DE DADOS                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────┐    ┌──────────────┐    ┌─────────────┐    ┌───────────┐  │
│  │  CSV    │───▶│ Apache Spark │───▶│   Parquet   │───▶│   Trino   │  │
│  │ (data/) │    │  (Ingestão)  │    │  (MinIO/S3) │    │  (Query)  │  │
│  └─────────┘    └──────────────┘    └─────────────┘    └─────┬─────┘  │
│                                                               │        │
│                                                               ▼        │
│                                                         ┌───────────┐  │
│                                                         │    dbt    │  │
│                                                         │(Transform)│  │
│                                                         └─────┬─────┘  │
│                                                               │        │
│                      ┌────────────────────────────────────────┼────┐   │
│                      │           DATA VAULT 2.0               │    │   │
│                      │                                        ▼    │   │
│                      │  ┌──────────┐  ┌────────────┐  ┌────────┐  │   │
│                      │  │   Hubs   │  │ Satellites │  │ Links  │  │   │
│                      │  └──────────┘  └────────────┘  └────────┘  │   │
│                      └─────────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Fluxo de Dados

1. **Ingestão (Spark)** — Lê o CSV de customers e persiste como tabela Parquet no MinIO via Hive Metastore
2. **Staging (dbt/Trino)** — Calcula hash keys, hash diffs e prepara os dados para o Data Vault
3. **Raw Vault (dbt/Trino)** — Transforma em Hubs, Satellites e Links seguindo o padrão Data Vault 2.0

---

## Tecnologias

| Componente | Tecnologia | Propósito |
|---|---|---|
| Ingestão | Apache Spark 4.0 (Spark Connect) | Leitura de CSV e escrita em Parquet via Hive Metastore |
| Armazenamento | Parquet + MinIO | Data lakehouse com object storage compatível S3 |
| Metastore | Hive Metastore + PostgreSQL | Catálogo de tabelas compartilhado entre Spark e Trino |
| Query Engine | Trino | Motor SQL distribuído para leitura/transformação dos dados |
| Transformação | dbt (dbt-trino) | Orquestração de modelos SQL com lineage e testes |
| Streaming | Apache Kafka + Schema Registry | Infraestrutura de streaming (extensível) |
| Observabilidade | Apache Superset | Visualização e dashboards |
| Object Storage | MinIO (compatível S3) | Armazenamento de arquivos Parquet |

---

## Estrutura do Projeto

```
spark_dbt_datavault_trino/
├── .devcontainer/              # Container de desenvolvimento
│   ├── Dockerfile
│   └── devcontainer.json
├── data/                       # Dados fonte (CSV)
│   ├── ecommerce_customer_analytics.csv
│   ├── data_dictionary.csv
│   └── dataset-metadata.json
├── src/                        # Scripts Python/Spark
│   └── ingest_raw_datavault.py # Ingestão CSV → Parquet/MinIO
├── dbt_datavault/              # Projeto dbt (Data Vault)
│   ├── dbt_project.yml         # Configuração do projeto dbt
│   ├── profiles.yml            # Conexão com Trino (catálogo hive)
│   ├── models/
│   │   ├── staging/
│   │   │   └── stg_customers.sql       # Staging com hash keys
│   │   ├── raw_vault/
│   │   │   ├── hubs/
│   │   │   │   ├── hub_customer.sql    # Hub de Clientes
│   │   │   │   ├── hub_country.sql     # Hub de Países
│   │   │   │   └── hub_category.sql    # Hub de Categorias
│   │   │   ├── satellites/
│   │   │   │   ├── sat_customer_details.sql  # Dados demográficos
│   │   │   │   └── sat_customer_metrics.sql  # Métricas transacionais
│   │   │   └── links/
│   │   │       ├── link_customer_country.sql   # Cliente ↔ País
│   │   │       └── link_customer_category.sql  # Cliente ↔ Categoria
│   │   └── schema.yml          # Documentação e testes
│   └── macros/                 # Macros reutilizáveis
├── spark-connect/              # Configuração do Spark Connect
│   ├── conf/
│   │   ├── core-site.xml
│   │   └── spark-defaults.conf
│   └── Dockerfile
├── hive-metastore/             # Hive Metastore
│   ├── conf/
│   │   └── metastore-site.xml
│   └── Dockerfile
├── trino/                      # Configuração do Trino
│   └── catalog/
│       ├── hive.properties
│       └── iceberg.properties
├── .env                        # Variáveis de ambiente
├── docker-compose.yml          # Orquestração de containers
├── Makefile                    # Comandos automatizados
├── reset.ps1                   # Reset completo (Windows 11)
├── reset.sh                    # Reset completo (Linux/macOS)
├── pyproject.toml              # Dependências Python (uv)
└── requirements.txt            # Dependências congeladas
```

---

## Pré-requisitos

- **Docker** e **Docker Compose** instalados
- Mínimo de **8 GB de RAM** disponível para os containers
- Portas disponíveis:
  - `9000`, `9001` (MinIO)
  - `8080` (Trino)
  - `9083` (Hive Metastore)
  - `15002` (Spark Connect)
  - `9092` (Kafka)
  - `8088` (Superset)
  - `4040` (Spark UI)
  - `5432` (PostgreSQL)

---

## Setup e Instalação

### 1. Subir a infraestrutura

```bash
make up
```

Isso inicializa todos os containers: MinIO, PostgreSQL, Hive Metastore, Spark Connect, Trino, Kafka, Schema Registry, Superset.

### 2. Aguardar os serviços ficarem saudáveis

Os health checks garantem a ordem correta de inicialização:
- PostgreSQL → Hive Metastore → Trino / Spark Connect

Verifique com:

```bash
docker-compose ps
```

### 3. Executar o pipeline Data Vault

```bash
make dv-pipeline
```

Ou passo a passo:

```bash
# Passo 1: Ingerir CSV para tabela raw no MinIO
make dv-ingest

# Passo 2: Executar modelos dbt (staging → hubs → satellites → links)
make dv-run

# Passo 3: Validar integridade dos dados
make dv-test
```

---

## Executando o Pipeline

### Comandos Disponíveis

| Comando | Descrição |
|---|---|
| `make up` | Inicia todos os containers |
| `make down` | Para todos os containers |
| `make dv-ingest` | Spark ingere CSV → tabela raw Parquet/MinIO |
| `make dv-run` | dbt executa modelos Data Vault |
| `make dv-test` | dbt executa testes de integridade |
| `make dv-pipeline` | Pipeline completo (ingest + transform) |
| `make logs` | Visualizar logs dos containers |

### Execução Manual (dentro do container dev)

```bash
# Acessar container de desenvolvimento
docker-compose exec dev bash

# Ingestão
python src/ingest_raw_datavault.py

# dbt
cd dbt_datavault
dbt run --profiles-dir .
dbt test --profiles-dir .
```

---

## Idempotência do Pipeline

O pipeline foi desenhado para ser **idempotente** — executar `make dv-pipeline` múltiplas vezes produz o mesmo resultado sem duplicação de dados ou perda de informação.

### Princípios aplicados

| Camada | Estratégia | Garantia |
|---|---|---|
| **Ingestão (Spark)** | `mode("overwrite")` atômico | Re-execução substitui dados raw sem janela de perda (sem `DROP` + `INSERT`) |
| **Hubs** | `incremental` + `NOT IN` | Só insere business keys que ainda não existem na tabela |
| **Satellites** | `incremental` + `NOT EXISTS` no hash diff | Só insere se houve mudança nos atributos (acumula histórico) |
| **Links** | `incremental` + `NOT IN` | Só insere relacionamentos novos |

### O que NÃO é idempotente (e por quê evitamos)

```
❌ DROP TABLE → INSERT  (perde dados se falhar entre os dois comandos)
❌ TRUNCATE → INSERT    (mesmo problema)
❌ materialized='table'  (recria tudo, perde histórico dos satellites)
```

### Comportamento em múltiplas execuções

```
1ª execução: insere 1200 customers em todos os modelos (full load)
2ª execução: 0 inserções (tudo já existe com mesmo hash)
Nª execução: 0 inserções (idempotente)

Se o CSV mudar (ex: cliente mudou de país):
→ Hub: não insere (customer_id já existe)
→ Satellite: insere nova linha (hash diff mudou) ← HISTORICIDADE
→ Link: insere novo link customer↔country (novo relacionamento)
```

### Por que `incremental` + `append` e não `merge`?

O connector Hive do Trino não suporta `MERGE`. A estratégia `append` com filtro `NOT IN` / `NOT EXISTS` alcança o mesmo resultado:
- Não duplica dados em re-execuções
- Preserva histórico nos satellites
- É compatível com qualquer connector Trino

---

## Scripts de Reset

Scripts para destruir e recriar o ambiente completo do zero.

### Windows 11 (PowerShell)

```powershell
.\reset.ps1
```

### Linux / macOS (Bash)

```bash
chmod +x reset.sh
./reset.sh
```

### O que os scripts fazem

1. `docker-compose down -v --remove-orphans` — Para containers e remove volumes
2. `rm -rf ./minio_data` — Limpa dados do MinIO local
3. `docker-compose up -d --build` — Reconstrói e sobe tudo
4. Aguarda Trino ficar acessível (health check com retry)
5. Executa pipeline completo: `ingest → dbt run → dbt test`

> ⚠️ **Atenção**: Os scripts destroem TODOS os dados. Use apenas para reset completo do ambiente.

---

## Modelagem Data Vault

### O que é Data Vault 2.0?

Data Vault é uma metodologia de modelagem para data warehouses que prioriza:

- **Auditabilidade** — todo dado tem `load_datetime` e `record_source`
- **Historicidade** — satellites rastreiam mudanças ao longo do tempo
- **Flexibilidade** — novos fontes/entidades são adicionados sem quebrar o modelo existente
- **Paralelismo** — hubs, satellites e links podem ser carregados de forma independente

### Componentes

```
┌─────────────────────────────────────────────────────────┐
│                      DATA VAULT                          │
│                                                         │
│  ┌─────────────┐       ┌──────────────────┐            │
│  │ HUB_CUSTOMER│◄──────│ SAT_CUSTOMER_    │            │
│  │             │       │ DETAILS          │            │
│  │ • hk_customer│       │ • age, gender    │            │
│  │ • customer_id│       │ • income_bracket │            │
│  └──────┬──────┘       │ • loyalty_tier   │            │
│         │              └──────────────────┘            │
│         │                                              │
│         │              ┌──────────────────┐            │
│         │◄─────────────│ SAT_CUSTOMER_    │            │
│         │              │ METRICS          │            │
│         │              │ • total_orders   │            │
│         │              │ • total_spent    │            │
│         │              │ • churn          │            │
│         │              └──────────────────┘            │
│         │                                              │
│    ┌────┴────────────┐       ┌─────────────┐          │
│    │ LINK_CUSTOMER_  │──────▶│ HUB_COUNTRY │          │
│    │ COUNTRY         │       │             │          │
│    └────┬────────────┘       │ • hk_country│          │
│         │                    │ • country   │          │
│         │                    └─────────────┘          │
│    ┌────┴────────────┐       ┌──────────────┐         │
│    │ LINK_CUSTOMER_  │──────▶│ HUB_CATEGORY │         │
│    │ CATEGORY        │       │              │         │
│    └─────────────────┘       │ • hk_category│         │
│                              │ • category   │         │
│                              └──────────────┘         │
└─────────────────────────────────────────────────────────┘
```

### Padrões Implementados

| Padrão | Implementação |
|---|---|
| **Hash Keys** | SHA-256 sobre business keys (determinístico, reproduzível) |
| **Hash Diff** | SHA-256 concatenando atributos do satellite (detecta mudanças) |
| **Incremental Load** | Satellites só inserem se `hd_*` mudou; Hubs/Links verificam existência |
| **Metadata** | `load_datetime` (timestamp da carga) + `record_source` (origem) |
| **Separação de responsabilidades** | Hubs = identidade, Satellites = contexto, Links = relacionamentos |
| **Idempotência** | Re-execuções não duplicam dados nem perdem histórico |

---

## Detalhamento das Camadas

### Camada Raw (Spark → Hive/MinIO)

O script `src/ingest_raw_datavault.py`:
- Conecta ao Spark via Spark Connect (gRPC na porta 15002)
- Lê o CSV com inferência de schema
- Adiciona colunas de metadata (`load_datetime`, `record_source`)
- Cria database `raw_vault` no Hive Metastore (idempotente com `IF NOT EXISTS`)
- Persiste como tabela Parquet no MinIO (`s3a://warehouse/raw_vault.db/`)
- Usa `mode("overwrite")` atômico — sem `DROP TABLE` prévio

### Camada Staging (dbt)

O modelo `stg_customers.sql`:
- Lê da tabela raw via Trino (catálogo `hive`)
- Calcula **hash keys** para cada entidade (customer, country, category)
- Calcula **hash keys compostas** para links (customer+country, customer+category)
- Calcula **hash diffs** para detecção de mudanças nos satellites
- Materializado como `view` (sem persistência, sempre fresco)

### Camada Raw Vault (dbt)

#### Hubs
- `hub_customer` — business key: `customer_id`
- `hub_country` — business key: `country`
- `hub_category` — business key: `preferred_category`
- Materialização: `incremental` + `append` com `NOT IN`

#### Satellites
- `sat_customer_details` — dados demográficos (age, gender, income, loyalty, device, payment)
- `sat_customer_metrics` — métricas transacionais (orders, spend, churn, satisfaction)
- Materialização: `incremental` + `append` com `NOT EXISTS` no hash diff

#### Links
- `link_customer_country` — associa cliente ao país de residência
- `link_customer_category` — associa cliente à categoria preferida
- Materialização: `incremental` + `append` com `NOT IN`

---

## Dataset

### Fonte

Arquivo: `data/ecommerce_customer_analytics.csv`

### Descrição

Dataset de analytics de clientes de e-commerce com 1000+ registros contendo:

| Coluna | Tipo | Descrição |
|---|---|---|
| `customer_id` | string | Identificador único (CUST-00001 … CUST-01200) |
| `age` | integer | Idade do cliente (18–75) |
| `gender` | string | Identidade de gênero |
| `country` | string | País de residência (15 países) |
| `region` | string | Região geográfica |
| `income_bracket` | string | Faixa de renda anual (USD) |
| `signup_date` | date | Data de cadastro |
| `last_purchase_date` | date | Data da última compra |
| `total_orders` | integer | Total de pedidos |
| `total_spent_usd` | float | Total gasto (USD) |
| `preferred_category` | string | Categoria preferida |
| `loyalty_tier` | string | Nível de fidelidade (Bronze/Silver/Gold/Platinum) |
| `satisfaction_score` | integer | Nota de satisfação (1–5) |
| `churn` | integer | Cliente churned (1) ou ativo (0) |

Dicionário completo em `data/data_dictionary.csv`.

---

## Acessando os Dados

### Via Trino CLI

```bash
docker-compose exec trino trino
```

```sql
-- Tabela raw
SELECT * FROM hive.raw_vault.raw_customers LIMIT 10;

-- Hub de clientes
SELECT * FROM hive.raw_vault.hub_customer LIMIT 10;

-- Satellite com detalhes
SELECT * FROM hive.raw_vault.sat_customer_details LIMIT 10;

-- Join Data Vault: customer + country
SELECT
    hc.customer_id,
    hco.country,
    sd.loyalty_tier,
    sm.total_spent_usd
FROM hive.raw_vault.hub_customer hc
JOIN hive.raw_vault.link_customer_country lcc ON hc.hk_customer = lcc.hk_customer
JOIN hive.raw_vault.hub_country hco ON lcc.hk_country = hco.hk_country
JOIN hive.raw_vault.sat_customer_details sd ON hc.hk_customer = sd.hk_customer
JOIN hive.raw_vault.sat_customer_metrics sm ON hc.hk_customer = sm.hk_customer
ORDER BY sm.total_spent_usd DESC
LIMIT 20;
```

### Via Python (Trino connector)

```python
import trino

conn = trino.dbapi.connect(host="localhost", port=8080, user="trino", catalog="hive", schema="raw_vault")
cursor = conn.cursor()
cursor.execute("SELECT count(*) FROM hub_customer")
print(cursor.fetchone())
```

### Interfaces Web

| Serviço | URL | Credenciais |
|---|---|---|
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Trino UI | http://localhost:8080 | — |
| Spark UI | http://localhost:4040 | — |
| Superset | http://localhost:8088 | admin / admin |

---

## Conectando via IDE (DataGrip, DBeaver, VS Code, etc.)

Trino é compatível com o protocolo JDBC, permitindo conexão direta a partir de qualquer IDE que suporte drivers JDBC/SQL.

### Parâmetros de Conexão

| Parâmetro | Valor |
|---|---|
| **Host** | `localhost` |
| **Porta** | `8080` |
| **Usuário** | `trino` |
| **Senha** | *(vazio — sem autenticação)* |
| **Catálogo (Database)** | `hive` |
| **Schema** | `raw_vault` |
| **Driver** | Trino JDBC |
| **URL JDBC** | `jdbc:trino://localhost:8080/hive/raw_vault` |

### DataGrip (JetBrains)

1. **File → New → Data Source → Trino**
2. Preencha os campos:
   - Host: `localhost`
   - Port: `8080`
   - User: `trino`
   - Password: *(deixar vazio)*
   - Database: `hive`
3. Na aba **Schemas**, marque `raw_vault` para visualizar as tabelas
4. Clique em **Test Connection** para validar
5. Se o driver não estiver instalado, o DataGrip oferece download automático

> 💡 **Dica**: Na URL avançada, use `jdbc:trino://localhost:8080/hive/raw_vault` para já abrir no schema correto.

### DBeaver

1. **Database → New Database Connection**
2. Busque por **Trino** (ou "Presto" em versões antigas)
3. Configure:
   - Host: `localhost`
   - Port: `8080`
   - Database/Catalog: `hive`
   - Username: `trino`
   - Password: *(vazio)*
4. Na aba **Driver Properties**, garanta que `SSL` está como `false`
5. Clique em **Test Connection**

> Se o driver Trino não estiver disponível, baixe o JAR em: https://trino.io/docs/current/client/jdbc.html

### VS Code (SQL Tools Extension)

1. Instale a extensão **SQLTools** + **SQLTools Trino Driver**
2. Adicione uma nova conexão com o JSON:

```json
{
  "name": "Trino Local",
  "driver": "Trino",
  "server": "localhost",
  "port": 8080,
  "username": "trino",
  "catalog": "hive",
  "schema": "raw_vault"
}
```

### IntelliJ IDEA / PyCharm (Database Tool)

Mesmo processo do DataGrip — o plugin de Database é idêntico:
1. **View → Tool Windows → Database**
2. **+ → Data Source → Trino**
3. Mesmos parâmetros acima

### Conexão via JDBC genérico

Para qualquer ferramenta que suporte JDBC:

- **Driver Class**: `io.trino.jdbc.TrinoDriver`
- **JDBC URL**: `jdbc:trino://localhost:8080/hive/raw_vault`
- **Driver JAR**: [trino-jdbc-latest.jar](https://repo1.maven.org/maven2/io/trino/trino-jdbc/)

### Queries de exemplo após conectar

```sql
-- Listar schemas disponíveis
SHOW SCHEMAS FROM hive;

-- Listar tabelas do Data Vault
SHOW TABLES FROM hive.raw_vault;

-- Contar registros no Hub
SELECT count(*) FROM hive.raw_vault.hub_customer;

-- Top 10 clientes por gasto
SELECT
    hc.customer_id,
    sm.total_spent_usd,
    sd.loyalty_tier,
    hco.country
FROM hive.raw_vault.hub_customer hc
JOIN hive.raw_vault.sat_customer_metrics sm ON hc.hk_customer = sm.hk_customer
JOIN hive.raw_vault.sat_customer_details sd ON hc.hk_customer = sd.hk_customer
JOIN hive.raw_vault.link_customer_country lcc ON hc.hk_customer = lcc.hk_customer
JOIN hive.raw_vault.hub_country hco ON lcc.hk_country = hco.hk_country
ORDER BY sm.total_spent_usd DESC
LIMIT 10;
```

### Nota sobre SSL/TLS

Este ambiente de desenvolvimento **não** utiliza SSL. Em ambientes produtivos, configure:
- `SSL=true` nas propriedades do driver
- Certificados no truststore Java

---

## Troubleshooting

### Hive Metastore não inicia

O Metastore depende do PostgreSQL estar healthy. Verifique:

```bash
docker-compose logs hive-metastore
docker-compose exec postgres pg_isready -U hive -d metastore
```

### Erro "Schema raw_vault does not exist" no dbt

Execute a ingestão primeiro — o Spark cria o schema:

```bash
make dv-ingest
```

### Tabelas não aparecem no Trino

Aguarde o Hive Metastore sincronizar e verifique:

```sql
SHOW SCHEMAS FROM hive;
SHOW TABLES FROM hive.raw_vault;
```

### Spark Connect timeout

Verifique se o container spark-connect está rodando:

```bash
docker-compose logs spark-connect
```

### dbt compilation error

Garanta que está usando o profiles.yml correto:

```bash
docker-compose exec dev bash -c "cd dbt_datavault && dbt debug --profiles-dir ."
```

### Reset completo do ambiente

Se algo estiver em estado inconsistente:

```powershell
# Windows
.\reset.ps1

# Linux/macOS
./reset.sh
```

---

## Extensões Futuras

- **Business Vault** — adicionar camadas calculadas (point-in-time tables, bridge tables)
- **Streaming** — ingestão via Kafka para carga near-real-time nos satellites
- **Data Quality** — integrar Great Expectations ou dbt-expectations
- **CDC** — captura de mudanças incrementais com Debezium
- **Superset Dashboards** — visualizações conectando diretamente ao Trino
- **Iceberg Format** — migrar para formato Iceberg quando Spark 4.0 estabilizar integração

---

## Licença

Projeto educacional para demonstração de arquitetura Data Vault com stack moderna de dados.
