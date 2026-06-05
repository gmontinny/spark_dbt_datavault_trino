# Engenharia de Dados Moderna com Data Vault 2.0, Apache Spark e Trino

## Resumo

Este artigo apresenta a implementação completa de uma arquitetura de dados baseada na metodologia **Data Vault 2.0**, utilizando um ecossistema open-source composto por Apache Spark 4.0, Trino, dbt e MinIO. Através de um estudo de caso com dados reais de e-commerce customer analytics (1.200 registros, 24 atributos), demonstramos como construir um pipeline de dados **idempotente**, **auditável** e **historicamente rastreável** — características essenciais para plataformas de dados corporativas. O artigo detalha decisões de arquitetura, padrões de implementação, estratégias de carga incremental sem suporte a `MERGE`, e valida a idempotência do pipeline com evidências de execução.

**Palavras-chave:** Data Vault 2.0, Apache Spark, Trino, dbt, Data Lakehouse, Idempotência, Hash Keys, Slowly Changing Dimensions.

---

## 1. Introdução

### 1.1 O Problema das Modelagens Tradicionais

Em ambientes de Big Data, a rigidez dos modelos dimensionais tradicionais (Star Schema, Snowflake) se manifesta em três pontos críticos:

- **Acoplamento entre fontes e modelo** — qualquer mudança na origem exige refatoração do warehouse
- **Perda de historicidade** — dimensões Type 1 (overwrite) destroem o estado anterior
- **Dificuldade de integração** — novas fontes exigem redesign de tabelas fato e dimensão

O **Data Vault 2.0**, proposto por Dan Linstedt, resolve esses problemas separando estruturalmente a identidade das entidades (Hubs), seus relacionamentos (Links) e seus atributos descritivos (Satellites).

### 1.2 Objetivo

Construir um pipeline de dados end-to-end que:

1. Ingira dados brutos de forma atômica e idempotente
2. Transforme em modelo Data Vault preservando historicidade
3. Seja re-executável N vezes sem duplicação ou perda de dados
4. Utilize exclusivamente ferramentas open-source

### 1.3 Contribuições

- Implementação completa de Data Vault 2.0 sobre Trino (Hive connector) sem suporte a `MERGE`
- Estratégia de idempotência via `incremental` + `append` com filtros anti-duplicação
- Demonstração de Spark Connect (gRPC) como padrão de ingestão desacoplado
- Validação empírica da idempotência com evidências de execução

---

## 2. Fundamentação Teórica

### 2.1 Panorama das Arquiteturas de Modelagem de Dados

Antes de detalhar o Data Vault, é fundamental entender como ele se posiciona em relação às outras abordagens de modelagem de data warehouse. Cada metodologia surgiu para resolver problemas específicos de uma era da engenharia de dados.

#### 2.1.1 Modelo Relacional Normalizado (3NF — Inmon)

Proposto por Bill Inmon nos anos 1990, o modelo Enterprise Data Warehouse (EDW) segue a **Terceira Forma Normal (3NF)**: dados completamente normalizados, sem redundância.

```
┌──────────┐     ┌───────────┐     ┌──────────────┐
│ CUSTOMER │────▶│  ADDRESS  │────▶│    COUNTRY   │
│          │     │           │     │              │
│ • id     │     │ • id      │     │ • id         │
│ • name   │     │ • street  │     │ • name       │
│ • email  │     │ • city    │     │ • region     │
└──────────┘     └───────────┘     └──────────────┘
```

| Vantagem | Desvantagem |
|---|---|
| Sem redundância de dados | Queries complexas com muitos JOINs |
| Consistência garantida | Performance ruim para analytics |
| Boa para sistemas OLTP | Difícil de evoluir sem refatoração |

#### 2.1.2 Modelo Dimensional (Star Schema — Kimball)

Ralph Kimball propôs o modelo dimensional nos anos 1990, otimizado para **consultas analíticas**:

```
                    ┌──────────────┐
                    │ DIM_CUSTOMER │
                    │              │
                    │ • customer_id│
                    │ • name       │
                    │ • country    │
                    │ • loyalty    │
                    └──────┬───────┘
                           │
┌──────────┐     ┌─────────┴─────────┐     ┌──────────┐
│ DIM_DATE │────▶│   FACT_ORDERS     │◀────│DIM_PROD  │
│          │     │                   │     │          │
│ • date_id│     │ • customer_id(FK) │     │ • prod_id│
│ • month  │     │ • date_id(FK)     │     │ • name   │
│ • year   │     │ • product_id(FK)  │     │ • categ  │
└──────────┘     │ • total_spent     │     └──────────┘
                 │ • quantity        │
                 └───────────────────┘
```

| Vantagem | Desvantagem |
|---|---|
| Queries simples e rápidas | Redundância intencional (desnormalização) |
| Fácil de entender para analistas | Historicidade limitada (SCD Type 1/2 manuais) |
| Boa performance em BI tools | Nova fonte = refatoração de fatos e dimensões |
| Padrão de mercado para BI | Acoplado à visão de negócio atual |

#### 2.1.3 Data Vault 2.0 (Linstedt)

Dan Linstedt propôs o Data Vault como uma **camada intermediária** entre raw e consumo, otimizada para **flexibilidade e auditabilidade**:

```
┌───────────┐     ┌────────────────┐     ┌────────────────┐
│    HUB    │◀────│   SATELLITE    │     │   SATELLITE    │
│ (Quem é)  │     │ (Versão 1)     │     │ (Versão 2)     │
│           │     │                │     │                │
│ • hash_key│     │ • hash_key(FK) │     │ • hash_key(FK) │
│ • biz_key │     │ • load_date: T1│     │ • load_date: T2│
│ • load_dt │     │ • attr_a: X    │     │ • attr_a: Y ←──── mudou!
│ • source  │     │ • attr_b: Z    │     │ • attr_b: Z    │
└─────┬─────┘     └────────────────┘     └────────────────┘
      │
      │
┌─────┴─────┐     ┌───────────┐
│   LINK    │────▶│    HUB    │
│(Relação)  │     │ (Outra    │
│           │     │  entidade)│
│ • hk_link │     │           │
│ • hk_a(FK)│     │ • hash_key│
│ • hk_b(FK)│     │ • biz_key │
└───────────┘     └───────────┘
```

| Vantagem | Desvantagem |
|---|---|
| Historicidade nativa (insert-only) | Mais tabelas, mais JOINs para consumo |
| Totalmente auditável | Curva de aprendizado mais alta |
| Nova fonte = novo hub/link (sem refatorar) | Precisa de camada de consumo sobre (Business Vault) |
| Paralelizável (sem dependência entre cargas) | Overhead em datasets pequenos |
| Idempotente por design | Hash keys adicionam complexidade |

#### 2.1.4 Modelagem One Big Table (OBT)

Abordagem moderna popular em data lakes: uma única tabela desnormalizada com todos os atributos.

| Vantagem | Desvantagem |
|---|---|
| Sem JOINs, máxima performance de leitura | Redundância extrema |
| Simples de implementar | Sem historicidade |
| Ideal para ML/analytics específicos | Não escala para múltiplas fontes |

#### 2.1.5 Comparação Consolidada

| Critério | 3NF (Inmon) | Star (Kimball) | Data Vault | OBT |
|---|---|---|---|---|
| **Historicidade** | Manual | SCD manual | Nativa | Nenhuma |
| **Auditabilidade** | Parcial | Parcial | Total | Nenhuma |
| **Flexibilidade** | Baixa | Baixa | Alta | Média |
| **Performance analítica** | Baixa | Alta | Média* | Máxima |
| **Complexidade** | Média | Baixa | Alta | Muito baixa |
| **Paralelismo de carga** | Baixo | Baixo | Alto | N/A |
| **Número de tabelas** | Médio | Baixo | Alto | 1 |
| **Ideal para** | OLTP/EDW | BI/Reports | Raw Vault/Integration | ML/Ad-hoc |

*_Performance analítica do Data Vault melhora com Business Vault (PIT/Bridge tables) na camada de consumo._

#### 2.1.6 Quando usar Data Vault?

O Data Vault é a escolha ideal quando:

- **Múltiplas fontes** precisam ser integradas sem acoplamento
- **Auditoria regulatória** exige rastreabilidade completa (LGPD, SOX, GDPR)
- **Mudanças frequentes** nos sistemas fonte (novos campos, novas entidades)
- **Equipes paralelas** precisam carregar dados independentemente
- **Historicidade** é requisito de negócio ("qual era o status do cliente em março?")

O Data Vault **não é ideal** quando:

- O dataset é pequeno e estável (overhead desnecessário)
- A necessidade é apenas um dashboard simples (Star Schema é suficiente)
- Não há requisito de historicidade ou auditoria

#### 2.1.7 Posicionamento na Arquitetura Medalhão

Na prática moderna, o Data Vault posiciona-se como a camada **Silver** de uma arquitetura Medalhão:

```
┌─────────────────────────────────────────────────────────────┐
│                    ARQUITETURA MEDALHÃO                       │
│                                                             │
│  ┌──────────┐    ┌──────────────────┐    ┌──────────────┐  │
│  │  BRONZE  │───▶│     SILVER       │───▶│    GOLD      │  │
│  │  (Raw)   │    │  (Data Vault)    │    │ (Star Schema │  │
│  │          │    │                  │    │  / OBT)      │  │
│  │ • CSV    │    │ • Hubs           │    │              │  │
│  │ • JSON   │    │ • Satellites     │    │ • dim_*      │  │
│  │ • Parquet│    │ • Links          │    │ • fact_*     │  │
│  │ • APIs   │    │ • Historicidade  │    │ • Dashboards │  │
│  └──────────┘    └──────────────────┘    └──────────────┘  │
│                                                             │
│  Ingestão fiel      Integração +          Consumo +        │
│  (append-only)      Auditoria             Performance       │
└─────────────────────────────────────────────────────────────┘
```

Esta separação permite que o Data Vault sirva como **single source of truth** auditável, enquanto camadas de consumo (Gold) podem ser modeladas como Star Schema ou OBT para performance analítica.

---

### 2.2 Data Vault 2.0 — Detalhamento

O Data Vault estrutura os dados em três tipos de entidades:

| Componente | Responsabilidade | Analogia |
|---|---|---|
| **Hub** | Identidade única de uma entidade de negócio | "Quem é" |
| **Link** | Relacionamento entre entidades | "Como se conectam" |
| **Satellite** | Atributos descritivos com versionamento temporal | "O que sabemos sobre" |

**Princípios fundamentais:**
- Todo registro possui `load_datetime` (quando foi carregado) e `record_source` (de onde veio)
- Hubs são imutáveis após inserção
- Satellites acumulam versões — nunca sobrescrevem
- Links registram a existência de um relacionamento, não seu estado

### 2.3 Hash Keys vs. Surrogate Keys

O Data Vault 2.0 preconiza o uso de **Hash Keys** em vez de sequences/identity:

```
hk_customer = SHA-256("CUST-00001") → determinístico, reproduzível, paralelizável
```

Vantagens:
- **Determinismo** — mesma entrada sempre gera mesma saída, independente da ordem de carga
- **Paralelismo** — não depende de lock de sequence no banco
- **Distribuição** — facilita sharding e particionamento
- **Reprodutibilidade** — recalculável a partir da source sem necessidade do target

### 2.4 Hash Diff para Detecção de Mudanças

O **Hash Diff** é um hash calculado sobre todos os atributos de um satellite. Ele funciona como uma "impressão digital" do estado atual da entidade:

```
hd_customer_details = SHA-256(age || gender || income_bracket || loyalty_tier || ...)
```

Se o hash diff de uma nova carga for diferente do último registrado, significa que houve mudança — e uma nova versão é inserida no satellite. Isso implementa nativamente o padrão **SCD Type 2** (Slowly Changing Dimension) sem necessidade de comparação campo a campo.

### 2.5 Idempotência em Pipelines de Dados

Um pipeline é **idempotente** quando `f(f(x)) = f(x)` — executar múltiplas vezes produz o mesmo resultado que executar uma vez. Em engenharia de dados, isso significa:

- Não duplicar registros em re-execuções
- Não perder dados em caso de falha parcial
- Não depender de estado externo (timestamps, counters)

---

## 3. Arquitetura da Solução

### 3.1 Visão Geral

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           PIPELINE DE DADOS                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────┐    ┌──────────────┐    ┌─────────────┐    ┌───────────┐  │
│  │  CSV    │───▶│ Apache Spark │───▶│   Parquet   │───▶│   Trino   │  │
│  │ (data/) │    │ (Spark       │    │  (MinIO/S3) │    │  (Query   │  │
│  │         │    │  Connect)    │    │             │    │   Engine) │  │
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
│                      │  │ (3)      │  │ (2)        │  │ (2)    │  │   │
│                      │  └──────────┘  └────────────┘  └────────┘  │   │
│                      └─────────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Stack Tecnológica

| Camada | Tecnologia | Versão | Justificativa |
|---|---|---|---|
| Ingestão | Apache Spark (Spark Connect) | 4.0.1 | gRPC thin-client, sem JVM no client |
| Storage | MinIO | Latest | S3-compatible, self-hosted, zero custo de licença |
| Metastore | Hive Metastore + PostgreSQL | 3.x / 16 | Catálogo compartilhado Spark↔Trino |
| Query Engine | Trino | Latest | Federação SQL, connector Hive nativo |
| Transform | dbt-trino | 1.10.2 | Lineage, testes, documentação automática |
| Orquestração | Make + Docker Compose | — | Simplicidade, reprodutibilidade |

### 3.3 Decisão: Hive Connector vs. Iceberg

Durante a implementação, avaliamos duas alternativas:

| Critério | Hive Connector | Iceberg |
|---|---|---|
| Compatibilidade Spark 4.0 | ✅ Nativo | ⚠️ Conflito de JARs (runtime externo) |
| Suporte `incremental` no dbt | ✅ `append` funciona | ✅ `append` + `merge` |
| Complexidade | Baixa | Alta (configuração de catálogo) |
| Time-travel | ❌ | ✅ |

**Decisão:** Optamos pelo connector **Hive** por estabilidade e simplicidade. O Spark grava tabelas Parquet registradas no Hive Metastore, e o Trino lê via connector Hive. A migração para Iceberg é prevista como evolução futura.

---

## 4. Implementação

### 4.1 Camada Raw — Ingestão via Spark Connect

O Apache Spark 4.0 introduz o **Spark Connect**, uma API gRPC que separa o client (Python) do server (JVM). O container `dev` envia comandos remotamente ao `spark-connect` na porta 15002 — sem necessidade de Spark instalado localmente.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()

df = spark.read.option("header", "true").option("inferSchema", "true").csv(CSV_PATH)

# Metadados Data Vault
df = df.withColumn("load_datetime", current_timestamp()) \
       .withColumn("record_source", lit("ecommerce_customer_analytics.csv"))

# Escrita atômica (overwrite sem DROP prévio)
spark.sql("CREATE DATABASE IF NOT EXISTS raw_vault")
df.write.mode("overwrite") \
    .option("path", "s3a://warehouse/raw_vault.db/raw_customers") \
    .saveAsTable("raw_vault.raw_customers")
```

**Garantia de idempotência:** O `mode("overwrite")` substitui atomicamente o conteúdo da tabela. Não há janela temporal onde a tabela fica vazia (como ocorreria com `DROP TABLE` + `INSERT`).

### 4.2 Camada Staging — Preparação de Hash Keys

A view de staging é o ponto central de cálculo. Ela não persiste dados — é recalculada a cada query:

```sql
-- stg_customers.sql (materializado como VIEW)
select
    customer_id,
    country,
    preferred_category,

    -- Hash Keys (SHA-256 sobre business keys)
    cast(to_hex(sha256(cast(customer_id as varbinary))) as varchar) as hk_customer,
    cast(to_hex(sha256(cast(country as varbinary))) as varchar) as hk_country,
    cast(to_hex(sha256(cast(preferred_category as varbinary))) as varchar) as hk_category,

    -- Hash Keys compostas (para Links)
    cast(to_hex(sha256(cast(concat(customer_id, '||', country) as varbinary))) as varchar)
        as hk_customer_country,

    -- Hash Diff (detecta mudanças nos Satellites)
    cast(to_hex(sha256(cast(concat(
        coalesce(cast(age as varchar), ''),
        coalesce(gender, ''),
        coalesce(income_bracket, ''),
        coalesce(loyalty_tier, '')
    ) as varbinary))) as varchar) as hd_customer_details,

    -- Atributos e metadados...
    load_datetime,
    record_source
from hive.raw_vault.raw_customers
```

**Nota técnica:** O uso de `coalesce(..., '')` garante que valores `NULL` não invalidem o hash. O separador `||` nas chaves compostas evita colisões entre valores diferentes que poderiam concatenar para o mesmo resultado.

### 4.3 Camada Raw Vault — Hubs

Hubs contêm apenas a business key e sua hash key. São inseridos uma única vez e nunca modificados:

```sql
-- hub_customer.sql
-- depends_on: {{ ref('stg_customers') }}
{{ config(materialized='incremental', incremental_strategy='append') }}

with staged as (
    select distinct hk_customer, customer_id, load_datetime, record_source
    from {{ ref('stg_customers') }}
)

select * from staged
{% if is_incremental() %}
where hk_customer not in (select hk_customer from {{ this }})
{% endif %}
```

**Resultado:** Na primeira execução, 1.200 clientes são inseridos. Em execuções subsequentes, 0 inserções (todas as chaves já existem).

### 4.4 Camada Raw Vault — Satellites

Satellites implementam SCD Type 2 via Hash Diff:

```sql
-- sat_customer_details.sql
-- depends_on: {{ ref('stg_customers') }}
{{ config(materialized='incremental', incremental_strategy='append') }}

select * from {{ ref('stg_customers') }} s
{% if is_incremental() %}
where not exists (
    select 1 from {{ this }} t
    where t.hk_customer = s.hk_customer
      and t.hd_customer_details = s.hd_customer_details
)
{% endif %}
```

**Comportamento:**
- Dados iguais → hash diff igual → `NOT EXISTS` filtra → 0 inserções
- Cliente mudou de loyalty tier → hash diff diferente → nova versão inserida
- Ambas as versões coexistem na tabela (historicidade preservada)

### 4.5 Camada Raw Vault — Links

Links registram a existência de um relacionamento entre entidades:

```sql
-- link_customer_country.sql
-- depends_on: {{ ref('stg_customers') }}
{{ config(materialized='incremental', incremental_strategy='append') }}

with staged as (
    select distinct hk_customer_country, hk_customer, hk_country, load_datetime, record_source
    from {{ ref('stg_customers') }}
)

select * from staged
{% if is_incremental() %}
where hk_customer_country not in (select hk_customer_country from {{ this }})
{% endif %}
```

**Cenário de mudança:** Se um cliente muda de país, a hash key composta `SHA-256(customer_id || '||' || novo_país)` será diferente da anterior — um novo link é inserido, sem destruir o link histórico.

---

## 5. Validação de Idempotência

### 5.1 Protocolo de Teste

Executamos o pipeline completo duas vezes consecutivas sem alterar os dados fonte:

```bash
# 1ª execução
make dv-pipeline

# 2ª execução (mesmos dados)
make dv-pipeline
```

### 5.2 Resultados — 1ª Execução (Full Load)

| Modelo | Operação | Linhas |
|---|---|---|
| stg_customers | CREATE VIEW | — |
| hub_customer | CREATE TABLE | 1.200 |
| hub_country | CREATE TABLE | 15 |
| hub_category | CREATE TABLE | 10 |
| sat_customer_details | CREATE TABLE | 1.200 |
| sat_customer_metrics | CREATE TABLE | 1.200 |
| link_customer_country | CREATE TABLE | 1.200 |
| link_customer_category | CREATE TABLE | 1.200 |

### 5.3 Resultados — 2ª Execução (Idempotência)

| Modelo | Operação | Linhas |
|---|---|---|
| stg_customers | CREATE VIEW | — |
| hub_customer | INSERT | **0** |
| hub_country | INSERT | **0** |
| hub_category | INSERT | **0** |
| sat_customer_details | INSERT | **0** |
| sat_customer_metrics | INSERT | **0** |
| link_customer_country | INSERT | **0** |
| link_customer_category | INSERT | **0** |

**Conclusão:** Zero duplicações confirmadas. O pipeline é matematicamente idempotente.

### 5.4 Cenário de Mudança (Simulação)

Se o CSV for alterado (ex: `CUST-00001` muda de `United States` para `Canada`):

| Modelo | Comportamento |
|---|---|
| hub_customer | 0 inserções (CUST-00001 já existe) |
| hub_country | 0 inserções (Canada já existe) |
| sat_customer_details | **1 inserção** (hash diff mudou) |
| link_customer_country | **1 inserção** (novo hk_customer_country) |

O link antigo (CUST-00001 ↔ United States) permanece na tabela como registro histórico.

---

## 6. Desafios Técnicos e Soluções

### 6.1 Ausência de MERGE no Hive Connector

O connector Hive do Trino **não suporta** a operação `MERGE INTO`. A estratégia `incremental` do dbt com `merge` falha. Solução:

```yaml
# dbt_project.yml
+incremental_strategy: append  # Em vez de 'merge'
```

Combinado com filtros `NOT IN` / `NOT EXISTS` no SQL, o resultado é equivalente ao `MERGE` sem duplicações.

### 6.2 Resolução de Dependências no dbt

O dbt não consegue inferir dependências quando `ref()` está dentro de `{% if is_incremental() %}`. Solução: hint explícito no topo do modelo:

```sql
-- depends_on: {{ ref('stg_customers') }}
```

### 6.3 Schema Naming no dbt-trino

O dbt concatena `profiles.schema` + `models.+schema` por padrão (ex: `raw_vault_raw_vault`). Solução: definir schema apenas no `profiles.yml` e não nos modelos.

### 6.4 Spark 4.0 + Iceberg (Conflito de Classes)

O Spark 4.0 inclui Iceberg nativamente, mas a versão built-in conflita com JARs externos (`iceberg-spark-runtime`). Solução: utilizar o connector Hive do Trino, que lê tabelas Parquet registradas no Hive Metastore — sem dependência de Iceberg no Spark.

---

## 7. Modelo de Dados Final

### 7.1 Diagrama Entidade-Relacionamento

```
┌─────────────────────────────────────────────────────────┐
│                      DATA VAULT                          │
│                                                         │
│  ┌─────────────┐       ┌──────────────────┐            │
│  │ HUB_CUSTOMER│◄──────│ SAT_CUSTOMER_    │            │
│  │  (1.200)    │       │ DETAILS (1.200+) │            │
│  │             │       │                  │            │
│  │ • hk_customer│       │ • age, gender    │            │
│  │ • customer_id│       │ • income_bracket │            │
│  └──────┬──────┘       │ • loyalty_tier   │            │
│         │              │ • device_type    │            │
│         │              │ • payment_method │            │
│         │              └──────────────────┘            │
│         │                                              │
│         │              ┌──────────────────┐            │
│         │◄─────────────│ SAT_CUSTOMER_    │            │
│         │              │ METRICS (1.200+) │            │
│         │              │                  │            │
│         │              │ • total_orders   │            │
│         │              │ • total_spent    │            │
│         │              │ • satisfaction   │            │
│         │              │ • churn          │            │
│         │              └──────────────────┘            │
│         │                                              │
│    ┌────┴────────────┐       ┌─────────────┐          │
│    │ LINK_CUSTOMER_  │──────▶│ HUB_COUNTRY │          │
│    │ COUNTRY (1.200) │       │   (15)      │          │
│    └────┬────────────┘       │             │          │
│         │                    │ • hk_country│          │
│         │                    │ • country   │          │
│         │                    └─────────────┘          │
│    ┌────┴────────────┐       ┌──────────────┐         │
│    │ LINK_CUSTOMER_  │──────▶│ HUB_CATEGORY │         │
│    │ CATEGORY (1.200)│       │   (10)       │         │
│    └─────────────────┘       │              │         │
│                              │ • hk_category│         │
│                              │ • pref_categ │         │
│                              └──────────────┘         │
└─────────────────────────────────────────────────────────┘
```

### 7.2 Query de Consumo (Exemplo)

```sql
SELECT
    hc.customer_id,
    hco.country,
    sd.loyalty_tier,
    sm.total_spent_usd,
    sm.satisfaction_score
FROM hive.raw_vault.hub_customer hc
JOIN hive.raw_vault.link_customer_country lcc ON hc.hk_customer = lcc.hk_customer
JOIN hive.raw_vault.hub_country hco ON lcc.hk_country = hco.hk_country
JOIN hive.raw_vault.sat_customer_details sd ON hc.hk_customer = sd.hk_customer
JOIN hive.raw_vault.sat_customer_metrics sm ON hc.hk_customer = sm.hk_customer
ORDER BY sm.total_spent_usd DESC
LIMIT 10;
```

---

## 8. Trabalhos Futuros

| Evolução | Descrição | Complexidade |
|---|---|---|
| **Business Vault** | Tabelas PIT (Point-in-Time) e Bridge para otimizar joins | Média |
| **CDC com Debezium** | Substituir batch por captura de mudanças em tempo real | Alta |
| **Iceberg Migration** | Migrar storage para Iceberg (time-travel, schema evolution) | Média |
| **Data Quality** | Integrar `dbt-expectations` para validação de contratos | Baixa |
| **Superset Dashboards** | Camada de visualização conectando ao Trino | Baixa |
| **Streaming Satellites** | Ingestão near-real-time via Kafka → Satellites | Alta |

---

## 9. Conclusão

A implementação demonstra que o Data Vault 2.0, quando sustentado por ferramentas modernas de código aberto, oferece uma base sólida para plataformas de dados que exigem:

- **Auditabilidade total** — cada registro carrega sua origem e timestamp
- **Historicidade nativa** — satellites acumulam versões sem destruir dados anteriores
- **Resiliência operacional** — idempotência comprovada empiricamente
- **Evolução sem refatoração** — novas fontes adicionam hubs/links sem alterar o existente

A separação entre identidade (Hubs), relacionamento (Links) e contexto (Satellites) permite que equipes de dados respondam rapidamente a mudanças de negócio sem a necessidade de migrações destrutivas em modelos legados.

O código-fonte completo está disponível para reprodução e serve como template para implementações em escala produtiva.

---

## 10. Referências

1. Linstedt, D., & Olschimke, M. (2015). *Building a Scalable Data Warehouse with Data Vault 2.0*. Morgan Kaufmann.
2. Apache Spark Documentation. Spark Connect Overview. https://spark.apache.org/docs/latest/spark-connect-overview.html
3. Trino Documentation. Hive Connector. https://trino.io/docs/current/connector/hive.html
4. dbt Labs. Incremental Models. https://docs.getdbt.com/docs/build/incremental-models
5. Linstedt, D. (2019). *Super Charge Your Data Warehouse*. Data Vault Alliance.
6. Kimball, R., & Ross, M. (2013). *The Data Warehouse Toolkit*. Wiley. (Referência comparativa)
7. MinIO Documentation. S3 Compatibility. https://min.io/docs/minio/linux/reference/s3-compatibility.html
