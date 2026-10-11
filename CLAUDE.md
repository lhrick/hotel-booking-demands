# CLAUDE.md — Desafio BeFly: Pipeline Medalhão em PySpark (v1, entrega do teste)

Este arquivo orienta o Claude Code na construção deste projeto. Leia inteiro antes de começar e siga as fases na ordem. **Ao fim de cada fase, pare, mostre um resumo do que foi feito e espere minha revisão antes de seguir.**

---

## 1. Contexto e escopo

- Teste técnico para a vaga de Engenheiro de Dados (Pleno/Sênior) na BeFly. O enunciado completo está em `enunciado_teste_engenheiro_de_dados.md` e é a fonte de verdade: em caso de dúvida, releia o enunciado e siga a recomendação dele.
- **Escopo deste repositório (v1):** pipeline PySpark local, exatamente como o enunciado pede. Execução com `spark-submit`, estrutura de pastas sugerida pelo enunciado, até ~2 horas de trabalho.
- **Fora de escopo:** Databricks, Asset Bundles, Lakeflow/expectations, dashboards, Airflow e CI. Essas peças serão uma v2 em outro repositório, apenas citada no comentário arquitetural do README (seção 8). Não adicione nada disso aqui.
- Sou engenheiro de dados com experiência em PySpark, Delta Lake, Airflow e AWS (S3, Glue, Trino). Seja direto; não explique conceitos básicos.

## 2. Regras obrigatórias do enunciado

- Transformações **somente com a API de DataFrames**. Não use `spark.sql(...)` nem `createOrReplaceTempView`. `F.expr()` é permitido apenas para avaliar as regras de qualidade (seção 6).
- **Bronze:**
  - ler os 3 CSVs com `header=True`, `sep=","` e `nullValue="NULL"`, usando **schema explícito** (bônus);
  - gravar Parquet Snappy em `data/bronze/{bookings,countries,hotels}/`;
  - particionar `bookings` por `arrival_date_year` (bônus).
- **Silver:**
  - conversão de tipos conforme a seção 4.2 do enunciado;
  - coluna `arrival_date` montada com `to_date` + `concat_ws` + `lpad`, mapeando o mês por extenso para 1–12;
  - `children` nulo vira 0; `country` nulo vira `"UNK"`;
  - filtros de registros inválidos (sem hóspedes; `adr < 0`), com **log de quantos registros cada filtro removeu**;
  - colunas derivadas: implementar todas as do enunciado (`total_nights`, `total_guests`, `is_family`, `revenue`, `booking_status`, `is_long_stay`);
  - LEFT JOIN com `country_metadata` (traz `country_name`, `continent`) e com `hotel_metadata` (renomeando para `hotel_city`, `hotel_star_rating`, `hotel_opened_year`);
  - **verificar que a contagem de linhas não muda após cada join** e falhar com erro claro se mudar;
  - gravar em `data/silver/bookings_enriched/`, particionado por `arrival_date_year` e `arrival_date_month_num` (bônus).
- **Gold:** implementar as 4 visões sugeridas mais uma própria, todas com `groupBy` + `agg`, em Parquet, com nomes de colunas autoexplicativos:
  - `data/gold/revenue_by_hotel_month/`
  - `data/gold/cancellation_by_segment/`
  - `data/gold/top_countries_by_revenue/` (Top 20, ordenado por `total_revenue` desc)
  - `data/gold/stay_profile/` (por `hotel` e `customer_type`)
  - `data/gold/cancellation_risk_profile/` (5ª visão, proposta além das sugeridas: perfis de risco de cancelamento)
- `revenue = adr * total_nights` apenas quando `is_canceled = 0`; caso contrário, 0. Documentar que No-Show conta como cancelado no dataset.
- `avg_adr` considera apenas reservas efetivas, como pede o enunciado.
- **Recorte:** dataset completo (opção recomendada pelo enunciado), registrado no README.

## 3. Estrutura do repositório (a sugerida pelo enunciado)

```
befly-medallion-pipeline/
  README.md
  CLAUDE.md
  enunciado_teste_engenheiro_de_dados.md
  requirements.txt              # pyspark, pyyaml (+ pytest, jupyter, pandas, matplotlib)
  scripts/
    raw_to_bronze.py
    bronze_to_silver.py
    silver_to_gold.py
  utils/
    __init__.py
    config.yaml                 # caminhos raw/bronze/silver/gold e opções de leitura
    config.py                   # carrega o YAML; permite sobrescrever o caminho base por argumento
    spark.py                    # get_spark(app_name): SparkSession local
    schemas.py                  # StructTypes explícitos da Bronze
    quality_rules.py            # regras de qualidade em SQL string
  notebooks/
    exploracao_inicial.ipynb    # Fase 0: exploração dos dados, com saídas versionadas
  tests/                        # extra leve: testes das transformações
    conftest.py
    test_bronze_to_silver.py
    test_silver_to_gold.py
  data/
    raw/                        # 3 CSVs (versionados)
    bronze/  silver/  gold/     # gerados (no .gitignore)
  .gitignore
```

**Imports dos scripts.** Com `spark-submit scripts/x.py`, a raiz do projeto não fica no `sys.path`. Cada script deve inserir a raiz do projeto no `sys.path` no topo, com um comentário de uma linha explicando o motivo, para funcionar sem `PYTHONPATH` em qualquer sistema operacional.

**Execução.** Cada script aceita `--config` (padrão `utils/config.yaml`) e roda a partir da raiz do repositório:

```
spark-submit scripts/raw_to_bronze.py
spark-submit scripts/bronze_to_silver.py
spark-submit scripts/silver_to_gold.py
```

## 4. Fase 0 — Setup e exploração dos dados

1. `git init`, `.gitignore`, `requirements.txt`, estrutura de pastas.
   - Fixar a versão do `pyspark`. PySpark 4.x exige Java 17+; confirme a versão do Java instalada e me avise se for incompatível.
2. Os 3 CSVs já existem no diretório. Localize-os e mova-os para `data/raw/`, se ainda não estiverem lá.
3. Crie `notebooks/exploracao_inicial.ipynb` (previsto no enunciado) para inspecionar:
   - cabeçalhos, schema inferido e contagem de linhas;
   - valores distintos de `arrival_date_month`, `market_segment`, `distribution_channel`, `customer_type` e `reservation_status`;
   - o **formato real** de `reservation_status_date`;
   - ocorrências dos literais `"NULL"` e `"NA"` por coluna;
   - os nomes reais das colunas de `country_metadata.csv` e `hotel_metadata.csv`;
   - os pontos conhecidos do dataset original, listados abaixo.
4. Pontos conhecidos a confirmar:
   - `children` tem valores `"NA"`, que o `nullValue="NULL"` não cobre. Na Bronze, leia `children` como **string** (fiel à fonte) e faça o cast na Silver.
   - Existem reservas com 0 hóspedes e pelo menos um `adr` negativo.
   - Há muitas **linhas inteiramente duplicadas** e não existe ID de reserva. **Não deduplique.** Registre a contagem e explique no README que, sem chave de negócio, não dá para distinguir duplicata de reservas idênticas legítimas.
   - Pode haver códigos de país sem correspondência em `country_metadata`. Conte quantos ficam sem `country_name`.
5. Commit do notebook **com as saídas renderizadas**.

**Aceite:** ambiente funcionando, notebook executado e relatório dos achados para mim antes da Fase 1.

## 5. Fase 1 — Bronze, Silver e Gold

- Implementar os três scripts conforme a seção 2.
- Usar funções pequenas e puras de transformação (`DataFrame -> DataFrame`), separadas de leitura e escrita, para facilitar os testes.
- Usar o módulo `logging`, sem `print`. Logar contagens em cada etapa: lidas, removidas por regra, após cada join e escritas.
- Escrita com `mode("overwrite")`, para que o pipeline seja idempotente e reprocessável.
- Não usar `.cache()` / `.persist()` sem necessidade real. O volume é pequeno.
- Testes leves com DataFrames pequenos montados à mão:
  - mapeamento de mês e montagem de `arrival_date`;
  - preenchimento de nulos;
  - cada filtro de remoção;
  - `revenue = 0` para reserva cancelada;
  - joins não duplicam linhas;
  - `cancellation_rate` entre 0 e 1;
  - Top 20 com no máximo 20 linhas e ordenação correta.

**Aceite:** os três `spark-submit` rodam do zero (com `data/bronze`, `silver` e `gold` apagados), o `pytest` passa e os logs mostram as contagens.

## 6. Regras de qualidade (`utils/quality_rules.py`)

Dicionários `{nome: expressão SQL}`:
- `SILVER_DROP_RULES`: `has_guests` (`adults + children + babies > 0`) e `non_negative_adr` (`adr >= 0`).
- `SILVER_WARN_RULES`: `valid_arrival_date`, `known_country` (`country <> 'UNK'`), `has_country_metadata`.

Na Silver, aplicar cada regra de remoção com `filter(F.expr(rule))`, logando quantos registros cada uma removeu. As regras de alerta apenas contam e logam.

Motivo do formato em SQL string: as mesmas regras podem ser reaproveitadas como expectations do Databricks na v2. Citar isso em uma linha no README, sem detalhar.

## 7. Fase 2 — README.md (em português)

Deve conter, nesta ordem:
1. Visão geral e diagrama simples das camadas (texto ou Mermaid).
2. Pré-requisitos: versões de Python, PySpark e Java; instalação com `pip install -r requirements.txt`.
3. Como apontar os caminhos dos CSVs (`utils/config.yaml`) e os comandos para rodar o pipeline e os testes.
4. Recorte: dataset completo.
5. Achados de qualidade dos dados: duplicatas não removidas e por quê, `"NA"` em `children`, países sem metadado, contagens removidas por regra (números reais da execução).
6. Principais transformações da Silver.
7. Visões Gold e a pergunta de negócio que cada uma responde.
8. Comentário arquitetural (seção 8 abaixo).
9. Limitações e próximos passos.

Tom técnico e direto, sem marketing.

## 8. Comentário arquitetural (exigido pelo enunciado)

1–2 parágrafos **autossuficientes**: o avaliador precisa entender a resposta sem clicar em nenhum link. Responder: como adaptar o pipeline para AWS e/ou Databricks em produção. Cobrir:
- **Armazenamento:** layout no S3 por camada e domínio (`s3://<bucket>/{bronze,silver,gold}/<domínio>/<tabela>/`), Delta Lake no lugar de Parquet (ACID, MERGE, time travel, OPTIMIZE/VACUUM).
- **Ingestão:** incremental com Auto Loader.
- **Orquestração:** Databricks Workflows, com deploy via Asset Bundles (IaC declarativa, targets dev/prod, CI/CD com GitHub Actions).
- **Qualidade:** expectations do Lakeflow Declarative Pipelines reaproveitando as regras de `quality_rules.py`.
- **Governança:** Unity Catalog (permissões, linhagem, catálogos por ambiente).
- **Alternativa AWS-nativa:** S3 + Glue Data Catalog + Glue jobs ou EMR + Athena/Trino, orquestrados com Airflow (MWAA) ou Step Functions.

Ao final, uma frase com link para a v2: `<!-- TODO: link da v2 -->`. Mantenha o placeholder; eu preencho quando a v2 estiver publicada. Se não houver link no momento da entrega, a frase deve ser removida.

Não afirmar experiência com Terraform. Descrever os bundles como IaC declarativa nativa do Databricks.

## 9. Higiene do repositório

- `.gitignore`: `data/bronze/`, `data/silver/`, `data/gold/`, `spark-warehouse/`, `metastore_db/`, `derby.log`, `.venv/`, `__pycache__/`, `.ipynb_checkpoints/`, `.pytest_cache/`, `.env`.
- Os CSVs em `data/raw/` são versionados (pequenos, garantem a reprodutibilidade para o avaliador).
- Código, nomes de colunas, logs e commits em **inglês**; comentários, docstrings e README em **português**.
- Conventional commits (`feat:`, `test:`, `docs:`, `chore:`), com pelo menos um commit por fase.
- Type hints e docstrings curtas.
- Não adicione dependências além das listadas sem me perguntar.
