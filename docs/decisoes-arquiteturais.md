# Decisões Arquiteturais

Registro das principais decisões de arquitetura do projeto, no espírito de **ADR (Architecture Decision Record)**: cada entrada descreve o *contexto* (o problema), a *decisão* tomada e os *trade-offs* aceitos. O objetivo é deixar rastreável **por que** o projeto é do jeito que é — inclusive para quando eu mesmo revisitá-lo no futuro.

---

## ADR-01 — Arquitetura Medalhão (Landing → Bronze → Silver → Gold)

**Contexto:** era preciso um padrão de organização das camadas de dados que separasse dado bruto de dado tratado e de dado pronto para análise, com rastreabilidade a cada etapa.

**Decisão:** adotar a arquitetura Medalhão com quatro camadas:
- **Landing** — cópia crua e imutável dos CSVs, particionada por data de ingestão.
- **Bronze** — mesmos dados em Parquet, sem transformação (só metadado técnico de processamento).
- **Silver** — dados limpos, tipados e padronizados.
- **Gold** — modelo dimensional (star schema) pronto para consumo analítico.

**Trade-offs:** há duplicação de armazenamento (o mesmo dado existe em 4 formas). Em contrapartida, cada camada pode ser reprocessada isoladamente e a origem de qualquer número é rastreável de volta até o CSV cru. Para um data lake, essa é uma troca padrão de mercado e vantajosa.

---

## ADR-02 — MinIO como Data Lake local (S3-compatible)

**Contexto:** o projeto precisava de um armazenamento de objetos para o data lake, mas sem depender de uma conta paga em nuvem para desenvolver e testar.

**Decisão:** usar **MinIO**, que expõe a mesma API do Amazon S3. O Spark acessa tudo via protocolo `s3a://`, exatamente como faria contra um bucket S3 real.

**Trade-offs:** MinIO local não reproduz características de escala/latência da nuvem real. Mas o **código de leitura/escrita é idêntico** — migrar para S3 de verdade seria só trocar endpoint e credenciais, sem tocar na lógica dos jobs. Ganha-se portabilidade e custo zero de desenvolvimento.

---

## ADR-03 — Separar a extração (Kaggle → Landing) da conversão (Landing → Bronze)

**Contexto:** poderia ser tentador fazer uma única DAG/script que baixa do Kaggle e já grava em Bronze de uma vez.

**Decisão:** manter **duas etapas independentes**. A extração (`kaggle_to_landing`) apenas pousa os CSVs crus na Landing Zone. A conversão (`landing_to_bronze`) lê a Landing e grava Bronze, usando um wildcard de caminho (`s3a://landing-zone/vendas/*/arquivo.csv`) que processa qualquer extração já pousada.

**Trade-offs:** é uma etapa a mais para orquestrar. Em troca, ganha-se uma propriedade crítica: **a conversão pode ser reprocessada infinitas vezes sem bater na API do Kaggle** (que é lenta, instável e sujeita a rate-limit). Se um bug de transformação for descoberto na Bronze, corrige-se e reprocessa só os CSVs locais — a parte frágil (rede/API externa) fica isolada na etapa de extração.

---

## ADR-04 — Parquet em todas as camadas internas

**Contexto:** definir o formato de arquivo de Bronze em diante.

**Decisão:** usar **Parquet** (colunar, comprimido, com schema embutido) em Bronze, Silver e Gold. Apenas a Landing mantém o CSV original.

**Trade-offs:** Parquet não é legível "a olho nu" como CSV. Mas é muito mais eficiente para leitura pelos jobs Spark subsequentes (leitura colunar, predicate pushdown, compressão) e carrega o schema junto, evitando reinferência de tipos. É o padrão de mercado para data lakes.

---

## ADR-05 — Uma DAG genérica por camada, com uma task por tabela

**Contexto:** a primeira versão do projeto tinha **10 DAGs + 10 scripts quase idênticos** (um par por tabela em cada camada). Isso gerava duplicação massiva: uma correção precisava ser replicada em vários arquivos, e uma delas chegou a passar despercebida.

**Decisão:** substituir por **uma DAG genérica por camada**, que gera dinamicamente uma task por tabela a partir de um **registro central de configuração** (`config/tabelas_olist.py`). A lógica específica de cada tabela vive num dicionário de funções (`silver_rules.py` / `gold_rules.py`).

**Trade-offs:** exige entender geração dinâmica de tasks no Airflow (menos óbvio que 10 arquivos explícitos). Em troca:
- Adicionar uma tabela nova = **1 linha de config** (e uma função de regra), não um par DAG/script copiado.
- **Isolamento de falha preservado**: cada tabela continua sendo uma task independente — se `reviews` falha, as outras 8 seguem.
- Zero duplicação de código de orquestração.

Ver detalhes da armadilha que motivou isso em [Desafios Técnicos → Duplicação de DAGs](desafios-tecnicos.md).

---

## ADR-06 — SparkSession centralizada e credenciais via variáveis de ambiente

**Contexto:** as primeiras versões tinham as credenciais do MinIO **hardcoded** em texto puro dentro do código (`utils/spark_utils.py` e notebooks), e cada script recriava a configuração de conexão S3A do zero.

**Decisão:**
- Centralizar a criação da sessão em `utils/spark_utils.py:create_spark_session()`, usado por **todos** os jobs e notebooks.
- Ler `MINIO_ENDPOINT` / `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` de **variáveis de ambiente** (`os.getenv`), nunca hardcoded.
- Declarar as conexões do Airflow (`spark_default`, `minio_default`) via `AIRFLOW_CONN_*` no `.env`, que fica fora do versionamento.

**Trade-offs:** exige que o ambiente (container) tenha as variáveis definidas — um script rodado fora do container sem elas falha. É o comportamento correto: segredo nenhum vive no código, e a configuração de conexão existe em um único lugar.

---

## ADR-07 — Spark em cluster standalone (Master/Worker) em vez de modo local

**Contexto:** o Spark poderia rodar em modo `local[*]` dentro do próprio container do Airflow, o que seria mais simples.

**Decisão:** subir um **cluster Spark standalone** com serviços separados de Master e Worker, e submeter os jobs via `SparkSubmitOperator` apontando para `spark://spark-master:7077`.

**Trade-offs:** mais containers e mais configuração (JARs de integração S3A, versões alinhadas entre Airflow-client e cluster). Em troca, reproduz o comportamento real de submissão de jobs distribuídos (driver no Airflow, execução no worker), muito mais próximo de um ambiente produtivo — e é justamente a habilidade que se quer demonstrar num portfólio.

---

## ADR-08 — Três fatos de grãos separados na Gold (evitar fan-out)

**Contexto:** ao modelar a camada Gold, a tentação seria criar uma única tabela-fato juntando pedidos, itens e pagamentos. Mas `items` e `payments` têm **grãos diferentes** por pedido: um pedido pode ter N itens e M registros de pagamento (parcelamento).

**Decisão:** criar **três fatos separados**, cada um no seu grão nativo:
- `fato_pedidos_itens` — 1 linha por item de pedido (receita, frete, prazo de entrega).
- `fato_pagamentos` — 1 linha por registro de pagamento.
- `fato_avaliacoes` — 1 linha por avaliação.

Análises cruzadas (ex: "atraso derruba a nota?") são feitas com um join entre fatos por `order_id`.

**Trade-offs:** o analista precisa saber qual fato usar para cada métrica (não há uma tabela "tudo em um"). Em troca, **evita-se o fan-out** — a multiplicação silenciosa de linhas que inflaria receita e frete. Isso foi medido com dados reais: o join ingênuo produziria ~4% de linhas a mais, contando receita duplicada sem gerar nenhum erro. Ver [Desafios Técnicos → Fan-out](desafios-tecnicos.md) e [Modelo Dimensional](modelo-dimensional.md).

---

## ADR-09 — Dremio como camada de consumo

**Contexto:** era preciso uma forma de consultar a Gold via SQL, idealmente sem copiar os dados para um data warehouse separado.

**Decisão:** usar **Dremio** como query engine sobre o lakehouse. Ele lê o Parquet diretamente do MinIO e expõe uma interface SQL, servindo como ponte para ferramentas de BI.

**Trade-offs:** é mais um serviço pesado no compose. Mas permite consulta federada sem duplicação de dados (query-in-place) e abre caminho para plugar um BI (Metabase/Superset) no futuro, sem reprocessar nada.

---

## ADR-10 — `zip_code_prefix` mantido como string, não inteiro

**Contexto:** as colunas de CEP (`customer_zip_code_prefix`, `seller_zip_code_prefix`, `geolocation_zip_code_prefix`) parecem numéricas e a tentação é castá-las para inteiro na Silver.

**Decisão:** mantê-las como **string**.

**Trade-offs:** ocupam um pouco mais de espaço que um int. Mas CEPs brasileiros podem ter **zeros à esquerda** (ex: `01234`) — um cast para inteiro os transformaria em `1234`, corrompendo o dado silenciosamente e quebrando joins geográficos. É um exemplo de decisão guiada pelo significado do dado, não pela aparência.

---

## ADR-11 — Validação de dados embutida nos jobs, com falha bloqueante

**Contexto:** o pipeline precisava de checks de qualidade de dados (nulos, duplicatas, integridade referencial, fan-out). Havia três formas de fazer: adotar uma ferramenta pronta (Great Expectations), criar tasks de validação separadas no Airflow rodando *depois* da escrita, ou embutir a validação dentro do próprio job, *antes* da escrita.

**Decisão:** framework leve próprio, **embutido nos jobs e rodando antes da escrita**:
- Checks reutilizáveis em `utils/validation_checks.py`, declarados por tabela em um registro central (`config/validacoes.py`) — mesmo padrão config + regras usado no restante do pipeline.
- **Falha bloqueia**: qualquer check reprovado levanta exceção, o job morre, a task fica vermelha no Airflow e **o dado inválido nunca é gravado na camada**.
- Cada execução grava um **manifest.json** de auditoria (linhas, schema, checksum do conteúdo, resultado dos checks) no bucket `logs`, espelhando o caminho do dado.
- O **checksum** usa `xxhash64` nativo do Spark somado por linha — determinístico, independente da ordem das linhas e executado 100% na JVM (sem UDF Python, de propósito — ver desafio nº 9).

**Trade-offs:**
- *Contra ferramenta pronta:* Great Expectations é o equivalente de mercado e traria relatórios ricos, mas é uma dependência pesada para 7 tipos de check — e o framework próprio mantém o projeto inteiro legível de ponta a ponta (valor importante num portfólio).
- *Contra task separada pós-escrita:* validar depois de escrever deixa uma janela em que a camada contém dado ruim (e outro consumidor pode ler nesse meio tempo). Validar antes da escrita elimina a janela; o custo é o `.cache()` do DataFrame, já que ele é percorrido mais de uma vez (contagem, checksum, checks e escrita).

Detalhes de uso e formato do manifest em [Validação de Dados](validacao-de-dados.md).
