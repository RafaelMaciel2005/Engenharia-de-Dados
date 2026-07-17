# Desafios Técnicos

Problemas reais enfrentados durante o desenvolvimento do projeto, como foram diagnosticados e como foram resolvidos. Estão aqui tanto os erros de infraestrutura quanto as armadilhas de dados — porque saber **identificar e resolver** um problema é tão importante quanto escrever o código que funciona de primeira.

---

## 1. Corrupção do PostgreSQL ao recriar a pasta `volumes/`

**Sintoma:** após apagar manualmente a pasta `volumes/` e rodar `docker compose up -d`, os containers do Airflow ficavam presos em estado `Created` e o Postgres saía com `Exited (128)`. O `up -d` "não funcionava", embora antes sempre tivesse funcionado.

**Diagnóstico:** os logs do Postgres mostravam o banco iniciando normalmente e, segundos depois, arquivos essenciais sumindo no meio da inicialização:
```
FATAL:  could not open file "global/pg_filenode.map": No such file or directory
LOG:  could not open file "postmaster.pid": No such file or directory
```
A causa raiz: `volumes/postgres`, `volumes/airflow/logs` etc. **nunca existiram no Git** (estão no `.gitignore`) — só existiam porque o Docker os criou automaticamente na primeira execução, há muito tempo. Ao apagar a pasta inteira e subir de novo, o Docker Desktop no Windows tentou recriar essa árvore de diretórios como *bind mount* **durante** a subida dos containers, e o Postgres começou a escrever no diretório de dados antes que a estrutura terminasse de ser criada de forma consistente — uma condição de corrida que corrompeu o container.

**Por que "antes não acontecia":** nas execuções anteriores as pastas já existiam, então o Docker nunca precisava criá-las durante o `up`. O problema só aparece quando a árvore precisa ser recriada do zero.

**Solução aplicada:**
1. `docker compose down` — parar tudo de forma limpa.
2. Recriar **manualmente** os diretórios vazios (`volumes/postgres`, `volumes/airflow/logs`, `volumes/airflow/plugins`, `volumes/dremio`) **antes** de subir.
3. `docker compose up -d` — sobe limpo, sem corrida entre criação de pasta e escrita do banco.

**Lição / melhoria futura:** versionar `.gitkeep` para esses subdiretórios de `volumes/` (usando negação no `.gitignore`, como já é feito em `lakehouse/`), garantindo que a estrutura de pastas sempre exista após um `git clone` ou uma limpeza acidental.

---

## 2. CSV do `reviews` com texto multilinha quebrando registros

**Sintoma:** a tabela de avaliações (`olist_order_reviews_dataset.csv`) gerava um número de linhas inflado na leitura, com colunas deslocadas.

**Diagnóstico:** o campo `review_comment_message` contém comentários de clientes com **quebras de linha e aspas dentro do próprio texto**. Lido de forma ingênua (`.option("header","true").option("sep",",")`), o Spark interpretava cada quebra de linha como um novo registro, "vazando" o texto para linhas seguintes e corrompendo o alinhamento das colunas.

**Solução aplicada:** ler esse CSV com opções extras que dizem ao Spark que um campo pode ocupar várias linhas e que aspas delimitam texto:
```python
.option("multiLine", "true").option("quote", "\"").option("escape", "\"")
```
Essas opções ficam declaradas por tabela em `config/tabelas_olist.py` (campo `opcoes_csv`), e o script genérico `landing_to_bronze.py` as aplica dinamicamente — as outras 8 tabelas usam a leitura padrão. A validação posterior confirmou a correção: a distribuição de `review_score` ficou toda entre 1 e 5 (valores fora dessa faixa indicariam linhas quebradas).

---

## 3. Fan-out silencioso ao juntar `items` × `payments`

**Sintoma:** risco (não um erro visível) de inflar a receita ao construir a camada Gold.

**Diagnóstico:** um pedido (`order_id`) pode ter vários itens **e** vários registros de pagamento (parcelamento é representado como linhas separadas em `payments`). Juntar `items` e `payments` diretamente por `order_id` faz o produto cartesiano dentro de cada pedido — um pedido com 3 itens e 2 pagamentos vira **6 linhas**, e a soma de `price`/`freight_value` conta em dobro. O pior: **não gera erro nem warning**, só números errados.

**Medição com dados reais** (validado via Spark antes de codificar a Gold):

| Métrica | Valor |
|---|---|
| Linhas no grão correto (`items`) | 112.650 |
| Linhas se juntasse `items` × `payments` por `order_id` | 117.601 |
| Fator de inflação | **1,04×** (~5 mil linhas de receita duplicada) |
| Pedidos com mais de 1 registro de pagamento | 2.962 |

**Solução aplicada:** modelar **fatos separados por grão** (`fato_pedidos_itens`, `fato_pagamentos`), nunca uma tabela única. Ver [ADR-08](decisoes-arquiteturais.md) e [Modelo Dimensional](modelo-dimensional.md).

---

## 4. A pegadinha do `customer_id` vs `customer_unique_id`

**Sintoma:** qualquer métrica de "clientes recorrentes" daria **zero** se usasse a chave errada.

**Diagnóstico:** no dataset Olist, `customer_id` é **único por pedido** — o mesmo comprador que faz duas compras aparece com dois `customer_id` diferentes. Quem identifica a pessoa de verdade é `customer_unique_id`. Medição real:

| Métrica | Valor |
|---|---|
| `customer_id` distintos | 99.441 |
| `customer_unique_id` distintos | 96.096 |
| Clientes recorrentes (diferença) | **3.345** |

Usar `customer_id` para contar "clientes únicos" reportaria 99.441 pessoas quando na verdade são 96.096 — e daria 0 recorrentes.

**Solução aplicada:** `dim_cliente` carrega **os dois campos**. `customer_id` é a chave de junção com os fatos (grão de pedido); `customer_unique_id` é o que deve ser usado em qualquer análise de recorrência/pessoa.

---

## 5. `pyspark` indisponível no `python3` puro dentro do container

**Sintoma:** rodar um script de validação com `docker exec ... python3 script.py` falhava com `ModuleNotFoundError: No module named 'pyspark'`.

**Diagnóstico:** o `pyspark` não está no `site-packages` do Python do container do Airflow — ele é disponibilizado dinamicamente pelo `spark-submit`, que monta o `PYTHONPATH` (incluindo `$SPARK_HOME/python` e o `py4j`) no momento da submissão. Chamar o `python3` diretamente não passa por esse setup.

**Solução aplicada:** rodar scripts que usam Spark sempre via `spark-submit --master spark://spark-master:7077 script.py`, que é exatamente como os jobs de produção executam (é o que o `SparkSubmitOperator` faz por baixo). Para exploração interativa, usar o Jupyter (imagem `pyspark-notebook`, que já traz o pyspark no ambiente).

---

## 6. Conversão de caminhos do Git Bash no Windows (`docker exec`)

**Sintoma:** ao passar um caminho absoluto de dentro do container para o `docker exec` a partir do Git Bash no Windows, o caminho era reescrito para algo como `/opt/airflow/C:/Program Files/Git/opt/pipelines/...` e o arquivo "não era encontrado".

**Diagnóstico:** o Git Bash (MSYS2) faz **conversão automática de caminhos** POSIX→Windows nos argumentos de comandos externos. Um argumento que começa com `/opt/...` é interpretado como caminho a ser convertido, corrompendo o path destinado ao container Linux.

**Solução aplicada:** prefixar o comando com `MSYS_NO_PATHCONV=1`, que desliga essa conversão:
```bash
MSYS_NO_PATHCONV=1 docker exec <container> spark-submit ... /opt/pipelines/scripts/job.py
```

**Nota:** é uma peculiaridade do ambiente de desenvolvimento (Windows + Git Bash), não do projeto em si. Dentro dos containers (Linux) o problema não existe.

---

## 7. Credenciais hardcoded e configuração de conexão duplicada

**Sintoma:** credenciais do MinIO (`admin_rafa`) em texto puro no código, e cada script recriando a mesma configuração S3A.

**Solução aplicada:** centralizar tudo em `utils/spark_utils.py`, lendo credenciais de variáveis de ambiente, e fazer todos os scripts e notebooks importarem `create_spark_session()`. Detalhes e trade-offs em [ADR-06](decisoes-arquiteturais.md).

---

## 8. Artefatos internos do MinIO versionados por engano

**Sintoma:** arquivos `_SUCCESS/xl.meta` (metadados internos gerados pelo MinIO/Spark) apareceram rastreados no Git.

**Solução aplicada:** removê-los do índice do Git (`git rm --cached`, mantendo-os em disco) e reforçar o `.gitignore` para ignorar todo o conteúdo de dados de `lakehouse/`, preservando apenas os `.gitkeep` que marcam a estrutura de pastas. Dado de data lake não pertence ao versionamento de código.

---

## 9. Mismatch de versão Python entre driver e worker do Spark

**Sintoma:** durante o teste negativo do framework de validação, um script que criava um DataFrame sintético com `spark.createDataFrame([...])` falhou com:
```
PySparkRuntimeError: [PYTHON_VERSION_MISMATCH] Python in worker has different
version (3, 10) than that in driver 3.11
```

**Diagnóstico:** o driver dos jobs roda no container do Airflow (imagem com Python **3.11**), mas os executors rodam no container do spark-worker (imagem com Python **3.10**). O PySpark exige a mesma versão *minor* nas duas pontas — mas só quando algum trabalho precisa executar **Python nos workers**. Operações puras da DataFrame API (filter, join, cast, agregações) são compiladas para a JVM e nunca tocam o Python do worker — por isso o pipeline inteiro sempre funcionou normalmente. O problema estava latente e só apareceu quando `createDataFrame` a partir de uma lista Python local precisou serializar dados via processo Python no executor.

**Mitigação aplicada:** o framework de validação foi mantido 100% em funções nativas da DataFrame API (inclusive o checksum, que usa `xxhash64` nativo em vez de UDF Python justamente por isso), e o teste sintético passou a gerar dados via CSV + `spark.read` (mesmo caminho do código de produção). O risco continua documentado: qualquer código futuro que use UDFs Python, `toPandas()` ou `createDataFrame` local vai esbarrar nele — **no cluster**. Os testes unitários (`tests/`) usam `createDataFrame` sem problema porque rodam em Spark `local[1]`, onde driver e executor compartilham o mesmo interpretador Python (ver [CI/CD](ci-cd.md)).

**Correção definitiva (pendente):** alinhar a versão do Python entre a imagem do Airflow e a do Spark (instalar 3.11 na imagem do worker ou fixar `PYSPARK_PYTHON` para um 3.11 disponível em ambas).

**Lição:** um cluster pode estar "funcionando" e ainda assim ter incompatibilidades latentes que só certos caminhos de código exercitam. Testes negativos não validam só as regras — expõem a infraestrutura.

---

## 10. Import com efeito colateral derrubando o parse das DAGs no CI

**Sintoma:** a primeira execução do CI falhou no job `validar-dags`, com o processo morrendo no meio do `DagBag` e uma mensagem inesperada no log: `Authentication required to call the Kaggle API`.

**Diagnóstico:** o script `kaggle_to_landing.py` importava a biblioteca do Kaggle no **topo do módulo**. Essa biblioteca tem efeito colateral no import: procura a credencial e **encerra o processo** se não encontrar. A cadeia do problema: o DagBag importa a DAG → a DAG importa `scripts.kaggle_to_landing` → o módulo importa `kaggle` → sem `kaggle.json` no runner, o processo inteiro morre. No ambiente local o problema nunca apareceu porque o `kaggle.json` está sempre montado no container — mais um caso de defeito latente que só outro ambiente revela.

**Solução aplicada:** mover o import para **dentro da função da task** (`extrair_do_kaggle`). Assim ele só executa quando a task roda (onde a credencial é obrigatória de qualquer forma), e o parse da DAG fica leve e sem dependências externas — que é a recomendação oficial do Airflow para imports pesados, independentemente do CI.

**Validação:** a condição do runner foi reproduzida localmente apontando `KAGGLE_CONFIG_DIR` para uma pasta vazia dentro do container — antes da correção o parse morria; depois, `airflow dags list-import-errors` retorna limpo.

**Lição:** o parse de DAG roda em muitos contextos além do scheduler (CI, IDE, `airflow dags list`) — código de nível de módulo em arquivo de DAG precisa ser inofensivo. E o job de validação de DAGs no CI provou o próprio valor na primeira execução: pegou um problema real que o ambiente local jamais mostraria.
