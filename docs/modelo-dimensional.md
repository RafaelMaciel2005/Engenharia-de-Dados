# Modelo Dimensional — Camada Gold

A camada Gold organiza os dados tratados da Silver em um **star schema** (esquema estrela): tabelas-**fato** (medidas de negócio + chaves) cercadas por tabelas-**dimensão** (atributos descritivos). É o formato pensado para consumo analítico e BI.

Todos os números neste documento foram **medidos com dados reais** processados pelo pipeline (dataset Olist completo, ~100 mil pedidos).

---

## Perguntas de negócio que a Gold responde

1. Qual a **receita** e o **ticket médio** por estado, categoria ou período?
2. Qual o **prazo de entrega** real vs. estimado, e a **taxa de atraso**?
3. O **atraso na entrega** impacta a **nota de avaliação** do cliente?
4. Como se distribuem as **formas de pagamento** e o **parcelamento**?
5. Quantos **clientes são recorrentes** (mais de um pedido)?

---

## Visão geral do esquema

```
        dim_cliente        dim_produto        dim_vendedor       dim_tempo
             |                  |                   |                 |
             |   +--------------+                   |                 |
             |   |              |                   |                 |
        +----------------- fato_pedidos_itens ------------------------+
        |    (grão: 1 item de pedido — receita, frete, prazo)
        |
        |  order_id (cruza com os outros fatos)
        |
   fato_pagamentos                    fato_avaliacoes
   (grão: 1 pagamento)                (grão: 1 avaliação)
```

Os três fatos se relacionam entre si por `order_id` — é assim que se respondem perguntas cruzadas (ex: atraso × nota).

---

## Fatos

### `fato_pedidos_itens`
**Grão:** 1 linha = 1 item de um pedido (mesmo grão de `silver/items`).
**Fontes:** `orders` + `items`.
**Contagem real:** 112.650 linhas (idêntica ao total de itens — **confirmação de que não houve fan-out**).

| Coluna | Tipo | Descrição |
|---|---|---|
| `order_id` | string | Chave do pedido (cruza com os demais fatos) |
| `order_item_id` | int | Sequência do item dentro do pedido |
| `product_id` | string | FK → `dim_produto` |
| `seller_id` | string | FK → `dim_vendedor` |
| `customer_id` | string | FK → `dim_cliente` |
| `data_pedido` | date | FK → `dim_tempo` (data da compra) |
| `order_status` | string | Status do pedido |
| `price` | double | **Medida:** valor do item |
| `freight_value` | double | **Medida:** frete do item |
| `prazo_entrega_dias` | int | **Medida derivada:** dias entre compra e entrega ao cliente |
| `atraso_entrega_dias` | int | **Medida derivada:** dias entre entrega real e estimada (positivo = atrasado) |
| `entregue_com_atraso` | boolean | `true`/`false`, ou **`null`** se o pedido não foi entregue |

> **Tratamento de nulo relevante:** pedidos sem data de entrega registrada (2.965 de 99.441 ≈ 2,98%) ficam com `entregue_com_atraso = null` — deliberadamente **não** marcados como "no prazo", para não distorcer a métrica de pontualidade.

### `fato_pagamentos`
**Grão:** 1 linha = 1 registro de pagamento (um pedido parcelado gera várias linhas).
**Fonte:** `payments`. **Contagem real:** 103.886 linhas.

| Coluna | Tipo | Descrição |
|---|---|---|
| `order_id` | string | FK do pedido |
| `payment_sequential` | int | Sequência do pagamento no pedido |
| `payment_type` | string | Forma de pagamento (`credit_card`, `boleto`, ...) |
| `payment_installments` | int | Número de parcelas |
| `payment_value` | double | **Medida:** valor pago |

### `fato_avaliacoes`
**Grão:** 1 linha = 1 avaliação.
**Fonte:** `reviews`. **Contagem real:** 99.224 linhas.

| Coluna | Tipo | Descrição |
|---|---|---|
| `review_id` | string | Identificador da avaliação |
| `order_id` | string | FK do pedido (cruza com `fato_pedidos_itens`) |
| `review_score` | int | **Medida:** nota de 1 a 5 |
| `data_avaliacao` | date | Data de criação da avaliação |
| `dias_ate_resposta` | int | **Medida derivada:** dias até o vendedor responder |

---

## Dimensões

| Dimensão | Grão | Contagem real | Atributos principais |
|---|---|---|---|
| `dim_cliente` | 1 por `customer_id` | 99.441 | `customer_unique_id` (pessoa real), cidade, estado, CEP |
| `dim_produto` | 1 por `product_id` | 32.951 | categoria (PT), categoria (EN, via join), peso e dimensões físicas |
| `dim_vendedor` | 1 por `seller_id` | 3.095 | cidade, estado, CEP |
| `dim_tempo` | 1 por data de compra | 634 | ano, mês, dia, trimestre, dia da semana |

> **`dim_cliente` carrega dois identificadores** de propósito: `customer_id` (único por pedido, usado como FK nos fatos) e `customer_unique_id` (identifica a pessoa). Métricas de recorrência devem usar o segundo — ver [Desafios Técnicos → customer_unique_id](desafios-tecnicos.md).

> **`dim_produto` só tem a categoria em inglês** porque na Silver normalizamos (`lower`/`trim`) a coluna `product_category_name` **nas duas tabelas** (`products` e `category_name`) — sem isso, o join da tradução falharia por diferença de caixa/espaços. É um exemplo de decisão na Silver que só "paga" na Gold.

---

## Linhagem (quais tabelas Silver alimentam cada objeto Gold)

| Objeto Gold | Fontes (Silver) |
|---|---|
| `dim_cliente` | `customers` |
| `dim_produto` | `products` + `category_name` |
| `dim_vendedor` | `sellers` |
| `dim_tempo` | `orders` |
| `fato_pedidos_itens` | `orders` + `items` |
| `fato_pagamentos` | `payments` |
| `fato_avaliacoes` | `reviews` |

Esse mapeamento é declarado em `pipelines/config/gold_objects.py`.

---

## Queries de exemplo (resultados reais)

### Receita e ticket médio por estado (top 5)
Join de `fato_pedidos_itens` com `dim_cliente`:

| Estado | Receita (R$) | Ticket médio (R$) | Pedidos |
|---|---|---|---|
| SP | 5.202.955,05 | 109,65 | 41.375 |
| RJ | 1.824.092,67 | 125,12 | 12.762 |
| MG | 1.585.308,03 | 120,75 | 11.544 |
| RS | 750.304,02 | 120,34 | 5.432 |
| PR | 683.083,76 | 119,00 | 4.998 |

### O atraso na entrega derruba a avaliação?
Join **cross-fact** de `fato_avaliacoes` com `fato_pedidos_itens` por `order_id`:

| Situação da entrega | Nota média | Avaliações |
|---|---|---|
| No prazo | **4,29** | 89.949 |
| Com atraso | **2,27** | 6.410 |
| Não entregue | **1,77** | 2.106 |

> Este resultado valida na prática o modelo dimensional: dois fatos de grãos independentes, cruzados por `order_id`, entregam um insight de negócio claro — **atraso e não-entrega derrubam drasticamente a satisfação** (de 4,29 para 2,27 e 1,77). É exatamente o tipo de análise que a separação de fatos (em vez de uma tabela única com fan-out) torna correta e confiável.

As garantias estruturais do modelo são protegidas em duas frentes: **em runtime**, pelos checks da camada Gold (unicidade do grão, integridade referencial e `row_count_equals_source` contra o fan-out — ver [Validação de Dados](validacao-de-dados.md)); e **em tempo de desenvolvimento**, por testes unitários no CI (`tests/test_gold_rules.py`) que verificam o grão do fato e o tratamento de nulo de "não entregue" a cada push.

---

## Como reproduzir

Com o ambiente no ar, as queries acima podem ser executadas no Jupyter (`http://localhost:8888`) lendo os Parquet da Gold:

```python
from utils.spark_utils import create_spark_session
from pyspark.sql import functions as F

spark = create_spark_session("Gold-Analise")
fato = spark.read.parquet("s3a://gold/olist/fato_pedidos_itens")
dim_cliente = spark.read.parquet("s3a://gold/olist/dim_cliente")

fato.join(dim_cliente.select("customer_id", "customer_state"), "customer_id") \
    .groupBy("customer_state") \
    .agg(F.round(F.sum("price"), 2).alias("receita")) \
    .orderBy(F.desc("receita")) \
    .show(5)
```
