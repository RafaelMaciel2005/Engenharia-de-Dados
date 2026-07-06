# Regras de construção da camada Gold (modelagem dimensional): uma função por objeto.
# Cada função recebe um dicionário {nome_da_fonte: DataFrame lido da Silver} e devolve
# o DataFrame final (dimensão ou fato). O script silver_to_gold.py busca a função certa
# no dicionário GOLD_BUILDERS (no final do arquivo).
#
# ============================================================================
# MODELO DIMENSIONAL (star schema)
# ============================================================================
#   Fatos (medidas + chaves):
#     - fato_pedidos_itens : grão = 1 item de um pedido (receita, frete, prazo de entrega)
#     - fato_pagamentos    : grão = 1 registro de pagamento (parcelamento gera várias linhas)
#     - fato_avaliacoes    : grão = 1 avaliação (nota do cliente)
#   Dimensões (atributos descritivos):
#     - dim_cliente, dim_produto, dim_vendedor, dim_tempo
#
# Por que 3 fatos separados em vez de uma tabela única?
#   items e payments têm grãos diferentes por pedido. Juntar os dois direto por order_id
#   multiplica as linhas (fan-out) e inflaria receita/frete silenciosamente. Mantendo
#   fatos separados, cada métrica é somada no seu grão nativo e os números batem.
# ============================================================================
from pyspark.sql import functions as F


# ----------------------------------------------------------------------------
# DIMENSÕES
# ----------------------------------------------------------------------------

def build_dim_cliente(dfs):
    # Grão: 1 linha por customer_id.
    # ATENÇÃO (pegadinha do Olist): customer_id é único POR PEDIDO — o mesmo comprador
    # que faz 2 pedidos aparece com 2 customer_id diferentes. Quem identifica a pessoa
    # de verdade é customer_unique_id. Ambos ficam na dimensão; qualquer métrica de
    # "cliente recorrente" deve agrupar por customer_unique_id, nunca por customer_id.
    return dfs["customers"] \
        .select(
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ) \
        .dropDuplicates(["customer_id"]) \
        .withColumn("dt_criacao_gold", F.current_timestamp())


def build_dim_produto(dfs):
    # Grão: 1 linha por product_id, já com a categoria traduzida para o inglês.
    # O join com category_name funciona porque na Silver normalizamos product_category_name
    # (lower/trim) nas DUAS tabelas — as chaves batem. Left join preserva produtos sem
    # categoria cadastrada (ficam com a tradução nula).
    return dfs["products"] \
        .select(
            "product_id",
            "product_category_name",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ) \
        .join(dfs["category_name"], "product_category_name", "left") \
        .withColumn("dt_criacao_gold", F.current_timestamp())


def build_dim_vendedor(dfs):
    # Grão: 1 linha por seller_id.
    return dfs["sellers"] \
        .select(
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state",
        ) \
        .dropDuplicates(["seller_id"]) \
        .withColumn("dt_criacao_gold", F.current_timestamp())


def build_dim_tempo(dfs):
    # Grão: 1 linha por data de compra distinta.
    # Derivamos o calendário a partir das datas reais de pedido (order_purchase_timestamp),
    # que é a chave de tempo usada pela fato de pedidos.
    return dfs["orders"] \
        .select(F.to_date("order_purchase_timestamp").alias("data")) \
        .dropna() \
        .distinct() \
        .withColumn("ano", F.year("data")) \
        .withColumn("mes", F.month("data")) \
        .withColumn("dia", F.dayofmonth("data")) \
        .withColumn("trimestre", F.quarter("data")) \
        .withColumn("dia_semana", F.dayofweek("data")) \
        .withColumn("dt_criacao_gold", F.current_timestamp())


# ----------------------------------------------------------------------------
# FATOS
# ----------------------------------------------------------------------------

def build_fato_pedidos_itens(dfs):
    # Grão: 1 linha por ITEM de pedido (mesmo grão de silver/items).
    # Juntamos o cabeçalho do pedido (orders, 1 linha por order_id) para trazer cliente,
    # status, data e as métricas de prazo de entrega — sem fan-out, pois orders é 1:1 com order_id.
    pedidos = dfs["orders"].select(
        "order_id",
        "customer_id",
        "order_status",
        F.to_date("order_purchase_timestamp").alias("data_pedido"),
        # Prazo real de entrega (dias entre compra e entrega ao cliente)
        F.datediff("order_delivered_customer_date", "order_purchase_timestamp").alias("prazo_entrega_dias"),
        # Atraso vs. estimativa: positivo = entregue DEPOIS do previsto
        F.datediff("order_delivered_customer_date", "order_estimated_delivery_date").alias("atraso_entrega_dias"),
    )

    return dfs["items"] \
        .select("order_id", "order_item_id", "product_id", "seller_id", "price", "freight_value") \
        .join(pedidos, "order_id", "inner") \
        .withColumn(
            # Se o pedido não tem data de entrega (não entregue), o atraso é nulo —
            # marcamos como NULL, e não como "no prazo", para não distorcer a métrica.
            "entregue_com_atraso",
            F.when(F.col("atraso_entrega_dias").isNull(), None)
             .otherwise(F.col("atraso_entrega_dias") > 0),
        ) \
        .withColumn("dt_criacao_gold", F.current_timestamp())


def build_fato_pagamentos(dfs):
    # Grão: 1 linha por registro de pagamento (um pedido parcelado gera várias linhas).
    return dfs["payments"] \
        .select(
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value",
        ) \
        .withColumn("dt_criacao_gold", F.current_timestamp())


def build_fato_avaliacoes(dfs):
    # Grão: 1 linha por avaliação. order_id é a chave para cruzar com fato_pedidos_itens
    # (ex: analisar se atraso na entrega derruba a nota).
    return dfs["reviews"] \
        .select(
            "review_id",
            "order_id",
            "review_score",
            F.to_date("review_creation_date").alias("data_avaliacao"),
            # Tempo de resposta do vendedor à avaliação (em dias)
            F.datediff("review_answer_timestamp", "review_creation_date").alias("dias_ate_resposta"),
        ) \
        .withColumn("dt_criacao_gold", F.current_timestamp())


# Mapeamento nome do objeto -> função de construção
GOLD_BUILDERS = {
    "dim_cliente": build_dim_cliente,
    "dim_produto": build_dim_produto,
    "dim_vendedor": build_dim_vendedor,
    "dim_tempo": build_dim_tempo,
    "fato_pedidos_itens": build_fato_pedidos_itens,
    "fato_pagamentos": build_fato_pagamentos,
    "fato_avaliacoes": build_fato_avaliacoes,
}
