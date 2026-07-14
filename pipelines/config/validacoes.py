# Registro central de validações de qualidade de dados, por camada e tabela/objeto.
# utils/validation_checks.py lê esta config e roda os checks declarados.
#
# Tipos de check disponíveis:
#   row_count_min             : numero minimo de linhas esperado
#   not_null                  : lista de colunas que nao podem ter nulo
#   unique                    : lista de colunas cuja combinacao deve ser unica (chave)
#   accepted_values           : {coluna: [valores permitidos]}
#   range                     : {coluna: (minimo, maximo)} -- None = sem limite naquele lado
#   row_count_equals_source   : nome da tabela na Silver que deve ter a MESMA contagem
#                                (usado nos fatos da Gold para detectar fan-out em joins)
#   referential                : {coluna: tabela_silver_de_referencia} -- toda linha aqui
#                                 precisa ter um valor de `coluna` que exista na tabela referenciada
#
# Escopo por camada (nem toda regra faz sentido em todo lugar):
#   Bronze : so row_count_min -- e dado cru, checks de conteudo aqui dariam falso positivo
#   Silver : not_null / unique / accepted_values / range -- "a limpeza funcionou?"
#   Gold   : unicidade do grao + referencial + row_count_equals_source -- "o star schema fecha?"
VALIDACOES = {
    "bronze": {
        "customers":     {"row_count_min": 1},
        "orders":        {"row_count_min": 1},
        "items":         {"row_count_min": 1},
        "payments":      {"row_count_min": 1},
        "reviews":       {"row_count_min": 1},
        "products":      {"row_count_min": 1},
        "sellers":       {"row_count_min": 1},
        "geolocation":   {"row_count_min": 1},
        "category_name": {"row_count_min": 1},
    },

    "silver": {
        "customers": {
            "not_null": ["customer_id", "customer_unique_id"],
            "unique": ["customer_id"],
        },
        "orders": {
            "not_null": ["order_id", "customer_id"],
            "unique": ["order_id"],
            "accepted_values": {
                "order_status": [
                    "delivered", "shipped", "canceled", "unavailable",
                    "invoiced", "processing", "created", "approved",
                ],
            },
        },
        "items": {
            "not_null": ["order_id", "product_id", "seller_id"],
            "unique": ["order_id", "order_item_id"],
            "range": {"price": (0, None), "freight_value": (0, None)},
        },
        "payments": {
            "not_null": ["order_id", "payment_type"],
            "range": {"payment_value": (0, None)},
        },
        "reviews": {
            "not_null": ["review_id", "order_id"],
            "range": {"review_score": (1, 5)},
        },
        "products": {
            "not_null": ["product_id"],
            "unique": ["product_id"],
        },
        "sellers": {
            "not_null": ["seller_id"],
            "unique": ["seller_id"],
        },
        "geolocation": {
            "not_null": ["geolocation_zip_code_prefix"],
        },
        "category_name": {
            "not_null": ["product_category_name"],
        },
    },

    "gold": {
        "dim_cliente":  {"unique": ["customer_id"]},
        "dim_produto":  {"unique": ["product_id"]},
        "dim_vendedor": {"unique": ["seller_id"]},
        "dim_tempo":    {"unique": ["data"]},

        "fato_pedidos_itens": {
            "not_null": ["order_id", "product_id", "seller_id", "customer_id"],
            "unique": ["order_id", "order_item_id"],
            "range": {"price": (0, None), "freight_value": (0, None)},
            "row_count_equals_source": "items",
            "referential": {
                "product_id": "products",
                "seller_id": "sellers",
                "customer_id": "customers",
            },
        },
        "fato_pagamentos": {
            "not_null": ["order_id"],
            "range": {"payment_value": (0, None)},
            "row_count_equals_source": "payments",
        },
        "fato_avaliacoes": {
            "not_null": ["order_id"],
            "range": {"review_score": (1, 5)},
            "row_count_equals_source": "reviews",
        },
    },
}
