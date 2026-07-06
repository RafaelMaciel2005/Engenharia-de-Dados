# Registro central dos objetos da camada Gold (modelagem dimensional / star schema).
# Cada entrada mapeia o objeto Gold -> quais tabelas da Silver ele precisa ler.
#
# Diferente de Bronze/Silver (onde cada tabela era 1:1), na Gold um objeto pode
# combinar VÁRIAS tabelas Silver (ex: dim_produto junta products + a tradução de categoria).
# Por isso a config declara a lista de "fontes" e a função de construção (em gold_rules.py)
# recebe um dicionário {nome_da_fonte: DataFrame}.
GOLD_OBJECTS = [
    # ---- Dimensões ----
    {"nome": "dim_cliente",        "fontes": ["customers"]},
    {"nome": "dim_produto",        "fontes": ["products", "category_name"]},
    {"nome": "dim_vendedor",       "fontes": ["sellers"]},
    {"nome": "dim_tempo",          "fontes": ["orders"]},

    # ---- Fatos (grãos separados para evitar fan-out entre items e payments) ----
    {"nome": "fato_pedidos_itens", "fontes": ["orders", "items"]},
    {"nome": "fato_pagamentos",    "fontes": ["payments"]},
    {"nome": "fato_avaliacoes",    "fontes": ["reviews"]},
]
