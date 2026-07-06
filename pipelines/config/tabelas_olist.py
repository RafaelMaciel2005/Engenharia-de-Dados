# Registro central das tabelas do dataset Olist.
# Cada entrada mapeia o nome da tabela no lakehouse -> arquivo CSV de origem na Landing Zone.
# Para adicionar uma nova tabela ao pipeline, basta incluir uma nova entrada aqui
# (e a regra de tratamento correspondente em scripts/silver_rules.py).
TABELAS_OLIST = [
    {"nome": "customers",     "arquivo": "olist_customers_dataset.csv"},
    {"nome": "orders",        "arquivo": "olist_orders_dataset.csv"},
    {"nome": "items",         "arquivo": "olist_order_items_dataset.csv"},
    {"nome": "payments",      "arquivo": "olist_order_payments_dataset.csv"},

    # reviews tem comentários de texto livre com quebras de linha e aspas dentro do CSV;
    # sem estas opções o Spark quebraria essas linhas em registros inválidos
    {"nome": "reviews",       "arquivo": "olist_order_reviews_dataset.csv",
     "opcoes_csv": {"multiLine": "true", "quote": "\"", "escape": "\""}},

    {"nome": "products",      "arquivo": "olist_products_dataset.csv"},
    {"nome": "sellers",       "arquivo": "olist_sellers_dataset.csv"},
    {"nome": "geolocation",   "arquivo": "olist_geolocation_dataset.csv"},
    {"nome": "category_name", "arquivo": "product_category_name_translation.csv"},
]
