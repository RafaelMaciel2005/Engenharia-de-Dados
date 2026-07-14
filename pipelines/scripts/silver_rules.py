# Regras de transformação Bronze -> Silver: uma função por tabela.
# Cada função recebe o DataFrame lido da Bronze e devolve o DataFrame tratado.
# O script bronze_to_silver.py busca a função certa no dicionário TRANSFORMACOES
# (no final do arquivo) — para adicionar uma tabela nova, basta escrever a função
# e registrar aqui.
#
# Padrão de tratamento adotado (o porquê de cada um):
#   - ids e códigos: trim — espaço acidental em id quebra join silenciosamente
#     ("abc " != "abc") e é invisível a olho nu no dado
#   - textos de categoria/cidade: lower + trim — "São Paulo", "sao paulo" e
#     "SAO PAULO " precisam agrupar juntos; padronizar aqui evita joins e
#     group by errados em TODAS as camadas seguintes
#   - estados (UF): upper + trim — convenção brasileira é UF maiúscula (SP, RJ)
#   - datas: to_timestamp — na Bronze tudo chega como string; converter aqui é o
#     que permite fazer datediff/agregações por período na Gold
#   - números: cast para int/double — mesma razão; string "10.5" não soma
#   - CEPs (zip_code_prefix): ficam como STRING de propósito — têm zeros à
#     esquerda ("01234") que um cast para int destruiria
from pyspark.sql.functions import col, trim, lower, upper, to_timestamp


def tratar_customers(df):
    return df \
        .withColumn("customer_id", trim(col("customer_id"))) \
        .withColumn("customer_unique_id", trim(col("customer_unique_id"))) \
        .withColumn("customer_zip_code_prefix", trim(col("customer_zip_code_prefix"))) \
        .withColumn("customer_city", lower(trim(col("customer_city")))) \
        .withColumn("customer_state", upper(trim(col("customer_state"))))


def tratar_orders(df):
    # As 5 colunas de data do pedido viram timestamp de verdade. As de entrega
    # podem ser nulas (pedido ainda não entregue) — o to_timestamp preserva o
    # nulo, e é responsabilidade da Gold decidir como tratar isso nas métricas.
    return df \
        .withColumn("order_id", trim(col("order_id"))) \
        .withColumn("customer_id", trim(col("customer_id"))) \
        .withColumn("order_status", lower(trim(col("order_status")))) \
        .withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp"))) \
        .withColumn("order_approved_at", to_timestamp(col("order_approved_at"))) \
        .withColumn("order_delivered_carrier_date", to_timestamp(col("order_delivered_carrier_date"))) \
        .withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date"))) \
        .withColumn("order_estimated_delivery_date", to_timestamp(col("order_estimated_delivery_date")))


def tratar_items(df):
    # price e freight_value são as colunas de dinheiro que alimentam o cálculo
    # de receita na Gold — se o cast falhar (formato inesperado), viram nulo,
    # e o check de range da validação pega.
    return df \
        .withColumn("order_id", trim(col("order_id"))) \
        .withColumn("order_item_id", col("order_item_id").cast("int")) \
        .withColumn("product_id", trim(col("product_id"))) \
        .withColumn("seller_id", trim(col("seller_id"))) \
        .withColumn("shipping_limit_date", to_timestamp(col("shipping_limit_date"))) \
        .withColumn("price", col("price").cast("double")) \
        .withColumn("freight_value", col("freight_value").cast("double"))


def tratar_payments(df):
    return df \
        .withColumn("order_id", trim(col("order_id"))) \
        .withColumn("payment_sequential", col("payment_sequential").cast("int")) \
        .withColumn("payment_type", lower(trim(col("payment_type")))) \
        .withColumn("payment_installments", col("payment_installments").cast("int")) \
        .withColumn("payment_value", col("payment_value").cast("double"))


def tratar_reviews(df):
    # O problema do texto multilinha dos comentários já foi resolvido na LEITURA
    # da Bronze (opcoes_csv em config/tabelas_olist.py) — aqui o dado já chega
    # íntegro e o trabalho é só de tipagem.
    return df \
        .withColumn("review_id", trim(col("review_id"))) \
        .withColumn("order_id", trim(col("order_id"))) \
        .withColumn("review_score", col("review_score").cast("int")) \
        .withColumn("review_comment_title", trim(col("review_comment_title"))) \
        .withColumn("review_comment_message", trim(col("review_comment_message"))) \
        .withColumn("review_creation_date", to_timestamp(col("review_creation_date"))) \
        .withColumn("review_answer_timestamp", to_timestamp(col("review_answer_timestamp")))


def tratar_products(df):
    # Atenção: "lenght" é um erro de escrita do PRÓPRIO dataset Olist. Mantemos
    # o nome original de propósito — renomear aqui quebraria a rastreabilidade
    # com a documentação oficial do dataset no Kaggle.
    # A categoria é normalizada (lower/trim) porque é a chave do join com a
    # tabela de tradução (category_name) lá na dim_produto da Gold.
    return df \
        .withColumn("product_id", trim(col("product_id"))) \
        .withColumn("product_category_name", lower(trim(col("product_category_name")))) \
        .withColumn("product_name_lenght", col("product_name_lenght").cast("int")) \
        .withColumn("product_description_lenght", col("product_description_lenght").cast("int")) \
        .withColumn("product_photos_qty", col("product_photos_qty").cast("int")) \
        .withColumn("product_weight_g", col("product_weight_g").cast("double")) \
        .withColumn("product_length_cm", col("product_length_cm").cast("double")) \
        .withColumn("product_height_cm", col("product_height_cm").cast("double")) \
        .withColumn("product_width_cm", col("product_width_cm").cast("double"))


def tratar_sellers(df):
    return df \
        .withColumn("seller_id", trim(col("seller_id"))) \
        .withColumn("seller_zip_code_prefix", trim(col("seller_zip_code_prefix"))) \
        .withColumn("seller_city", lower(trim(col("seller_city")))) \
        .withColumn("seller_state", upper(trim(col("seller_state"))))


def tratar_geolocation(df):
    # O arquivo original tem ~1 milhão de linhas com muita repetição EXATA
    # (mesmo CEP, mesma lat/lng, mesma cidade). O dropDuplicates no final corta
    # essa redundância — medido no sandbox, a redução é grande e não perde
    # nenhuma informação (só cópias idênticas).
    return df \
        .withColumn("geolocation_zip_code_prefix", trim(col("geolocation_zip_code_prefix"))) \
        .withColumn("geolocation_lat", col("geolocation_lat").cast("double")) \
        .withColumn("geolocation_lng", col("geolocation_lng").cast("double")) \
        .withColumn("geolocation_city", lower(trim(col("geolocation_city")))) \
        .withColumn("geolocation_state", upper(trim(col("geolocation_state")))) \
        .dropDuplicates()


def tratar_category_name(df):
    # Tabela de tradução PT -> EN das categorias. As duas colunas são normalizadas
    # com a MESMA regra usada em products (lower/trim) — é isso que garante que o
    # join da dim_produto encontra todas as categorias.
    return df \
        .withColumn("product_category_name", lower(trim(col("product_category_name")))) \
        .withColumn("product_category_name_english", lower(trim(col("product_category_name_english"))))


# Mapeamento nome da tabela -> função de tratamento.
# O script bronze_to_silver.py valida que a tabela pedida existe aqui antes de rodar.
TRANSFORMACOES = {
    "customers": tratar_customers,
    "orders": tratar_orders,
    "items": tratar_items,
    "payments": tratar_payments,
    "reviews": tratar_reviews,
    "products": tratar_products,
    "sellers": tratar_sellers,
    "geolocation": tratar_geolocation,
    "category_name": tratar_category_name,
}
