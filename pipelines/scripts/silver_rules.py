# Regras de transformação Bronze -> Silver: uma função por tabela.
# Cada função recebe o DataFrame lido da Bronze e devolve o DataFrame tratado.
# O script bronze_to_silver.py busca a função certa no dicionário TRANSFORMACOES (no final do arquivo).
#
# Padrão de tratamento adotado:
#   - ids e códigos: trim (remove espaços acidentais)
#   - textos de categoria/cidade: lower + trim (padroniza para comparações e joins)
#   - estados (UF): upper + trim
#   - datas: to_timestamp (na Bronze tudo chega como string)
#   - números: cast para int/double conforme o significado da coluna
from pyspark.sql.functions import col, trim, lower, upper, to_timestamp


def tratar_customers(df):
    return df \
        .withColumn("customer_id", trim(col("customer_id"))) \
        .withColumn("customer_unique_id", trim(col("customer_unique_id"))) \
        .withColumn("customer_zip_code_prefix", trim(col("customer_zip_code_prefix"))) \
        .withColumn("customer_city", lower(trim(col("customer_city")))) \
        .withColumn("customer_state", upper(trim(col("customer_state"))))


def tratar_orders(df):
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
    return df \
        .withColumn("review_id", trim(col("review_id"))) \
        .withColumn("order_id", trim(col("order_id"))) \
        .withColumn("review_score", col("review_score").cast("int")) \
        .withColumn("review_comment_title", trim(col("review_comment_title"))) \
        .withColumn("review_comment_message", trim(col("review_comment_message"))) \
        .withColumn("review_creation_date", to_timestamp(col("review_creation_date"))) \
        .withColumn("review_answer_timestamp", to_timestamp(col("review_answer_timestamp")))


def tratar_products(df):
    # Atenção: "lenght" é um erro de escrita do próprio dataset Olist, mantido de propósito
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
    # O arquivo original tem ~1 milhão de linhas com muita repetição exata,
    # por isso o dropDuplicates no final
    return df \
        .withColumn("geolocation_zip_code_prefix", trim(col("geolocation_zip_code_prefix"))) \
        .withColumn("geolocation_lat", col("geolocation_lat").cast("double")) \
        .withColumn("geolocation_lng", col("geolocation_lng").cast("double")) \
        .withColumn("geolocation_city", lower(trim(col("geolocation_city")))) \
        .withColumn("geolocation_state", upper(trim(col("geolocation_state")))) \
        .dropDuplicates()


def tratar_category_name(df):
    return df \
        .withColumn("product_category_name", lower(trim(col("product_category_name")))) \
        .withColumn("product_category_name_english", lower(trim(col("product_category_name_english"))))


# Mapeamento nome da tabela -> função de tratamento
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
