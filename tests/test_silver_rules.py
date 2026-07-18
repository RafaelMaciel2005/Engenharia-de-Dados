# Testes unitarios das regras de transformacao Bronze -> Silver.
#
# Cada teste monta um DataFrame pequeno em memoria com um caso que ja deu (ou
# daria) problema de verdade, aplica a regra e confere o resultado. Nao precisa
# de MinIO nem de cluster — e exatamente o tipo de teste que roda no CI.
from silver_rules import tratar_customers, tratar_orders, tratar_items, tratar_geolocation


def test_customers_normaliza_texto_e_preserva_zeros_do_cep(spark):
    df = spark.createDataFrame(
        [("  c1  ", "u1", "01234", "  São Paulo ", " sp ")],
        "customer_id string, customer_unique_id string, customer_zip_code_prefix string, "
        "customer_city string, customer_state string",
    )
    linha = tratar_customers(df).first()

    assert linha["customer_id"] == "c1"                    # trim aplicado
    assert linha["customer_city"] == "são paulo"           # lower + trim
    assert linha["customer_state"] == "SP"                 # upper + trim
    # A regra de ouro do CEP: continua string e mantem o zero a esquerda
    assert linha["customer_zip_code_prefix"] == "01234"


def test_orders_converte_datas_e_preserva_nulo_de_pedido_nao_entregue(spark):
    df = spark.createDataFrame(
        [("o1", "c1", " DELIVERED ", "2017-10-02 10:56:33", None, None, None, "2017-10-10 00:00:00")],
        "order_id string, customer_id string, order_status string, order_purchase_timestamp string, "
        "order_approved_at string, order_delivered_carrier_date string, "
        "order_delivered_customer_date string, order_estimated_delivery_date string",
    )
    resultado = tratar_orders(df)
    linha = resultado.first()

    assert linha["order_status"] == "delivered"
    # Datas viram timestamp de verdade (nao string)
    assert dict(resultado.dtypes)["order_purchase_timestamp"] == "timestamp"
    assert linha["order_purchase_timestamp"].year == 2017
    # Pedido sem entrega: o nulo atravessa a conversao sem virar erro
    assert linha["order_delivered_customer_date"] is None


def test_items_converte_valores_monetarios_para_double(spark):
    df = spark.createDataFrame(
        [("o1", "1", "p1", "s1", "2017-09-19 09:45:35", "58.90", "13.29")],
        "order_id string, order_item_id string, product_id string, seller_id string, "
        "shipping_limit_date string, price string, freight_value string",
    )
    resultado = tratar_items(df)
    tipos = dict(resultado.dtypes)
    linha = resultado.first()

    assert tipos["price"] == "double"
    assert tipos["order_item_id"] == "int"
    assert linha["price"] == 58.90


def test_geolocation_remove_duplicatas_exatas(spark):
    # O arquivo real tem muita repeticao exata — a regra precisa deduplicar
    df = spark.createDataFrame(
        [
            ("01037", "-23.54", "-46.63", "sao paulo", "SP"),
            ("01037", "-23.54", "-46.63", "sao paulo", "SP"),  # copia identica
            ("01046", "-23.55", "-46.64", "sao paulo", "SP"),
        ],
        "geolocation_zip_code_prefix string, geolocation_lat string, geolocation_lng string, "
        "geolocation_city string, geolocation_state string",
    )
    assert tratar_geolocation(df).count() == 2
