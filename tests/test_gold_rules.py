# Testes unitarios das regras de construcao da camada Gold.
#
# O foco e proteger as duas decisoes centrais do modelo dimensional:
# o grao do fato (sem fan-out) e o tratamento de nulo nas metricas de entrega.
from datetime import datetime

from gold_rules import build_dim_cliente, build_fato_pedidos_itens


def _orders_de_teste(spark):
    return spark.createDataFrame(
        [
            # pedido entregue com atraso (entrega depois da estimativa)
            ("o1", "c1", "delivered",
             datetime(2017, 10, 2), datetime(2017, 10, 12), datetime(2017, 10, 10)),
            # pedido ainda nao entregue (datas de entrega nulas)
            ("o2", "c2", "shipped",
             datetime(2017, 11, 1), None, datetime(2017, 11, 15)),
        ],
        "order_id string, customer_id string, order_status string, "
        "order_purchase_timestamp timestamp, order_delivered_customer_date timestamp, "
        "order_estimated_delivery_date timestamp",
    )


def _items_de_teste(spark):
    return spark.createDataFrame(
        [
            ("o1", 1, "p1", "s1", 100.0, 10.0),
            ("o1", 2, "p2", "s1", 50.0, 5.0),   # o1 tem 2 itens
            ("o2", 1, "p1", "s2", 30.0, 3.0),
        ],
        "order_id string, order_item_id int, product_id string, seller_id string, "
        "price double, freight_value double",
    )


def test_dim_cliente_deduplica_por_customer_id(spark):
    df = spark.createDataFrame(
        [("c1", "u1", "01234", "sao paulo", "SP"),
         ("c1", "u1", "01234", "sao paulo", "SP")],  # duplicata exata
        "customer_id string, customer_unique_id string, customer_zip_code_prefix string, "
        "customer_city string, customer_state string",
    )
    assert build_dim_cliente({"customers": df}).count() == 1


def test_fato_pedidos_itens_mantem_o_grao_de_items(spark):
    # A garantia anti fan-out em miniatura: 3 itens de entrada -> 3 linhas no
    # fato, mesmo com o join contra orders.
    fato = build_fato_pedidos_itens({
        "orders": _orders_de_teste(spark),
        "items": _items_de_teste(spark),
    })
    assert fato.count() == 3


def test_fato_marca_atraso_e_preserva_nulo_de_nao_entregue(spark):
    fato = build_fato_pedidos_itens({
        "orders": _orders_de_teste(spark),
        "items": _items_de_teste(spark),
    })
    por_pedido = {linha["order_id"]: linha for linha in fato.collect()}

    # o1: entregue dia 12, estimativa dia 10 -> atrasado
    assert por_pedido["o1"]["entregue_com_atraso"] is True
    assert por_pedido["o1"]["atraso_entrega_dias"] == 2
    # o2: nao entregue -> NULO (e nao False), para nao contar como "no prazo"
    assert por_pedido["o2"]["entregue_com_atraso"] is None
