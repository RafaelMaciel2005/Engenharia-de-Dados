# Testes unitarios do framework de validacao (utils/validation_checks.py e
# utils/validation_checksum.py).
#
# A parte mais importante aqui e o teste do BLOQUEIO: o contrato do framework
# e que dado invalido derruba o job antes da escrita — se isso regredir, o
# pipeline inteiro perde a garantia de qualidade.
import pytest

from utils.validation_checks import (
    check_not_null,
    check_unique,
    check_range,
    check_accepted_values,
    executar_validacoes,
)
from utils.validation_checksum import calcular_checksum


@pytest.fixture()
def df_com_problemas(spark):
    # id nulo, id duplicado e valor negativo — um defeito de cada tipo
    return spark.createDataFrame(
        [("1", 10.0), ("1", 10.0), (None, -5.0)],
        "id string, price double",
    )


def test_not_null_detecta_nulo(df_com_problemas):
    resultado = check_not_null(df_com_problemas, "id")
    assert resultado["passou"] is False


def test_unique_detecta_duplicata(df_com_problemas):
    resultado = check_unique(df_com_problemas, ["id"])
    assert resultado["passou"] is False


def test_range_detecta_valor_negativo(df_com_problemas):
    resultado = check_range(df_com_problemas, "price", minimo=0)
    assert resultado["passou"] is False


def test_range_aceita_valores_validos(spark):
    df = spark.createDataFrame([(1,), (5,)], "review_score int")
    resultado = check_range(df, "review_score", minimo=1, maximo=5)
    assert resultado["passou"] is True


def test_accepted_values_detecta_valor_desconhecido(spark):
    df = spark.createDataFrame([("delivered",), ("status_invalido",)], "order_status string")
    resultado = check_accepted_values(df, "order_status", ["delivered", "shipped"])
    assert resultado["passou"] is False
    assert "status_invalido" in resultado["detalhe"]


def test_executar_validacoes_bloqueia_com_runtime_error(spark):
    # Usa as regras REAIS de silver/customers (not_null + unique em customer_id)
    # contra um DataFrame que viola as duas — o contrato e levantar RuntimeError.
    df_ruim = spark.createDataFrame(
        [("c1", "u1"), ("c1", "u2"), (None, "u3")],
        "customer_id string, customer_unique_id string",
    )
    with pytest.raises(RuntimeError):
        executar_validacoes(df_ruim, "silver", "customers", spark)


def test_executar_validacoes_passa_com_dado_valido(spark):
    df_bom = spark.createDataFrame(
        [("c1", "u1"), ("c2", "u2")],
        "customer_id string, customer_unique_id string",
    )
    resultados = executar_validacoes(df_bom, "silver", "customers", spark)
    assert all(r["passou"] for r in resultados)


def test_checksum_independe_da_ordem_das_linhas(spark):
    # Propriedade central do checksum: o Spark nao garante ordem de leitura,
    # entao o mesmo conteudo em ordens diferentes PRECISA dar o mesmo valor.
    df_a = spark.createDataFrame([("a", 1), ("b", 2)], "chave string, valor int")
    df_b = spark.createDataFrame([("b", 2), ("a", 1)], "chave string, valor int")
    assert calcular_checksum(df_a) == calcular_checksum(df_b)


def test_checksum_muda_quando_o_conteudo_muda(spark):
    df_a = spark.createDataFrame([("a", 1)], "chave string, valor int")
    df_b = spark.createDataFrame([("a", 2)], "chave string, valor int")
    assert calcular_checksum(df_a) != calcular_checksum(df_b)
