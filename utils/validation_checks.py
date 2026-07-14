# Arquivo: validation_checks.py
#
# Checks de qualidade de dados usados pelos tres scripts do pipeline
# (landing_to_bronze, bronze_to_silver e silver_to_gold).
#
# Como o fluxo funciona:
#   1. O script chama executar_validacoes(df, camada, tabela, spark) logo ANTES
#      de escrever o resultado no lakehouse.
#   2. executar_validacoes busca em pipelines/config/validacoes.py quais checks
#      aquela tabela precisa e roda um por um.
#   3. Cada check devolve um dicionario no mesmo formato:
#         {"check": nome, "passou": True/False, "detalhe": explicacao}
#      Esses dicionarios tambem sao gravados no manifest.json, entao cada execucao
#      deixa um registro auditavel do que foi verificado.
#   4. Se QUALQUER check falhar, executar_validacoes levanta RuntimeError. Isso
#      derruba o job Spark, a task fica vermelha no Airflow e o dado invalido
#      NUNCA chega a ser gravado na camada. Falha bloqueia — decisao do projeto.
from pyspark.sql import functions as F


def check_row_count_min(df, minimo):
    # Guarda-chuva mais basico de todos: pega tabela que veio vazia — por exemplo,
    # se o glob da Landing nao casou com nenhum arquivo, ou se uma extracao
    # anterior falhou em silencio e deixou o bucket sem dados.
    total = df.count()
    return {
        "check": f"row_count_min>={minimo}",
        "passou": total >= minimo,
        "detalhe": f"{total} linhas encontradas",
    }


def check_not_null(df, coluna):
    # Colunas de chave (ids) nao podem ter nulo: um id nulo quebra joins
    # e faz linhas "sumirem" em agregacoes sem ninguem perceber.
    nulos = df.filter(F.col(coluna).isNull()).count()
    return {
        "check": f"not_null:{coluna}",
        "passou": nulos == 0,
        "detalhe": f"{nulos} linhas com {coluna} nulo",
    }


def check_unique(df, colunas):
    # Garante que a combinacao de colunas informada e uma chave de verdade.
    # Se o total de linhas for maior que o total de combinacoes distintas,
    # existe duplicata — o grao da tabela esta violado.
    total = df.count()
    distintas = df.select(*colunas).distinct().count()
    duplicadas = total - distintas
    chave = "+".join(colunas)
    return {
        "check": f"unique:{chave}",
        "passou": duplicadas == 0,
        "detalhe": f"{duplicadas} linhas duplicadas na chave ({chave})",
    }


def check_accepted_values(df, coluna, valores_aceitos):
    # Compara os valores distintos da coluna contra uma lista fechada.
    # Um valor "novo" aqui quase sempre indica problema de parse do CSV
    # (linha quebrada, coluna deslocada) — nao um valor de negocio legitimo.
    encontrados = [
        row[coluna] for row in
        df.select(coluna).distinct().filter(~F.col(coluna).isin(valores_aceitos)).collect()
    ]
    return {
        "check": f"accepted_values:{coluna}",
        "passou": len(encontrados) == 0,
        "detalhe": f"valores fora do esperado: {encontrados}" if encontrados else "ok",
    }


def check_range(df, coluna, minimo=None, maximo=None):
    # Confere se os valores da coluna estao dentro do intervalo esperado.
    # Contamos cada limite separadamente (duas passadas no pior caso) porque o
    # codigo fica muito mais facil de ler — e o volume deste dataset nao justifica
    # otimizar isso.
    fora_do_range = 0
    if minimo is not None:
        fora_do_range += df.filter(F.col(coluna) < minimo).count()
    if maximo is not None:
        fora_do_range += df.filter(F.col(coluna) > maximo).count()

    return {
        "check": f"range:{coluna}[{minimo},{maximo}]",
        "passou": fora_do_range == 0,
        "detalhe": f"{fora_do_range} linhas com {coluna} fora do range",
    }


def check_row_count_equals_source(df, tabela_origem, spark, camada_origem="silver"):
    # Usado nos fatos da Gold: a contagem do fato deve ser IGUAL a da tabela
    # Silver de origem. Se vier maior, algum join multiplicou linhas (fan-out)
    # e as metricas somariam valores duplicados — sem gerar nenhum erro visivel.
    # Esse problema foi medido de verdade neste projeto: ver docs/desafios-tecnicos.md.
    total_df = df.count()
    total_origem = spark.read.parquet(f"s3a://{camada_origem}/olist/{tabela_origem}").count()
    return {
        "check": f"row_count_equals_source:{tabela_origem}",
        "passou": total_df == total_origem,
        "detalhe": f"{total_df} linhas aqui vs {total_origem} em {camada_origem}/{tabela_origem} "
                    f"(diferença = possível fan-out em algum join)",
    }


def check_referential(df, coluna, tabela_referencia, spark, camada_referencia="silver"):
    # Integridade referencial via anti-join: conta valores de `coluna` que NAO
    # existem na tabela de referencia (registros "orfaos").
    # A referencia e sempre a SILVER, e nao um objeto irmao da propria Gold,
    # porque as tasks da Gold rodam em paralelo — nao ha garantia de que
    # dim_produto ja exista quando o fato esta sendo validado.
    df_ref = spark.read.parquet(f"s3a://{camada_referencia}/olist/{tabela_referencia}").select(coluna).distinct()
    orfaos = df.select(coluna).distinct().join(df_ref, coluna, "left_anti").count()
    return {
        "check": f"referential:{coluna}->{tabela_referencia}",
        "passou": orfaos == 0,
        "detalhe": f"{orfaos} valores de {coluna} sem correspondência em {camada_referencia}/{tabela_referencia}",
    }


def executar_validacoes(df, camada, tabela, spark):
    """Roda todos os checks declarados para (camada, tabela) em config/validacoes.py.

    Retorna a lista de resultados (que o script grava no manifest). Levanta
    RuntimeError se qualquer check falhar — a falha e sempre bloqueante.
    """
    # Import local (e nao no topo do arquivo) de proposito: utils/ tambem e usado
    # pelos notebooks do Jupyter, onde a pasta pipelines/config nao esta no path.
    # Com o import aqui dentro, os notebooks podem usar os checks individuais
    # sem precisar da config completa do pipeline.
    from config.validacoes import VALIDACOES

    regras = VALIDACOES.get(camada, {}).get(tabela)
    if not regras:
        print(f"Nenhuma validacao configurada para {camada}/{tabela} — seguindo sem checks.")
        return []

    resultados = []

    # Cada bloco abaixo so roda se a regra correspondente estiver declarada na
    # config daquela tabela. A ordem segue da checagem mais barata para a mais cara.
    if "row_count_min" in regras:
        resultados.append(check_row_count_min(df, regras["row_count_min"]))

    if "not_null" in regras:
        for coluna in regras["not_null"]:
            resultados.append(check_not_null(df, coluna))

    if "unique" in regras:
        resultados.append(check_unique(df, regras["unique"]))

    if "accepted_values" in regras:
        for coluna, valores in regras["accepted_values"].items():
            resultados.append(check_accepted_values(df, coluna, valores))

    if "range" in regras:
        for coluna, (minimo, maximo) in regras["range"].items():
            resultados.append(check_range(df, coluna, minimo, maximo))

    if "row_count_equals_source" in regras:
        resultados.append(check_row_count_equals_source(df, regras["row_count_equals_source"], spark))

    if "referential" in regras:
        for coluna, tabela_ref in regras["referential"].items():
            resultados.append(check_referential(df, coluna, tabela_ref, spark))

    # Resumo legivel no log da task do Airflow — e o primeiro lugar que se olha
    # quando uma task fica vermelha.
    falhas = [r for r in resultados if not r["passou"]]
    print(f"Validacao de {camada}/{tabela}: {len(resultados)} checks, {len(falhas)} falha(s)")
    for r in resultados:
        status = "OK " if r["passou"] else "FALHOU"
        print(f"  [{status}] {r['check']} -- {r['detalhe']}")

    if falhas:
        detalhes = "; ".join(f"{f['check']} ({f['detalhe']})" for f in falhas)
        raise RuntimeError(f"Validacao de {camada}/{tabela} falhou: {detalhes}")

    return resultados
