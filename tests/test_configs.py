# Testes de consistencia entre os registros centrais de configuracao.
#
# Sao os testes mais baratos e que mais pegam erro de manutencao no dia a dia:
# alguem adiciona uma tabela em tabelas_olist.py e esquece a regra em
# silver_rules.py (ou vice-versa) — o pipeline so descobriria em runtime.
# Aqui o CI pega antes do merge, sem precisar de Spark nem de cluster.
from config.tabelas_olist import TABELAS_OLIST
from config.gold_objects import GOLD_OBJECTS
from config.validacoes import VALIDACOES
from silver_rules import TRANSFORMACOES
from gold_rules import GOLD_BUILDERS

NOMES_TABELAS = [t["nome"] for t in TABELAS_OLIST]
NOMES_OBJETOS_GOLD = [o["nome"] for o in GOLD_OBJECTS]


def test_nomes_de_tabelas_sao_unicos():
    assert len(NOMES_TABELAS) == len(set(NOMES_TABELAS))


def test_toda_tabela_tem_regra_silver():
    sem_regra = set(NOMES_TABELAS) - set(TRANSFORMACOES)
    assert not sem_regra, f"Tabelas sem regra em silver_rules.py: {sem_regra}"


def test_toda_regra_silver_tem_tabela():
    sem_tabela = set(TRANSFORMACOES) - set(NOMES_TABELAS)
    assert not sem_tabela, f"Regras orfas em silver_rules.py (tabela nao existe na config): {sem_tabela}"


def test_todo_objeto_gold_tem_builder():
    sem_builder = set(NOMES_OBJETOS_GOLD) - set(GOLD_BUILDERS)
    assert not sem_builder, f"Objetos Gold sem funcao em gold_rules.py: {sem_builder}"


def test_todo_builder_gold_tem_objeto():
    sem_objeto = set(GOLD_BUILDERS) - set(NOMES_OBJETOS_GOLD)
    assert not sem_objeto, f"Builders orfaos em gold_rules.py (objeto nao existe na config): {sem_objeto}"


def test_fontes_dos_objetos_gold_existem_na_silver():
    # Toda fonte declarada em gold_objects.py precisa ser uma tabela real do
    # pipeline — um typo aqui viraria "path does not exist" so em runtime.
    for objeto in GOLD_OBJECTS:
        fontes_invalidas = set(objeto["fontes"]) - set(NOMES_TABELAS)
        assert not fontes_invalidas, f"{objeto['nome']} referencia fontes inexistentes: {fontes_invalidas}"


def test_validacoes_referenciam_tabelas_e_objetos_existentes():
    for camada in ("bronze", "silver"):
        desconhecidas = set(VALIDACOES.get(camada, {})) - set(NOMES_TABELAS)
        assert not desconhecidas, f"validacoes[{camada}] referencia tabelas inexistentes: {desconhecidas}"

    desconhecidos = set(VALIDACOES.get("gold", {})) - set(NOMES_OBJETOS_GOLD)
    assert not desconhecidos, f"validacoes[gold] referencia objetos inexistentes: {desconhecidos}"


def test_checks_de_origem_e_referencial_apontam_para_tabelas_silver_validas():
    # row_count_equals_source e referential leem s3a://silver/olist/<tabela> —
    # a tabela apontada precisa existir no registro central.
    for objeto, regras in VALIDACOES.get("gold", {}).items():
        origem = regras.get("row_count_equals_source")
        if origem is not None:
            assert origem in NOMES_TABELAS, f"{objeto}: row_count_equals_source aponta para '{origem}'"

        for coluna, tabela_ref in regras.get("referential", {}).items():
            assert tabela_ref in NOMES_TABELAS, f"{objeto}: referential de {coluna} aponta para '{tabela_ref}'"
