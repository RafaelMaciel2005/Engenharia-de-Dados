# Validação de Dados — checks, checksum e manifest

Toda tabela/objeto do pipeline passa por checks de qualidade **antes de ser gravada** na sua camada. Se qualquer check falhar, o job levanta `RuntimeError`, a task fica vermelha no Airflow e **o dado inválido nunca chega ao lakehouse** — a camada permanece com a última versão válida. A decisão de arquitetura por trás disso (e as alternativas descartadas) está no [ADR-11](decisoes-arquiteturais.md).

---

## Como funciona

```
script do pipeline (ex: bronze_to_silver.py)
    │
    ├─ 1. lê a origem e aplica a transformação
    ├─ 2. count + checksum do resultado
    ├─ 3. executar_validacoes(df, camada, tabela, spark)
    │       └─ lê config/validacoes.py e roda os checks daquela tabela
    │           ├─ todos passaram  -> segue
    │           └─ algum falhou    -> RuntimeError (task vermelha, nada é escrito)
    ├─ 4. gravar_manifest(...)  -> auditoria em s3a://logs/...
    └─ 5. escreve o Parquet na camada
```

Os módulos vivem em `utils/` e são importados pelos três scripts genéricos:

| Módulo | Responsabilidade |
|---|---|
| `utils/validation_checks.py` | Os checks em si + `executar_validacoes()` (orquestra e bloqueia) |
| `utils/validation_checksum.py` | Checksum determinístico do conteúdo do DataFrame |
| `utils/validation_manifest.py` | Grava o `manifest.json` de auditoria no bucket `logs` |
| `pipelines/config/validacoes.py` | Registro central: quais checks cada tabela roda, por camada |

---

## Tipos de check disponíveis

| Check | O que garante | Exemplo de uso |
|---|---|---|
| `row_count_min` | A tabela não veio vazia | Todas as tabelas da Bronze |
| `not_null` | Colunas de chave sem nulo (nulo em id quebra joins silenciosamente) | `order_id`, `customer_id`... |
| `unique` | A combinação de colunas é uma chave de verdade (grão respeitado) | `order_id + order_item_id` no fato |
| `accepted_values` | Valores dentro de uma lista fechada (valor "novo" ≈ erro de parse) | `order_status` com os 8 status do Olist |
| `range` | Valores dentro do intervalo esperado | `price >= 0`, `review_score` entre 1 e 5 |
| `row_count_equals_source` | Contagem do fato = contagem da origem Silver → **detecta fan-out** em join | `fato_pedidos_itens` vs `silver/items` |
| `referential` | Toda FK existe na tabela de referência (anti-join conta órfãos) | `product_id` do fato → `silver/products` |

**Escopo por camada** (nem toda regra faz sentido em todo lugar):
- **Bronze** — só volume (`row_count_min`): é dado cru, check de conteúdo aqui daria falso positivo.
- **Silver** — nulos, unicidade, domínio e range: *"a limpeza funcionou?"*
- **Gold** — unicidade do grão, integridade referencial e fan-out: *"o star schema fecha matematicamente?"*

A referência dos checks referenciais da Gold é sempre a **Silver** (camada já fechada), nunca um objeto irmão da própria Gold — as tasks da Gold rodam em paralelo, sem ordem garantida entre si.

---

## Como declarar checks para uma tabela

Tudo declarativo, em `pipelines/config/validacoes.py`:

```python
"gold": {
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
},
```

Adicionar validação a uma tabela nova = adicionar uma entrada nesse dicionário. Nenhum script muda.

---

## O manifest de auditoria

Cada execução grava um `manifest.json` no bucket `logs`, **espelhando o caminho do dado**:

```
dado:     s3a://silver/olist/orders/*.parquet
manifest: s3a://logs/silver/olist/orders/manifest.json
```

Exemplo real (gerado pelo pipeline):

```json
{
  "camada": "gold",
  "tabela": "fato_pedidos_itens",
  "linhas": 112650,
  "colunas": ["order_id:string", "order_item_id:int", "price:double", "..."],
  "checksum": "-4311417336736020894",
  "validacoes": [
    {"check": "unique:order_id+order_item_id", "passou": true, "detalhe": "0 linhas duplicadas..."},
    {"check": "row_count_equals_source:items", "passou": true, "detalhe": "112650 linhas aqui vs 112650..."}
  ],
  "gerado_em": "2026-07-13T23:57:07.814591+00:00"
}
```

**Usos práticos:**
- **Auditoria** — o que exatamente foi verificado em cada rodada fica registrado.
- **Comparação entre execuções** — checksum de hoje igual ao de ontem = o dado não mudou, sem precisar reler os Parquets. O checksum usa `xxhash64` somado por linha: determinístico e independente da ordem das linhas (o Spark não garante ordem de leitura). Não é hash criptográfico — detecta mudança acidental, não adulteração intencional.
- **Diagnóstico** — o schema gravado mostra com que colunas/tipos a tabela foi escrita naquela rodada.

O manifest é gravado via `boto3` (não via Spark) porque é um arquivo único e pequeno — `df.write` geraria uma pasta com part-file e `_SUCCESS` em vez de um JSON limpo.

---

## Resultados da primeira execução completa

| Camada | Tabelas/objetos | Checks | Falhas |
|---|---|---|---|
| Bronze | 9 | 9 | 0 |
| Silver | 9 | 25 | 0 |
| Gold | 7 | 21 | 0 |

O comportamento de bloqueio foi verificado com **teste negativo**: um dataset sintético com nulo em chave, duplicata e valor fora de range foi submetido às regras reais — todos os defeitos foram detectados e o `RuntimeError` foi levantado como esperado. Esse teste também expôs um problema latente de infraestrutura (mismatch de versão Python entre driver e worker), documentado em [Desafios Técnicos → nº 9](desafios-tecnicos.md).
