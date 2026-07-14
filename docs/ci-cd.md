# CI/CD — GitHub Actions

O projeto tem duas esteiras no GitHub Actions, com papéis distintos. O racional da estratégia (e por que o "CD" daqui publica imagens em vez de fazer deploy) está no [ADR-12](decisoes-arquiteturais.md).

---

## CI — `.github/workflows/ci.yml`

Roda a cada push nas branches de trabalho (`main`, `rafa-dev`) e em todo PR para a `main`. São três jobs independentes — se um falhar, os outros ainda reportam:

| Job | O que verifica | Por que importa |
|---|---|---|
| `lint` | `ruff check` em todo o código | Erros de sintaxe, imports quebrados, variáveis não usadas — o básico que não deveria chegar a um PR |
| `testes` | `pytest` com **Spark local** | Regras Silver/Gold, checks de validação e consistência das configs (ver abaixo) |
| `validar-dags` | `DagBag` do Airflow importa as DAGs de verdade | Pega o erro clássico de "DAG quebrada" (import que falha, config faltando) antes de chegar no scheduler |

### O que os testes cobrem

- **Consistência das configs** (sem Spark, os mais baratos): toda tabela em `tabelas_olist.py` tem regra em `silver_rules.py` e vice-versa; todo objeto Gold tem builder; as fontes declaradas em `gold_objects.py` existem; as validações referenciam tabelas/objetos reais. É o teste que pega o erro de manutenção mais comum: adicionar uma tabela e esquecer a regra.
- **Regras Silver** (Spark local): normalização de texto, CEP preservando zero à esquerda, datas virando timestamp com nulo preservado, casts monetários, dedup do geolocation.
- **Regras Gold** (Spark local): dedup da `dim_cliente`, **grão do fato preservado (anti fan-out em miniatura)** e o nulo de "não entregue" que não vira "no prazo".
- **Framework de validação** (Spark local): cada check detecta a violação que promete detectar, `executar_validacoes` **bloqueia com `RuntimeError`** usando as regras reais, e o checksum é independente da ordem das linhas.

### Detalhe técnico importante: Spark local nos testes

Os testes usam `SparkSession` em `local[1]` — driver e executor no mesmo processo. Isso tem dois efeitos deliberados:

1. **Nenhuma dependência de infraestrutura**: os testes não precisam de MinIO, cluster nem credenciais — rodam em qualquer runner do GitHub com Java 17 + `pyspark` instalado (fixado na mesma versão do cluster, 3.5.1).
2. **Imunidade ao mismatch de Python**: como driver e executor compartilham o mesmo interpretador, o problema de versão Python driver/worker do cluster (ver [Desafios Técnicos → nº 9](desafios-tecnicos.md)) não afeta os testes — e é por isso que aqui pode-se usar `createDataFrame` livremente.

---

## CD — `.github/workflows/cd.yml`

| Evento | Comportamento |
|---|---|
| PR para `main` tocando `infrastructure/docker/**` | **Builda** as imagens (Airflow e Spark) — valida que os Dockerfiles continuam construindo, sem publicar nada |
| Push na `main` tocando `infrastructure/docker/**` | Builda e **publica** no GitHub Container Registry: `ghcr.io/rafaelmaciel2005/olist-lakehouse-{airflow,spark}` com tags `latest` e o SHA do commit |

O build usa cache de camadas entre execuções (`type=gha`) porque o download do Spark dentro do Dockerfile é pesado (~400 MB) — sem cache, cada build repetiria tudo.

### Por que isso é o "CD" do projeto

Este projeto roda em `docker-compose` local — não existe ambiente de produção para fazer deploy. O artefato entregável de um pipeline containerizado são as **imagens versionadas**: é exatamente o que um deploy futuro em Kubernetes (pasta `infrastructure/kubernetes/`, no roadmap) consumiria. Publicar a imagem com a tag do commit dá rastreabilidade ("qual código está nessa imagem?") e reprodutibilidade (qualquer versão pode ser puxada de volta).

---

## Rodando localmente o que o CI roda

```bash
pip install -r requirements-dev.txt

ruff check .    # lint
pytest          # testes (precisa de Java 17 no PATH para o Spark local)
```

Configurações de lint e pytest ficam no `pyproject.toml` da raiz.
