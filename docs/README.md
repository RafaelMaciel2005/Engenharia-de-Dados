# Documentação do Projeto

Esta pasta reúne a documentação técnica do lakehouse Olist — não *o que* o código faz (isso os próprios scripts e comentários mostram), mas **por que** foi feito assim e **quais problemas** apareceram no caminho.

## Índice

| Documento | Conteúdo |
|---|---|
| [Decisões Arquiteturais](decisoes-arquiteturais.md) | Registro das principais escolhas de arquitetura (formato ADR): contexto, decisão e trade-offs de cada uma. |
| [Desafios Técnicos](desafios-tecnicos.md) | Problemas reais enfrentados durante o desenvolvimento e como foram diagnosticados e resolvidos. |
| [Modelo Dimensional (camada Gold)](modelo-dimensional.md) | Documentação do star schema: grão de cada fato, dimensões, perguntas de negócio respondidas e queries de exemplo com resultados reais. |
| [Validação de Dados](validacao-de-dados.md) | O framework de qualidade do pipeline: tipos de check, falha bloqueante, checksum e o manifest de auditoria por tabela. |
| [CI/CD](ci-cd.md) | As esteiras do GitHub Actions: o que o CI verifica (lint, testes com Spark local, DagBag), o que o CD publica (imagens no GHCR) e como rodar tudo localmente. |

## Para quem lê este projeto

Se você está avaliando este repositório (recrutador, revisor técnico, ou eu mesmo daqui a alguns meses), a leitura recomendada é:

1. [README principal](../README.md) — visão geral, arquitetura e como rodar.
2. [Modelo Dimensional](modelo-dimensional.md) — o "produto final" do pipeline e o valor de negócio que ele entrega.
3. [Decisões Arquiteturais](decisoes-arquiteturais.md) e [Desafios Técnicos](desafios-tecnicos.md) — o raciocínio de engenharia por trás do código.
