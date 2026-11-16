# Requisitos e critérios de aceite

Este documento define o escopo do laboratório e como avaliar sua execução. Requisito, configuração e estado operacional não equivalem automaticamente a homologação funcional.

## Requisitos funcionais

| ID | Requisito | Artefatos |
|---|---|---|
| RF-01 | Capturar eventos CDC de customers, accounts, cards e transactions. | `database/init/`, `infrastructure/connectors/cdc/` |
| RF-02 | Publicar eventos nos tópicos Kafka correspondentes. | CDC Source V2 |
| RF-03 | Declarar metadados de chave necessários ao Flink. | `flink/sql/00-*` |
| RF-04 | Enriquecer transações com accounts por Temporal Join. | `flink/sql/01-enrichment.sql` |
| RF-05 | Preparar a entrada append usada pela detecção. | `flink/sql/02-append-only.sql` |
| RF-06 | Detectar três ou mais eventos positivos do mesmo cartão em até 60 segundos de `$rowtime`. | `flink/sql/04-fraud-detection.sql` |
| RF-07 | Publicar alertas no tópico Kafka `fraud_alerts`. | Flink SQL |
| RF-08 | Preservar evidências de execução e verificações operacionais. | `operations/`, `evidence/` |
| RF-09 | Persistir alertas em PostgreSQL por Sink com UPSERT. | `infrastructure/connectors/sink/`, `database/` |

## Requisitos técnicos

- PostgreSQL de origem compatível com logical replication e acessível pelo conector.
- Confluent Cloud com Kafka, Connect, Schema Registry e Flink.
- Confluent CLI autenticada e ferramentas Bash, `psql`, `jq` e `curl`.
- Segredos e configurações locais fora do Git.
- Dados sintéticos determinísticos: 200 customers, 240 accounts, 320 cards e 2.000 transactions.
- PostgreSQL Sink com role dedicada e tabela de destino previamente criada.

## Critérios de aceite e estado registrado

| ID | Critério | Estado sustentado pela evidência |
|---|---|---|
| CA-01 | Conector CDC operacional. | Estado RUNNING registrado em `evidence/cloud/03-cdc-connector.txt`. |
| CA-02 | Snapshot das quatro tabelas (200/240/320/2.000). | Comprovado: 2.760 eventos `op=r`, resumo em `evidence/homologation/20261115T203833Z-34858-cdc-snapshot/summary.txt`. |
| CA-03 | CDC incremental INSERT/UPDATE/DELETE. | Comprovado com `snapshot=false`, partição e offsets em `evidence/homologation/20261115T210658Z-cdc-incremental.txt`. |
| CA-04 | Evolução compatível do subject Avro real. | Comprovada: versão 1→2, BACKWARD e evento no offset 2000 em `evidence/homologation/20261115T210745Z-real-schema-evolution.txt`. |
| CA-05 | Statements Flink executados e encerrados. | Execução registrada; enrichment e detecção posteriormente confirmados STOPPED em `evidence/homologation/20261115T212311Z-flink-statements-stop.txt`. |
| CA-06 | Enriquecimento funcional e detecção correlacionada ao cenário positivo. | Parcial. Alertas foram gerados, mas não há correlação inequívoca entre todos os alertas e os INSERTs de teste. |
| CA-07 | Mensagem no tópico de alertas. | Mensagem Avro lida em partição 3, offset 0; não correlacionada ao cenário positivo específico. |
| CA-08 | Sink e persistência. | Connector/task RUNNING e consulta ao destino com registros. Não há teste controlado de reentrega nem correlação de uma mensagem específica com uma linha. |
| CA-09 | Segurança e observabilidade. | ACLs/RBAC aprovados; métricas e Billing coletados. Evidências em `evidence/observability/`. |
| CA-10 | Teardown. | Confluent confirmado ausente. Limpeza Neon parcial: falha ao remover role `cdc`; teardown integral não comprovado. |

## Limitações de interpretação

- `NOT ENFORCED` declara metadados de chave, mas não valida unicidade na origem.
- `$rowtime` é o tempo de evento Kafka, não `created_at`.
- Append não equivale a filtrar exclusivamente INSERTs; atualizações CDC podem contribuir como novas linhas.
- A regra é demonstrativa e opera sobre dados sintéticos. Alertas não comprovam fraude real.
- A homologação positiva exige correlacionar IDs/offsets dos eventos de entrada com o alerta. Cartão, contagem e janela temporal, isoladamente, podem aceitar alertas preexistentes.
- O Sink RUNNING não comprova persistência nem exactly-once. UPSERT pode tornar reentregas com a mesma chave idempotentes, mas não oferece garantia ponta a ponta.
- O teardown é independente por provedor. A falha de uma etapa impede declarar a limpeza integral concluída.

Consulte [Homologação e aprendizados](plano-homologacao.md) e as evidências primárias em `evidence/`.
