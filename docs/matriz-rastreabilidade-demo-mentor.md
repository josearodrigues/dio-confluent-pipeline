# Rastreabilidade da demonstração e aprendizados

Este documento mantém a correspondência funcional entre a demonstração de referência e os componentes do projeto, sem diário cronológico de implantação. A execução reproduzível está no [README](../README.md).

## Correspondência entre etapas

| Etapa conceitual | Implementação neste projeto | Aprendizado / limite |
|---|---|---|
| Banco de origem | PostgreSQL no Neon, schema `payments` e dados sintéticos. | A conectividade direta, logical replication e escopo do banco precisam ser conferidos antes do CDC. |
| Captura CDC | Confluent PostgreSQL CDC Source V2 para customers, accounts, cards e transactions. | Snapshot e CDC incremental são validações distintas. |
| Snapshot inicial | Verificação das contagens de eventos `op=r`. | Não iniciar a regra de fraude antes de concluir a carga inicial. |
| CDC incremental | Teste correlacionado de INSERT, UPDATE e DELETE. | Verificar `op`, `snapshot=false`, partição e offset; o estado RUNNING do conector não substitui a leitura dos eventos. |
| Schema Registry | Teste de compatibilidade Avro e evolução do subject real do CDC. | O subject isolado de teste não substitui a evolução do subject efetivamente produzido pelo conector. |
| Temporal Join | Enriquecimento de transactions com accounts por `$rowtime`. | Tempo de evento não é o timestamp de negócio. |
| Append | Alteração de changelog de `enriched_payments`. | Append não é sinônimo de filtrar somente INSERTs; updates podem ser representados como novas linhas. |
| Detecção | `MATCH_RECOGNIZE` por cartão, três ou mais valores positivos em até 60 segundos. | A regra é heurística didática; precisa de correlação de entrada e saída para ser homologada. |
| Saída | Tópico Kafka `fraud_alerts`. | Inspecionar partição, offset, chave e conteúdo; uma mensagem antiga não comprova alerta novo. |
| Persistência downstream | PostgreSQL Sink Avro com UPSERT. | RUNNING não comprova escrita; comparar uma mensagem específica com o registro persistido. |
| Observabilidade | Metrics API, ACLs/RBAC e Costs API. | Métricas, permissões e custos respondem perguntas diferentes e devem ser analisados separadamente. |
| Encerramento | Teardown Confluent e limpeza seletiva Neon. | Cada provedor precisa de confirmação independente; falha parcial deve permanecer explícita. |

## Adaptações relevantes

- O projeto mantém nomes próprios de Environment, cluster, tópicos, statements e tabelas; não reutiliza identificadores do ambiente de demonstração.
- O snapshot é validado antes de iniciar Flink.
- O teste isolado de contrato Avro e a evolução do subject real do CDC são verificações diferentes.
- A regra utiliza `$rowtime` do stream e não `created_at`.
- O PostgreSQL Sink é uma extensão downstream da arquitetura, não deve ser apresentado como requisito universal da demonstração.
- O projeto usa scripts por etapa para tornar explícitos os efeitos e as validações, em vez de esconder todo o provisionamento em um único comando.

## Critérios para declarar uma etapa concluída

1. Identificar o objeto/fluxo efetivamente testado.
2. Registrar o comando ou ação e o contexto não sensível.
3. Conferir o resultado observado, incluindo partição/offset quando aplicável.
4. Correlacionar entrada e saída para validações funcionais.
5. Registrar limitações e falhas sem converter estados intermediários em aprovação.

## Aprendizados consolidados

- Código versionado demonstra preparação, não execução.
- Um conector ou statement em RUNNING demonstra estado operacional, não correção funcional.
- Alertas de dados sintéticos não caracterizam fraude real.
- Uma consulta de saída sem correlação pode retornar alertas históricos ou de outros eventos.
- Uma janela temporal depende de event time e watermark, não apenas do tempo de espera no terminal.
- O Sink precisa de evidência de persistência correlacionada; UPSERT não é garantia exactly-once.
- Teardown deve ser validado por recurso e provedor, com distinção entre removido, preservado, falha e não verificado.

## Evidências principais

- Snapshot: `evidence/homologation/20261115T203833Z-34858-cdc-snapshot/summary.txt`
- CDC incremental: `evidence/homologation/20261115T210658Z-cdc-incremental.txt`
- Evolução Avro real: `evidence/homologation/20261115T210745Z-real-schema-evolution.txt`
- Validação de alertas: `evidence/homologation/20261115T212130Z-fraud-alerts-validation.txt`
- Leitura Kafka: `evidence/homologation/20261116T005130Z-fraud-alerts-consume.log`
- Sink: `evidence/cloud/20261116T005433Z-05-sink-connector.txt`
- Observabilidade: `evidence/observability/`
- Teardown: `evidence/teardown/20261116T022748Z-teardown-report.md`
