# Matriz de garantias do pipeline

## Como interpretar

- **Preparado:** existe código ou configuração versionada.
- **Executado/observado:** há evidência de execução ou resultado.
- **Comprovado:** a evidência valida o comportamento específico descrito.
- Um estado `RUNNING` não comprova, isoladamente, entrega, persistência, ausência de duplicatas ou recuperação.

## Matriz por trecho

| Trecho | Resultado sustentado pelas evidências | Garantia que pode ser afirmada | Limite |
|---|---|---|---|
| PostgreSQL → CDC | Snapshot de 2.760 eventos e INSERT/UPDATE/DELETE incrementais observados. | Captura inicial e operações incrementais foram comprovadas nesta execução. | Não prova exactly-once, ausência de duplicatas após restart ou ordenação global. |
| CDC → Kafka | Eventos Debezium correlacionados com operações, `snapshot=false`, partição e offsets. | Publicação dos eventos de teste nos tópicos foi comprovada. | Ordem só pode ser considerada no escopo da partição; não afirmar ordem global. |
| Schema Registry | Evolução do subject real de transactions da versão 1 para 2, BACKWARD, evento no offset 2000. | Evolução compatível do subject real foi comprovada. | Não garante compatibilidade de qualquer mudança futura. |
| Kafka → Flink | Statements submetidos e observados; enrichment e detecção executados e depois parados. | Execução dos statements foi observada. | Estado RUNNING não prova resultado correto, recuperação ou ausência de perda/duplicação. |
| Enrichment / append | Temporal Join e alteração append configurados. | O fluxo foi executado conforme evidências disponíveis. | Append não filtra somente INSERTs; updates CDC podem contribuir como novas linhas. PK NOT ENFORCED não valida unicidade. |
| Detecção de padrões | Alertas foram gerados para vários cartões; validação de cartão 1 passou após ampliação da janela consultada. | A saída de alertas foi observada. | Não há correlação inequívoca entre cada alerta e os INSERTs do cenário. Não afirmar fraude real nem homologação funcional inequívoca. |
| Tópico `fraud_alerts` | Mensagem Avro lida na partição 3, offset 0. | Leitura e presença dos campos esperados foram comprovadas. | A mensagem lida não foi correlacionada ao cenário positivo específico. |
| PostgreSQL Sink | Connector e task observados RUNNING; consulta ao destino retornou registros. | O conector foi criado e o destino contém registros consultáveis. | A consulta limitada não correlaciona a primeira mensagem lida do Kafka com uma linha específica. Reentrega controlada não comprovada. |
| ACLs/RBAC | 17 ACLs esperadas encontradas; nenhuma ausente/divergente ou extra; MetricsViewer presente. | A matriz de permissões foi aprovada na coleta registrada. | Reflete o estado na data da coleta, não um estado futuro após teardown. |
| Métricas | Dois pontos de `received_bytes` retornados; consumer group `connect-lcc-nvppvxk` observado STABLE. | Coleta read-only e estado do grupo foram observados. | Dois pontos não comprovam cobertura contínua nem ausência de tráfego nos demais intervalos. |
| Billing | Seis itens do Environment; soma USD 2,7315 (aprox. USD 2,73). | Custos retornados para o Environment e período consultados. | Não é custo total da organização; Costs API pode atrasar até 72 horas. |
| Teardown Confluent | Environment ausente; metrics-reader e MetricsViewer com remoção confirmada. | Remoção dos alvos indicados no relatório foi confirmada. | Não implica remoção de recursos de outro provedor. |
| Teardown Neon | Schema/tabelas/publication/slot tratados; `DROP ROLE cdc` falhou. | Parte da limpeza foi executada. | Teardown Neon e teardown integral não podem ser declarados concluídos. |

## Conclusão

O projeto comprovou snapshot CDC, CDC incremental, evolução do subject real, auditoria de ACLs/RBAC, coleta de métricas e consulta de Billing. Também observou a geração/leitura de alertas e a execução do PostgreSQL Sink com registros no destino.

Não há evidência suficiente para afirmar fraude real, correlação causal inequívoca do cenário positivo, exactly-once ponta a ponta, ou teardown integral. A limpeza Confluent foi confirmada; a limpeza Neon terminou com falha ao remover a role `cdc`.

As evidências primárias estão em `evidence/`. Consulte [Homologação e aprendizados](plano-homologacao.md) para os detalhes e limitações.
