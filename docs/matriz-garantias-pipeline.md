# Matriz própria de garantias do pipeline

**Data da análise:** 14/11/2026  
**Natureza:** análise técnica própria baseada nos materiais 4.0 disponíveis e nos arquivos versionados dos projetos. **Não é reprodução da tabela original** mencionada no PDF, pois o README que a conteria não está no acervo Dropbox.

## Como interpretar

- **Configurado/preparado:** existe configuração ou código versionado.
- **Executado/observado:** há registro de execução ou resultado observado.
- **Homologado:** existe evidência rastreável que valida o comportamento específico.
- A existência de um conector em estado `RUNNING`, de SQL ou de script não comprova, isoladamente, entrega correta, ausência de perda/duplicação ou semântica exactly-once.
- “At-least-once” e “exactly-once” só são atribuídos quando há evidência/documentação suficiente para o trecho e para o efeito considerado.

## Matriz por trecho

| Trecho | O que está configurado ou documentado | Garantia que pode ser afirmada | Lacuna/risco e evidência |
|---|---|---|---|
| PostgreSQL → CDC | PostgreSQL usa logical replication, publication, replication slot e plugin `pgoutput`; CDC Source V2 está configurado com `snapshot.mode=initial`, `tasks.max=1` e `REPLICA IDENTITY FULL` no DDL. | Snapshot inicial e captura de INSERT/UPDATE/DELETE foram observados e homologados no ambiente registrado. A captura incremental observada preservou `op=c/u/d`, `before/after` e offsets. | A configuração não prova, sozinha, semântica exactly-once nem ausência de duplicatas após falha/restart. Não há garantia geral de ordenação entre tabelas. Evidências públicas registradas no plano: snapshot 2.760/2.760 e incremental offsets 231–233 (14/11). |
| CDC → Kafka | Conector publica em tópicos `payments.*`, com Avro para chave e valor e Schema Registry; a publicação inclui quatro tabelas. | Foi homologada a presença dos eventos de snapshot e CDC incremental nos tópicos; a evolução do subject real foi observada com evento Avro no tópico. Kafka preserva ordem dentro de uma partition, não entre partitions. | O JSON não define explicitamente uma chave de negócio nem prova que eventos correlatos permaneçam sempre na mesma partition. Não afirmar ordenação global, entrega exactly-once ou ausência de duplicatas. Evidências de snapshot, offsets 231–233 e evolução do subject/offset 2000. |
| Kafka → Flink | SQL usa Temporal Join de transactions com accounts em `$rowtime`; declarações de PK são metadados `NOT ENFORCED`; há transformação append e `MATCH_RECOGNIZE`. | Foram observados estados de statements (PK/append COMPLETED; enrichment/fraude RUNNING) e consulta de linhas no catálogo. O Temporal Join usa o tempo do registro no stream, não necessariamente `created_at`. | Estado RUNNING não prova correção funcional, checkpoint/restart, recuperação de estado ou ausência de perda/duplicação. A PK `NOT ENFORCED` não valida unicidade na origem. A execução Flink registrada é parcial. |
| Flink: enrichment/append/detecção | O SQL público converte `enriched_payments` para changelog append e detecta três ou mais eventos positivos por cartão em 60 s de `$rowtime`. | A submissão/execução dos statements foi observada parcialmente. A regra e o relógio usados estão explícitos no SQL. | Append não equivale a filtrar somente INSERT: updates podem contribuir como novas linhas e deletes não são propagados como novas linhas, conforme a documentação do próprio projeto. Não há homologação funcional do cenário positivo; os alertas consultados não foram correlacionados às transações de teste. |
| Flink → tópico de alertas | A tabela Flink `fraud_alerts` representa a saída prevista; o README público declara o tópico Kafka como destino do fluxo principal. | A saída está planejada e documentada. | Não foi localizada evidência que comprove a mensagem de alerta do cenário positivo no tópico, com chave, partition, offset e correlação com as transações. Portanto, a entrega de alertas não está homologada. |
| Tópico de alertas → PostgreSQL Sink | O projeto público agora contém configuração `PostgresSink` em Avro, UPSERT, tabela pré-criada `public.fraud_alerts`, role `alerts_sink`, script protegido de criação e consulta read-only de privilégios/persistência. A chave composta é `(card_id, first_txn)`, alinhada ao SQL Flink público. | A documentação Confluent declara entrega at-least-once para o PostgreSQL Sink; UPSERT pode tornar reentregas com a mesma chave idempotentes no destino, se schema e chave forem compatíveis. Referência: https://docs.confluent.io/cloud/current/connectors/cc-postgresql-sink.html. Isso é garantia documentada do conector, não homologação deste fluxo. | Não foi executado nem homologado. O schema Avro efetivo do tópico `fraud_alerts`, a suficiência dos privilégios e a persistência real ainda precisam ser verificados. `RUNNING` não comprova escrita; não afirmar exactly-once. |
| Sink → efeito PostgreSQL | O PostgreSQL Sink grava em `public.fraud_alerts` por UPSERT com chave `(card_id, first_txn)`. | A documentação Confluent declara entrega at-least-once; UPSERT pode tornar reentregas com a mesma chave idempotentes no destino. | O fluxo ainda não foi executado/homologado. Não há evidência de escrita, reentrega controlada ou coordenação transacional entre offsets e efeito. Não afirmar exactly-once ponta a ponta. |

## Leitura comparativa dos projetos

### Projeto público — `josearodrigues/dio-confluent-pipeline`

- PostgreSQL → CDC → Kafka: snapshot, CDC incremental e evolução do subject real têm homologações documentadas.
- Flink: execução parcial observada; detecção do cenário positivo e publicação correlacionada no tópico não homologadas.
- Destino: tópico Kafka `fraud_alerts` e PostgreSQL Sink preparados no código; a execução do Sink e a persistência ainda não foram homologadas.
- Conclusão: não declarar pipeline end-to-end funcional/homologada enquanto a saída de alertas não for demonstrada e correlacionada.

### Projeto privado — `josearodrigues/dio-confluent-pipeline-pagamentos-estudos`

- Os sete commits adicionais do projeto de estudos foram analisados; as proteções do script, a auditoria SQL e os critérios de homologação foram adaptados ao contrato e aos caminhos do projeto público.
- O projeto público contém DDL, role, configuração, script de criação, verificação read-only, proteção de teardown e documentação do PostgreSQL Sink.
- Conclusão: Sink preparado no projeto público; execução e persistência não comprovadas. O repositório privado permanece como referência e não foi alterado nesta transferência.

## Conclusão sobre o requisito de saída

O material do desafio não determina PostgreSQL como único destino possível. Neste repositório, a decisão de arquitetura foi acrescentar PostgreSQL Sink como etapa downstream: Flink publica no tópico `fraud_alerts`, e o Sink está preparado para persistir os registros em `public.fraud_alerts` por UPSERT. Essa decisão amplia o fluxo do projeto, mas não significa que o Sink seja requisito universal do desafio.

A implementação está preparada no código, não executada. A entrega end-to-end só será considerada homologada após verificar o schema Avro real, os privilégios efetivos, a mensagem Kafka correlacionada e a linha persistida no PostgreSQL. A documentação do conector declara at-least-once; UPSERT pode tornar a gravação idempotente para uma chave estável, mas não comprova exactly-once ponta a ponta.

## Próximo passo

Próxima etapa: obter o schema Avro real do tópico `fraud_alerts`, auditar os privilégios efetivos de `alerts_sink` e confirmar compatibilidade antes de qualquer criação do conector. Nenhuma operação Cloud/Neon foi executada durante a transferência.
