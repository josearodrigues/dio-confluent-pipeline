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
| Tópico de alertas → consumidor/efeito | No projeto público não há consumidor downstream nem Sink no fluxo principal. No projeto privado há template PostgresSink, script e DDL da tabela destino. | No privado, UPSERT com chave `(card_id, first_tx_time)` está preparado. A configuração corresponde aos campos emitidos pelo SQL privado e à PK da tabela de destino. UPSERT pode tornar reentregas idempotentes para essa chave, desde que a chave seja estável e o efeito seja determinístico. | Não há evidência Git rastreável atual de criação/execução do Sink: `evidence/cloud/05-sink-connector.txt` retorna 404 no branch `main`. A própria configuração não prova consumo, persistência ou exactly-once. O script comenta entrega at-least-once; não transformar isso em garantia validada do conector/efeito. |
| Consumidor → efeito | O exemplo do desafio destaca que a idempotência do efeito é responsabilidade da aplicação. | Pode-se afirmar o princípio: se a entrega puder ser repetida, o consumidor/efeito precisa tolerar reprocessamento, por exemplo com chave idempotente e operação UPSERT. | Não há consumidor de aplicação implementado/homologado no projeto público. No privado, o Sink está preparado, mas sem prova de execução. Não há evidência de commit de offsets coordenado transacionalmente com o efeito. |

## Leitura comparativa dos projetos

### Projeto público — `josearodrigues/dio-confluent-pipeline`

- PostgreSQL → CDC → Kafka: snapshot, CDC incremental e evolução do subject real têm homologações documentadas.
- Flink: execução parcial observada; detecção do cenário positivo e publicação correlacionada no tópico não homologadas.
- Destino: tópico Kafka previsto; consumidor/efeito downstream não implementado no fluxo principal.
- Conclusão: não declarar pipeline end-to-end funcional/homologada enquanto a saída de alertas não for demonstrada e correlacionada.

### Projeto privado — `josearodrigues/dio-confluent-pipeline-pagamentos-estudos`

- Arquitetura, decisões, DDL, template e script preparam PostgreSQL Sink com UPSERT.
- O plano de homologação define a Etapa 6, mas não há registro de aprovação.
- O caminho exato `evidence/cloud/05-sink-connector.txt` não existe no branch `main` consultado (404).
- Conclusão: Sink planejado e preparado em arquivos; execução e persistência não comprovadas.

## Conclusão sobre o requisito de saída

O material do desafio exige conceitualmente entregar o resultado a um destino/consumidor; PostgreSQL é recomendado, mas não obrigatório. O mecanismo concreto não está especificado nos materiais disponíveis. Um tópico Kafka é um destino tecnicamente consumível, mas a documentação introdutória sugere uma etapa de entrega ao destino após o processamento. A matriz de confiabilidade também destaca o elo “consumer → efeito”.

Assim, a interpretação mais segura é: **publicar o alerta no tópico é necessário para disponibilizá-lo, mas não há base documental suficiente para afirmar que isso, isoladamente, satisfaz a entrega end-to-end**. É preciso demonstrar um consumidor/efeito ou justificar claramente o tópico como destino final consumido. Não se conclui que PostgreSQL Sink seja obrigatório.

## Próximo passo

Antes de escolher entre tópico como destino final, Sink ou consumidor próprio, fechar a análise do contrato de saída e dos critérios de aceite existentes; depois revisar estaticamente a implementação correspondente. Nenhum Sink, consumidor, SQL ou recurso Cloud/Neon deve ser executado nesta etapa.
