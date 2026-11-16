# Análise de confiabilidade e aprendizados

- Data de referência: 15/11/2026 (horário de Brasília).
- Natureza: análise retrospectiva das evidências de execução e da arquitetura versionada.
- Escopo: CDC, Kafka, Schema Registry, Flink, detecção sintética, PostgreSQL Sink, observabilidade, custos e teardown.
- Limite: esta análise consolida os resultados registrados; não representa uma nova execução nem uma garantia formal de confiabilidade.

## Resumo executivo

O laboratório comprovou partes importantes do fluxo: snapshot CDC, operações incrementais, evolução compatível do subject Avro real, execução dos statements Flink, leitura de mensagem no tópico de alertas e existência de registros na tabela PostgreSQL de destino. Também foram coletadas evidências de ACLs/RBAC, métricas e Billing.

A detecção produziu alertas, mas apresentou comportamento anômalo: muitos cartões tiveram alertas concentrados em poucos milissegundos, e os registros não carregam identificadores das transações de origem que permitam atribuir cada alerta ao cenário sintético. A validação do cartão 1 falhou com uma janela de consulta mais estreita e passou após a ampliação do intervalo. Isso comprova a presença de alertas compatíveis com os critérios consultados, mas não a correlação causal inequívoca com os três INSERTs de teste.

O PostgreSQL Sink e sua task foram observados em RUNNING, e a consulta ao destino retornou registros. Ainda assim, não foi correlacionada uma mensagem Kafka específica com a linha correspondente no PostgreSQL, nem foi testada reentrega controlada.

O teardown Confluent foi confirmado por listagem. A limpeza Neon removeu objetos do laboratório, mas falhou ao remover a role `cdc` por falta de privilégio. Portanto, o encerramento integral não foi concluído.

## Resultados observados e limites

| Trecho / área | Resultado observado | O que pode ser afirmado | Limite remanescente |
|---|---|---|---|
| PostgreSQL → CDC | Snapshot com 2.760 eventos `op=r`: customers 200, accounts 240, cards 320 e transactions 2.000. | O snapshot inicial foi validado nos quatro tópicos. | Não comprova recuperação após falha, ausência de duplicatas ou exactly-once. |
| CDC incremental | INSERT, UPDATE e DELETE observados no tópico customers, com `snapshot=false`, partição 0 e offsets 200–202. | As três operações incrementais foram correlacionadas com o registro de teste. | O teste não cobre todos os cenários de concorrência, restart ou recuperação do conector. |
| Schema Registry | Subject `payments.transactions-value` evoluiu da versão 1 para 2; BACKWARD aceito; evento com a nova coluna observado no offset 2000. | A evolução específica do contrato real foi comprovada. | Não garante compatibilidade de alterações futuras diferentes. |
| Flink | Statements de enrichment e detecção observados em execução e depois confirmados como STOPPED. | A submissão, execução observada e parada dos statements foram registradas. | RUNNING não prova correção funcional; a regra não foi homologada de forma causal ponta a ponta. |
| Detecção sintética | Alertas para diversos cartões, muitos concentrados por volta de 21:10:40 UTC; validação do cartão 1 reprovada na janela inicial e aprovada após ampliação. | A saída de alertas foi produzida e consultada. | A ampliação temporal também incluiu alertas anteriores ao cenário. Não há IDs de origem que comprovem quais transações geraram cada alerta. |
| Tópico `fraud_alerts` | Primeira mensagem Avro lida na partição 3, offset 0, com `card_id=290` e os quatro campos esperados. | A leitura de uma mensagem e a presença do contrato esperado foram comprovadas. | Essa mensagem não foi correlacionada ao cenário positivo do cartão 1. |
| PostgreSQL Sink | Connector e task 0 observados em RUNNING; configuração Avro/UPSERT; consulta ao destino retornou registros. | O conector foi criado/executado e havia registros consultáveis em `public.fraud_alerts`. | Não foi correlacionada uma mensagem Kafka específica à linha persistida correspondente; não houve teste controlado de reentrega. |
| ACLs/RBAC | 17 ACLs esperadas encontradas, sem ausências/divergências ou extras; vínculo MetricsViewer presente. | A auditoria registrada foi aprovada para o estado consultado. | É uma fotografia da coleta, não uma garantia de estado futuro. |
| Métricas | Dois pontos de `received_bytes` retornados; consumer group `connect-lcc-nvppvxk` observado STABLE. | Houve coleta read-only e observação do estado do grupo. | Dois pontos horários não demonstram cobertura contínua nem tráfego zero nos intervalos sem dados. |
| Billing | Seis itens do Environment consultado, total de USD 2,7315 (aprox. USD 2,73), para 15/11/2026. | Esse foi o custo retornado para o Environment e período consultados. | Não representa o custo total da organização; a Costs API pode atrasar até 72 horas. |
| Teardown Confluent | Environment ausente após a operação; remoção de `metrics-reader` e vínculo MetricsViewer verificada. | A remoção do Environment e dos alvos indicados foi confirmada por verificação posterior. | Não implica remoção de recursos Neon. |
| Teardown Neon | Publication, replication slot, schema `payments` e tabela `public.fraud_alerts` tratados; `DROP ROLE cdc` falhou por falta de CREATEROLE e ADMIN sobre a role. | A limpeza seletiva foi iniciada e parte dos objetos foi removida. | A rotina terminou com erro; não declarar teardown Neon nem teardown integral concluídos. |

## Anomalia da detecção e interpretação dos alertas

O cenário positivo inseriu três transações positivas para o cartão 1 em aproximadamente 0,29 segundo. A consulta de alertas retornou muitos cartões com três transações em timestamps concentrados em poucos milissegundos, além de registros do cartão 1.

A validação do cartão 1 foi reprovada quando a janela consultada começava às 21:14 UTC: os quatro alertas retornados não estavam dentro do intervalo. Após ampliar o início da janela para 21:10 UTC, os quatro registros passaram pelo critério de cartão, quantidade e intervalo temporal. Essa aprovação é limitada ao filtro consultado; não demonstra que os quatro alertas foram causados pelos três INSERTs do cenário.

A primeira mensagem lida diretamente do tópico `fraud_alerts` foi de `card_id=290`, não do cartão 1. A consulta ao PostgreSQL também retornou alertas de vários cartões. Como a saída não preserva os IDs das transações de origem, não há rastreabilidade suficiente para ligar cada alerta às entradas que o produziram.

O padrão observado deve ser classificado como comportamento anômalo / possível falso positivo ou entrada não correlacionada. As evidências não identificam a causa. Não houve análise de transações financeiras reais e, portanto, não se pode afirmar que fraude real foi detectada.

O cenário negativo não foi homologado: o script `slow-pattern.sql` falhou ao inserir porque não preencheu `created_at`, coluna NOT NULL.

## Aprendizados técnicos

### CDC e semântica de eventos

- Snapshot, CDC incremental e evolução do schema são validações distintas; o sucesso de uma não substitui as demais.
- Os offsets observados identificam posições dentro de partições. Não se deve inferir ordenação global entre partições.
- A leitura de eventos CDC deve considerar `op`, `snapshot`, partição e offset, e não apenas o conteúdo de `after`.

### Flink e tempo

- `$rowtime` é o tempo de evento associado ao stream Kafka, não o campo de negócio `created_at` do PostgreSQL.
- A janela de 60 segundos depende do tempo de evento e dos watermarks; aguardar 60 ou 90 segundos no relógio de parede não garante emissão imediata.
- `changelog.mode=append` não filtra somente INSERTs: updates CDC podem contribuir como novas linhas; deletes não são representados como novas linhas append.
- Chaves `NOT ENFORCED` declaram metadados para o Flink, mas não validam unicidade na origem.

### Alertas e Sink

- A condição `amount > 0` e a contagem de três eventos são uma regra didática, não um motor antifraude.
- Cartão, quantidade e intervalo temporal não bastam para provar causalidade quando há alertas preexistentes ou não correlacionados.
- Para uma homologação mais forte, a saída precisa preservar identificadores de origem e permitir correlação com eventos Kafka por partição/offset e com o registro persistido.
- O estado RUNNING do Sink não comprova persistência. Uma consulta com linhas comprova que há dados no destino, mas não a correspondência de uma mensagem específica.
- UPSERT com chave estável pode reduzir duplicações lógicas em reentregas com a mesma chave. Isso não equivale a exactly-once ponta a ponta; a semântica do conector permanece at-least-once.

### Observabilidade, custos e encerramento

- Métricas agregadas devem ser interpretadas dentro do intervalo e granularidade solicitados. Ausência de pontos não significa tráfego zero.
- Billing filtrado por Environment não representa o custo total da organização; dados recentes podem sofrer atraso.
- O encerramento de recursos Confluent e a limpeza do banco Neon são operações independentes.
- Teardown deve ser declarado por componente somente após verificação posterior. Falhas de permissão não devem ser contornadas com remoções amplas como `DROP OWNED CASCADE`.

## Estado final do laboratório

- **CDC:** snapshot, operações incrementais e evolução do subject Avro real comprovados nas execuções registradas.
- **Flink:** statements executados/observados e posteriormente parados.
- **Detecção:** alertas sintéticos observados; correlação causal completa não comprovada; cenário negativo não homologado.
- **Sink:** conector/task RUNNING e registros consultados no destino; rastreabilidade mensagem-a-linha e reentrega controlada não comprovadas.
- **Segurança e observabilidade:** auditoria ACL/RBAC aprovada e coleta limitada de métricas realizada.
- **Custos:** USD 2,7315 retornados para o Environment no dia consultado, sujeitos ao atraso da API.
- **Confluent:** Environment ausente confirmado.
- **Neon:** limpeza parcial; falha ao remover a role `cdc`. O teardown integral permanece não concluído.

## Evidências primárias consultadas

- `evidence/homologation/20261115T203833Z-34858-cdc-snapshot/summary.txt`
- `evidence/homologation/20261115T210658Z-cdc-incremental.txt`
- `evidence/homologation/20261115T210745Z-real-schema-evolution.txt`
- `evidence/homologation/20261115T212052Z-fraud-alerts-validation.txt`
- `evidence/homologation/20261115T212130Z-fraud-alerts-validation.txt`
- `evidence/homologation/20261116T005130Z-fraud-alerts-consume.log`
- `evidence/homologation/20261116T005824Z-alerts.log`
- `evidence/homologation/20261115T212311Z-flink-statements-stop.txt`
- `evidence/observability/20261116T014113Z-metrics.md`
- `evidence/observability/20261116T014218Z-acls.md`
- `evidence/observability/20261116T021948Z-billing.md`
- `evidence/teardown/20261116T022748Z-teardown-report.md`
- `evidence/local/20261116T022745Z-postgres-cleanup.log`
- `docs/arquitetura.md`
- `docs/decisoes-tecnicas.md`
- `docs/matriz-garantias-pipeline.md`
