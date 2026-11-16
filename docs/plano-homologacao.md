# Homologação e aprendizados

## Objetivo

Este documento consolida os resultados verificáveis do laboratório e os limites da homologação. O roteiro de execução reproduzível permanece no [README](../README.md); os registros brutos permanecem em `evidence/`.

## Resultados comprovados

| Área | Resultado observado | Evidência |
|---|---|---|
| Banco de origem | Carga sintética: 200 customers, 240 accounts, 320 cards e 2.000 transactions. | `evidence/local/20261115T201040Z-01-database.log` |
| CDC snapshot | 2.760 eventos `op=r`, com as quatro contagens esperadas. | `evidence/homologation/20261115T203833Z-34858-cdc-snapshot/summary.txt` |
| CDC incremental | INSERT, UPDATE e DELETE correlacionados, `snapshot=false`, offsets 200–202. | `evidence/homologation/20261115T210658Z-cdc-incremental.txt` |
| Evolução Avro real | Subject `payments.transactions-value` evoluiu da versão 1 para 2, compatibilidade BACKWARD, evento observado no offset 2000. | `evidence/homologation/20261115T210745Z-real-schema-evolution.txt` |
| Flink | Statements de enrichment e detecção executados; posteriormente parados e confirmados como STOPPED. | `evidence/cloud/04-flink.txt`; `evidence/homologation/20261115T212311Z-flink-statements-stop.txt` |
| Tópico de alertas | Mensagem Avro lida no tópico `fraud_alerts`, partição 3, offset 0. | `evidence/homologation/20261116T005130Z-fraud-alerts-consume.log` |
| PostgreSQL Sink | Conector e task observados como RUNNING; consulta ao destino retornou registros. | `evidence/cloud/20261116T005433Z-05-sink-connector.txt`; `evidence/homologation/20261116T005824Z-alerts.log` |
| ACLs/RBAC | 17 ACLs esperadas encontradas, sem ausências/divergências ou extras; um vínculo MetricsViewer. | `evidence/observability/20261116T014218Z-acls.md` |
| Métricas | Consulta read-only retornou dois pontos de received_bytes; consumer group observado como STABLE. | `evidence/observability/20261116T014113Z-metrics.md` |
| Billing | Seis itens associados ao Environment; total retornado de USD 2,7315 (exibição arredondada: USD 2,73). | `evidence/observability/20261116T021948Z-billing.json`; `evidence/observability/20261116T021948Z-billing.md` |
| Teardown Confluent | Environment ausente após a operação; remoção de metrics-reader e vínculo MetricsViewer confirmadas. | `evidence/teardown/20261116T022748Z-teardown-report.md` |
| Teardown Neon | Limpeza de objetos do laboratório iniciada, mas `DROP ROLE cdc` falhou por falta de privilégio. A conclusão integral não foi validada. | `evidence/local/20261116T022745Z-postgres-cleanup.log`; relatório de teardown |

## Detecção de padrões e interpretação dos alertas

A regra Flink procura três ou mais eventos com `amount > 0` para o mesmo cartão em até 60 segundos de `$rowtime`. Ela é uma demonstração técnica, não um motor antifraude.

O cenário sintético inseriu três transações para o cartão 1 em aproximadamente 0,29 segundo e uma transação para o cartão 320. A consulta posterior encontrou vários alertas para diversos cartões. A validação do cartão 1 foi reprovada com uma janela mais estreita e aprovada após ampliar o intervalo consultado; os alertas aprovados não carregam os IDs das transações de origem e seus timestamps não reproduzem exatamente os três INSERTs registrados.

**Conclusão:** foi observada a geração de alertas e foi lida uma mensagem no tópico. A evidência disponível não permite atribuir inequivocamente todos os alertas ao cenário de teste nem afirmar fraude real. Os dados são sintéticos; não houve análise de transações reais. O cenário negativo também não foi validado: seu script falhou porque omitiu `created_at`, coluna NOT NULL.

## Aprendizados técnicos

- `$rowtime` representa o tempo de evento usado pelo Flink, não o campo de negócio `created_at`.
- A janela de `MATCH_RECOGNIZE` depende do tempo de evento e dos watermarks; aguardar um intervalo em relógio de parede não garante emissão imediata.
- `NOT ENFORCED` declara metadados de chave no Flink; não valida unicidade na origem.
- `changelog.mode=append` não equivale a filtrar somente INSERTs. Atualizações CDC podem ser representadas como novas linhas; exclusões não são propagadas como linhas append.
- Uma validação robusta deve correlacionar IDs/offsets dos eventos de entrada com o alerta produzido. Cartão, contagem e faixa temporal, isoladamente, podem aceitar alertas preexistentes.
- O Sink em estado RUNNING não comprova persistência. A consulta ao destino confirma que há registros, mas a evidência disponível não correlaciona a primeira mensagem lida do Kafka com uma linha específica no recorte consultado.
- UPSERT com chave estável pode tornar reentregas idempotentes no destino; não garante exactly-once ponta a ponta.
- Métricas agregadas e Billing têm escopo e atraso próprios. Ausência de pontos não significa tráfego zero; Costs API pode levar até 72 horas para refletir consumo recente.
- Teardown deve ser considerado concluído por componente somente após verificação posterior. Falha em remover uma role não deve ser ocultada pelo sucesso das demais etapas.

## Estado final e limitações

- **Confluent:** Environment ausente confirmado; statements Flink parados; identidades/vínculo registrados como removidos no relatório.
- **Neon/PostgreSQL:** schema `payments`, tabela `public.fraud_alerts`, publication e replication slot foram tratados pela rotina; a remoção da role `cdc` falhou. A verificação final do SQL não foi alcançada. Não declarar teardown integral concluído.
- **Fraude:** alertas sintéticos observados; correlação causal completa com os INSERTs de teste não comprovada.
- **Sink:** conector/task RUNNING e registros consultados no destino; não há evidência de teste controlado de reentrega com a mesma chave.

Os arquivos em `evidence/` são os registros primários. Este resumo não substitui os logs nem transforma observações parciais em homologação integral.
