# Custos e métricas

## Antes da execução
- Conferir região, tipo do cluster Kafka e limite de CFUs do compute pool.
- Confirmar créditos e saldo disponíveis no console da Confluent.
- Evitar criar recursos duplicados e cargas sem objetivo de teste.

## Durante
Acompanhar estado do cluster, conector CDC, compute pool, statements Flink, throughput, atraso e erros. Registrar somente valores observados.

## Após
- Coletar evidências.
- Parar statements que não precisem continuar.
- Remover compute pool e conectores somente após conferir dependências.
- Consultar Billing para confirmar consumo final.

Não estimar custo sem configuração e tabela de preços vigentes. Recursos provisionados podem gerar consumo mesmo sem mensagens.

## Observabilidade — primeira consulta à Metrics API (14/11/2026)

- Consulta read-only autenticada ao endpoint `/v2/metrics/cloud/query`, métrica `io.confluent.kafka.server/received_bytes`, filtrada pelo Kafka cluster configurado e com janela solicitada de seis horas (`PT6H/now`), granularidade `PT1H`.
- Resposta JSON observada pelo usuário:

| Timestamp UTC | Valor retornado |
|---|---:|
| 2026-11-14T05:00:00Z | 133168.0 |
| 2026-11-14T06:00:00Z | 1250.0 |

- A resposta contém dois pontos de dados e nenhum campo de erro foi apresentado. Isso confirma que a consulta retornou séries para o filtro/intervalo utilizado.
- **Interpretação pendente:** não registrar ainda os valores como bytes por hora, taxa ou volume acumulado. Confirmar a unidade e a semântica de agregação da métrica antes de calcular ou comparar throughput.
- Esta coleta é evidência inicial de observabilidade; não comprova cobertura completa das seis horas, ausência de tráfego nos demais intervalos nem estado atual do cluster.
- Nenhum recurso Cloud foi criado, alterado ou removido nesta consulta.
- Próxima ação: confirmar na documentação/descriptors oficiais a unidade e a semântica de `received_bytes`; depois registrar a interpretação validada. Em seguida, avançar para a verificação read-only das ACLs, sem alterar permissões nesta etapa.

### Interpretação confirmada da métrica (14/11/2026)

A documentação oficial da Metrics API classifica `io.confluent.kafka.server/received_bytes` como contador (`COUNTER_INT64`), unidade `By` (bytes), cuja amostra representa o delta de bytes recebidos da rede desde a amostra anterior, coletada a cada 60 segundos. Contadores são somados quando agregados ao longo do tempo. Com granularidade `PT1H`, os pontos observados correspondem a bytes agregados nos intervalos horários, não a bytes/s.

Conversão binária apenas para leitura:
- 05:00 UTC: 133.168 bytes ≈ 130,05 KiB.
- 06:00 UTC: 1.250 bytes ≈ 1,22 KiB.

A métrica representa tráfego recebido pelo cluster Kafka; não isola o tráfego do conector CDC nem identifica, por si só, qual produtor originou os bytes. A consulta retornou somente dois pontos; não inferir que os demais intervalos da janela tiveram tráfego zero.

Fonte oficial: https://api.telemetry.confluent.cloud/docs (descritor `io.confluent.kafka.server/received_bytes`, tipo, unidade, frequência e agregação).

## Coleta automatizada de métricas

O verificador `operations/verify-metrics.sh` automatiza a consulta read-only à Confluent Metrics API para a métrica `io.confluent.kafka.server/received_bytes`, filtrada pelo `CLUSTER_ID` local. Reutiliza `METRICS_API_KEY` e `METRICS_API_SECRET` já armazenadas no ambiente local; não cria API keys nem altera recursos Cloud.

Execução:

```bash
./operations/verify-metrics.sh
```

Padrões: janela `PT6H/now` e granularidade `PT1H`. Podem ser ajustados por `METRICS_INTERVAL` (formato `PT<n>H/now`) e `METRICS_GRANULARITY` (formato `PT<n>M` ou `PT<n>H`). O script grava a resposta JSON original e um resumo Markdown datados em `evidence/observability/`, com permissão local restrita. As evidências não incluem API key ou secret.

O resumo apresenta os valores em bytes e conversão para KiB. A métrica representa bytes recebidos pelo cluster na granularidade consultada; não identifica o produtor e ausência de pontos não significa tráfego zero. A coleta manual realizada em 14/11/2026 permanece como evidência histórica independente. A coleta automatizada foi executada com sucesso pelo usuário em 14/11/2026 e suas evidências foram versionadas; sua validação está registrada abaixo.

O futuro `operations/verify-observability.sh` será o orquestrador dos verificadores de métricas, ACLs, custos e confiabilidade. Ele só deve ser criado quando esses verificadores individuais estiverem implementados, para não declarar etapas inexistentes como concluídas.

### Execução do verificador automatizado (14/11/2026)

O usuário sincronizou `main`, validou a sintaxe de `operations/verify-metrics.sh` com `bash -n` e executou o script com sucesso. A execução gerou e versionou:

- `evidence/observability/20261114T100354Z-metrics.json` — resposta original da API.
- `evidence/observability/20261114T100354Z-metrics.md` — resumo com valores e conversões.

Foram retornados dois pontos: 133.168 bytes às 05:00 UTC (≈130,05 KiB) e 1.250 bytes às 06:00 UTC (≈1,22 KiB), para o cluster `lkc-rrwvjjk`, com janela solicitada `PT6H/now` e granularidade `PT1H`. A consulta confirma a coleta de dados para os pontos retornados, mas não cobertura contínua das seis horas. A coleta automatizada está validada nesta execução; a observabilidade geral permanece parcial até a verificação read-only de ACLs, Billing e confiabilidade.
