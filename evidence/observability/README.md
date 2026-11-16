# Evidências de observabilidade e confiabilidade

Este diretório reúne resultados reais de consultas read-only ao ambiente Confluent e análises técnicas baseadas nas evidências versionadas.

## Arquivos

- `*-metrics.json/md`: resposta e resumo da Metrics API.
- `*-acls.md`: auditoria de ACLs e vínculo RBAC MetricsViewer.
- `*-billing.json/md`: custos retornados pela Costs API, filtrados pelo Environment do laboratório.
- [`20261115-reliability-analysis.md`](20261115-reliability-analysis.md): análise consolidada de confiabilidade, resultados, limites e aprendizados do laboratório.

## Coletas registradas

- Métricas: dois pontos de `io.confluent.kafka.server/received_bytes` e consumer group observado como STABLE.
- ACLs/RBAC: 17 permissões esperadas encontradas, sem ausências/divergências ou extras; um vínculo MetricsViewer.
- Billing: seis itens associados ao Environment, total USD 2,7315 (aprox. USD 2,73) para o período diário retornado.

A análise de confiabilidade é um documento interpretativo, não uma saída bruta de execução. Ela distingue resultados observados, comportamentos comprovados e limites que permanecem sem validação.

Consulte também [Custos, métricas e segurança operacional](../../operations/cost-and-metrics.md).

## Limites

A métrica representa bytes recebidos no cluster, não taxa em bytes/s nem origem individual do tráfego. Dois pontos não demonstram cobertura contínua. Billing representa somente o Environment filtrado e pode atrasar até 72 horas. A coleta não comprova confiabilidade ponta a ponta.

Não incluir API keys, secrets, connection strings ou outros dados sensíveis. Não interpretar ausência de pontos como tráfego zero.
