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
