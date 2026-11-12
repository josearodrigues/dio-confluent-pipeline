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
