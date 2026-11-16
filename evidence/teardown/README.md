# Evidências de teardown

Esta pasta reúne os relatórios gerados pelas rotinas de encerramento.

## Execução registrada

O relatório `20261116T022748Z-teardown-report.md` registra:

- Environment Confluent ausente após a operação.
- Service Account `metrics-reader` e vínculo MetricsViewer com remoção verificada.
- Limpeza seletiva no Neon iniciada, com remoção de objetos do laboratório.
- Falha ao remover a role PostgreSQL `cdc`, por falta de privilégio CREATEROLE/ADMIN sobre a role.

**Conclusão:** teardown Confluent confirmado; limpeza Neon parcial. Não declarar teardown integral concluído. O projeto, branch, compute, endpoint e banco Neon foram preservados conforme o escopo do script.

## Como interpretar

A presença de um relatório comprova apenas que a rotina registrou aquela execução. Diferencie solicitação, execução, verificação posterior, bloqueios e falhas. Uma etapa com falha não deve ser considerada concluída.

Os relatórios não substituem a inspeção do estado atual dos recursos. Não inclua credenciais, tokens, senhas ou valores de variáveis secretas. Preserve os arquivos originais e seus timestamps.
