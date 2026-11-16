# Custos, métricas e segurança operacional

## Consultas

### Métricas

```bash
./operations/verify-metrics.sh
```

Padrões: janela `PT6H/now` e granularidade `PT1H`. Variáveis opcionais: `METRICS_INTERVAL` e `METRICS_GRANULARITY`.

A consulta utiliza `io.confluent.kafka.server/received_bytes`, filtrada pelo cluster configurado. A métrica é um contador em bytes, agregado na granularidade solicitada. Ela mede bytes recebidos pelo cluster; não identifica isoladamente o produtor. Ausência de pontos não significa tráfego zero.

### ACLs e RBAC

```bash
./operations/verify-acls.sh
```

A verificação é read-only e compara as permissões efetivas com a matriz esperada. Não aplicar alterações de ACL/RBAC como consequência automática de uma divergência; revisar primeiro o principal, o recurso e o escopo.

### Billing

```bash
./operations/verify-billing.sh
./operations/verify-billing.sh 2026-11-01 2026-11-11
```

Sem argumentos, consulta o mês UTC corrente até amanhã (fim exclusivo). Com argumentos, usa início inclusivo e fim exclusivo, em `YYYY-MM-DD`. A consulta filtra os itens para o `ENV_ID` do laboratório e não deve ser interpretada como custo total da organização.

A Costs API agrega itens diariamente e pode levar até 72 horas para disponibilizar dados recentes. Uma consulta sem itens não comprova custo zero fora do período ou da janela de disponibilidade.

## Resultados registrados

| Verificação | Resultado |
|---|---|
| Métricas | `received_bytes`: 512.436 bytes às 20:00 UTC e 117.194 bytes às 21:00 UTC; dois pontos retornados. |
| Consumer group | `connect-lcc-nvppvxk` observado como `STABLE`. |
| ACLs/RBAC | 17 ACLs esperadas; 17 encontradas; nenhuma ausente/divergente ou extra; um vínculo MetricsViewer. |
| Billing | Seis itens do Environment, total de USD 2,7315 (exibição arredondada: USD 2,73), no período diário retornado para 15/11/2026. |

Evidências:
- Métricas: `evidence/observability/20261116T014113Z-metrics.md` e JSON correspondente.
- ACLs/RBAC: `evidence/observability/20261116T014218Z-acls.md`.
- Billing: `evidence/observability/20261116T021948Z-billing.json` e resumo Markdown correspondente.

## Aprendizados

- Métrica de bytes agregada por hora não é taxa em bytes/s.
- Uma amostra parcial não permite inferir tráfego zero nos intervalos ausentes.
- ACLs e RBAC devem ser auditados por principal, operação e recurso; a quantidade total, sozinha, não demonstra menor privilégio.
- Billing deve ser filtrado pelo Environment do projeto. Não publicar resposta organizacional integral como evidência.
- Recursos provisionados podem gerar custos mesmo sem tráfego.
- Conferir Billing após o teardown e considerar o atraso de até 72 horas antes de interpretar o custo final.
- Evidências devem ser datadas, não conter credenciais e distinguir resultado observado de conclusão inferida.

## Segurança e encerramento

As consultas deste documento são somente leitura. Os scripts de criação, alteração e teardown têm efeitos próprios e exigem revisão de alvo e confirmação. O resultado do teardown está registrado em `evidence/teardown/20261116T022748Z-teardown-report.md`: a remoção do Environment Confluent foi confirmada, mas a limpeza Neon falhou ao remover a role `cdc`. Não declarar a limpeza integral concluída.
