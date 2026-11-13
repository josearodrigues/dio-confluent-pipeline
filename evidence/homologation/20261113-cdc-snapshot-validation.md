# Homologação do snapshot CDC — 13/11/2026

**Etapa:** validação do snapshot inicial nos quatro tópicos Kafka  
**Resultado:** aprovada — 2.760 de 2.760 eventos `op=r` observados.

## Resultado da execução aprovada

- Início registrado pelo verificador: `2026-11-13T18:51:52Z`.
- Consumer group: `snapshot-check-20261113T185152Z-24189`.
- Timeout individual configurado: `2.400 segundos (40 minutos)`.
- Critério: atingir a quantidade esperada de eventos Debezium `op=r` em cada tópico.
- Os marcadores Debezium de snapshot foram registrados para diagnóstico, sem condicionar a conclusão.

| Tópico | Esperado | `op=r` | Marcadores | Registros | Resultado |
|---|---:|---:|---:|---:|---|
| `payments.customers` | 200 | 200 | 200 | 200 | OK |
| `payments.accounts` | 240 | 240 | 240 | 240 | OK |
| `payments.cards` | 320 | 320 | 320 | 320 | OK |
| `payments.transactions` | 2.000 | 2.000 | 2.000 | 2.000 | OK |
| **Total** | **2.760** | **2.760** | **2.760** | **2.760** | **OK** |

O verificador encerrou cada consumidor ao atingir sua meta (`target-reached`). Resultado final informado: `snapshot validado nos quatro tópicos (2.760 eventos op=r)`.

Resumo e logs locais produzidos pelo verificador:
`evidence/homologation/20261113T185152Z-24189-cdc-snapshot/summary.txt`  
`evidence/homologation/20261113T185152Z-24189-cdc-snapshot/payments-customers.log`  
`evidence/homologation/20261113T185152Z-24189-cdc-snapshot/payments-accounts.log`  
`evidence/homologation/20261113T185152Z-24189-cdc-snapshot/payments-cards.log`  
`evidence/homologation/20261113T185152Z-24189-cdc-snapshot/payments-transactions.log`

Este documento registra o resumo fornecido da execução. Os logs individuais permanecem no ambiente local do usuário; não são reproduzidos nem declarados versionados por este registro.

## Tentativas anteriores e comportamento observado

1. Execução com timeout de 1.800 s: customers 200/200, accounts 240/240, cards 320/320; transactions 1.910/2.000 e timeout. Resultado parcial: 2.670/2.760.
2. Nova execução com timeout de 2.400 s: os quatro tópicos atingiram exatamente as quantidades esperadas, incluindo transactions 2.000/2.000.
3. O usuário observou aproximadamente 34 minutos entre a criação e a última alteração do arquivo de log de transactions na execução aprovada. Esse intervalo é uma observação operacional aproximada, não uma medição formal de throughput nem uma garantia para execuções futuras.
4. No projeto privado de estudos, uma execução histórica recebeu 2.000 eventos `op=r` de transactions em menos de 30 minutos. O verificador daquela versão também exigia 2.000 marcadores de snapshot; foram observados 1.998, razão pela qual aquela versão podia classificar a execução como incompleta apesar de `op=r=2.000`. Portanto, a comparação de duração entre os projetos deve considerar diferenças de ambiente e de critério/implementação do verificador.

## Observações operacionais

- Na primeira tentativa do projeto atual, a autenticação da Confluent CLI ainda dependia da conclusão da interação no navegador; dois consumidores encerraram com 0 eventos. Depois da conclusão do login, uma nova execução avançou normalmente. A sequência foi observada, mas não comprova causalidade técnica. Não foi identificada evidência suficiente para atribuir aquela falha exclusivamente à sessão da CLI.
- Neon e Confluent estavam configurados na mesma região informada pelo usuário (`us-east-2`). Ainda assim, transactions exigiu mais tempo que os demais tópicos. Proximidade regional, isoladamente, não explica o desempenho.
- A execução aprovada demonstra que o consumidor conseguiu ler os 2.000 eventos dentro da janela de 2.400 s. Não identifica, por si só, o gargalo responsável pelo tempo de consumo.

## Conclusão e estado da etapa

**Snapshot inicial homologado para os quatro tópicos:** 200 customers, 240 accounts, 320 cards e 2.000 transactions, totalizando 2.760 eventos `op=r`.

O timeout de 1.800 s foi insuficiente para concluir transactions naquela tentativa específica; o timeout de 2.400 s permitiu concluir a validação. Mantém-se 2.400 s como referência operacional para esta configuração, sem afirmar que seja um limite ótimo ou universal.

A etapa de snapshot está concluída. Conforme a sequência do projeto, a próxima etapa é revisar/executar a preparação Flink, respeitando as verificações e autorizações previstas no plano de homologação. Esta evidência não declara Temporal Join, processamento de fraude ou pipeline ponta a ponta homologados.
