# Requisitos e critérios de aceite

Este documento define o escopo funcional e técnico do laboratório e os critérios para avaliar sua execução. Requisitos descrevem o que deve ser demonstrado; **não são evidência de implementação executada ou homologada**.

## Requisitos funcionais

| ID | Requisito | Artefatos relacionados |
|---|---|---|
| RF-01 | Capturar eventos CDC das tabelas customers, accounts, cards e transactions. | `database/init/01-database.sql`, `infrastructure/connectors/cdc/connector.json` |
| RF-02 | Publicar os eventos CDC nos tópicos Kafka correspondentes. | `infrastructure/connectors/cdc/connector.json` |
| RF-03 | Declarar no Flink as chaves necessárias ao processamento. | `flink/sql/00-pk-accounts.sql` |
| RF-04 | Enriquecer transações com a dimensão accounts por Temporal Join. | `flink/sql/01-enrichment.sql` |
| RF-05 | Preparar a entrada append-only usada pelo reconhecimento de padrão. | `flink/sql/02-append-only.sql` |
| RF-06 | Identificar três ou mais transações positivas do mesmo cartão em até 60 segundos de `$rowtime` Kafka. | `flink/sql/04-fraud-detection.sql` |
| RF-07 | Publicar o resultado da detecção no tópico Kafka `fraud_alerts`. | `flink/sql/04-fraud-detection.sql` |
| RF-08 | Permitir verificar etapas e preservar evidências da execução. | `operations/`, `evidence/` |

## Requisitos técnicos

- PostgreSQL de origem com logical replication e conectividade de rede a partir do conector gerenciado.
- Confluent Cloud com Kafka, Connect, Schema Registry e Flink.
- Confluent CLI autenticada e compatível com os comandos usados pelos scripts.
- Ferramentas locais: Bash, `psql`, `jq` e `curl`.
- Configurações e segredos mantidos fora do Git; arquivos locais devem respeitar as regras de `.gitignore`.
- Dados sintéticos determinísticos: 200 customers, 240 accounts, 320 cards e 2.000 transactions. A tabela merchants é auxiliar e não integra a publication CDC.

## Critérios de aceite

Os critérios abaixo devem ser avaliados com evidências do ambiente efetivamente executado.

| ID | Critério | Evidência/observação necessária |
|---|---|---|
| CA-01 | O conector CDC está em estado RUNNING. | Descrição do conector registrada em `evidence/cloud/03-cdc-connector.txt`. |
| CA-02 | O snapshot das quatro tabelas foi concluído com as contagens esperadas: 200/240/320/2000. | Logs por tópico e `summary.txt` gerados por `operations/verify-cdc-snapshot.sh`. |
| CA-03 | Os statements Flink foram submetidos e atingiram os estados esperados pelo fluxo. | `evidence/cloud/04-flink.txt`, conferindo COMPLETED/RUNNING conforme o plano. |
| CA-04 | O Temporal Join produz registros enriquecidos com a dimensão accounts. | Consulta/observação de dados de saída que demonstre campos da transação e da conta; o estado RUNNING do statement não basta. |
| CA-05 | As transações de teste são publicadas e processadas no fluxo. | Evidência do cenário positivo e observação dos eventos correspondentes no fluxo. |
| CA-06 | Três ou mais transações positivas do mesmo cartão em até 60 segundos produzem alerta. | Mensagem observada no tópico `fraud_alerts`, com cartão, contagem e tempos suficientes para demonstrar o padrão. |
| CA-07 | As evidências identificam data, ambiente e escopo realmente testado, sem expor segredos. | Arquivos em `evidence/` e registro do resultado/limitações. |
| CA-08 | O contrato Avro aceita a evolução compatível e rejeita a evolução incompatível configuradas no exercício. | `evidence/cloud/02-schema-registry.txt`, distinguindo o subject isolado `contract.transactions-value` do subject CDC real. |

## Critério complementar

O cenário negativo de três transações espaçadas além de 60 segundos pode ser usado para avaliar o limite temporal. O resultado deve ser observado no tópico de saída com tempo suficiente para considerar o processamento e o watermark. Esse teste é complementar ao aceite mínimo e não deve ser declarado aprovado apenas porque o SQL de carga terminou.

A verificação de CDC incremental (`INSERT`, `UPDATE`, `DELETE`) e a evolução do schema real do CDC também são verificações complementares, documentadas no plano de homologação. A evolução do subject isolado de contrato Avro não substitui a evolução do subject produzido pelo conector CDC.

## Limitações que afetam os critérios

- `NOT ENFORCED` declara metadados de chave ao Flink, mas não verifica unicidade real na origem.
- `$rowtime` representa o timestamp de evento Kafka utilizado pelo Flink, não o timestamp de negócio `created_at`.
- Append não equivale a filtrar exclusivamente INSERTs: eventos CDC de criação, snapshot e atualização podem entrar como novas linhas; exclusões são ignoradas conforme o SQL versionado.
- A regra é demonstrativa. Não implementa reconciliação completa de estado, deduplicação, avaliação de risco de produção ou persistência em PostgreSQL.
- A presença de scripts, SQL e documentação no repositório não comprova execução cloud. Critérios sem evidência permanecem pendentes.
