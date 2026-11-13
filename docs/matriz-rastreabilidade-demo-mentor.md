# Rastreabilidade da demonstração do mentor

Este documento relaciona as etapas observáveis da demonstração de referência aos artefatos correspondentes neste repositório. A matriz descreve o escopo implementado nos arquivos versionados; **não comprova que os recursos tenham sido provisionados ou que as etapas tenham sido executadas na Confluent Cloud**.

## Matriz de rastreabilidade

| Etapa da demonstração | Artefato do projeto | Papel no fluxo |
|---|---|---|
| PostgreSQL como origem CDC | `database/init/` e `infrastructure/connectors/cdc/connector.json` | Prepara o schema/dataset e configura o PostgreSQL CDC Source V2. |
| Captura das tabelas | `infrastructure/connectors/cdc/connector.json` | Inclui customers, accounts, cards e transactions na captura CDC. |
| Verificação do snapshot | `operations/verify-cdc-snapshot.sh` | Consome os tópicos e compara os eventos de snapshot esperados por tabela. |
| Início de transactions no offset atual | `flink/sql/00-startup-transactions.sql` | Configura transactions para iniciar em latest-offset após a conclusão do snapshot. |
| Chave da dimensão accounts | `flink/sql/00-pk-accounts.sql` | Declara a chave primária NOT ENFORCED exigida pelo lado dimensional do Temporal Join. |
| Chave de transactions | `flink/sql/00-pk-transactions.sql` | Declara a chave primária NOT ENFORCED da tabela de eventos. |
| Temporal Join | `flink/sql/01-enrichment.sql` | Enriquece cada transação com a versão de accounts válida no rowtime do evento. |
| Entrada append-only | `flink/sql/02-append-only.sql` | Configura o changelog append para a etapa de reconhecimento de padrão. |
| Detecção de padrão temporal | `flink/sql/04-fraud-detection.sql` | Usa MATCH_RECOGNIZE para identificar três ou mais transações positivas por cartão em até 60 segundos de rowtime Kafka. |
| Cenário positivo | `database/seed/simulate-events.sql` | Insere quatro transações positivas para o mesmo cartão, em sequência. |
| Saída de alertas | Tópico Kafka `fraud_alerts` | Destino do resultado da detecção; o fluxo principal não inclui PostgreSQL Sink. |
| Verificação de CDC incremental | `operations/verify-cdc-incremental.sh` | Prepara a verificação de INSERT, UPDATE e DELETE na tabela customers. |

## Adaptações em relação à demonstração

- O projeto utiliza o domínio `payments` e os nomes de tabelas `customers`, `accounts`, `cards` e `transactions`.
- A tabela auxiliar `merchants` não integra a publication CDC.
- Após validar o snapshot, transactions é configurada para iniciar em `latest-offset`, evitando que a carga histórica seja interpretada como novos eventos pelo fluxo de fraude. A dimensão accounts permanece disponível para o Temporal Join.
- A saída principal é o tópico Kafka `fraud_alerts`; persistência em PostgreSQL não faz parte do escopo principal.
- O cenário positivo usa INSERTs novos. Isso demonstra a regra pretendida, mas não elimina as limitações gerais da transformação append.

## Semântica e limites de interpretação

- A janela de 60 segundos usa o timestamp de evento Kafka (`$rowtime`), não o campo de negócio `created_at`.
- As chaves declaradas como `NOT ENFORCED` são metadados para o Flink; não verificam unicidade na origem.
- O modo append não equivale a filtrar exclusivamente operações Debezium `op=c`. Conforme a configuração documentada no SQL, eventos `c`, `r` e `u` são tratados como novas linhas e `d` é ignorado. Portanto, atualizações podem contribuir para a contagem do padrão.
- A regra é didática e não representa reconciliação completa do estado das entidades, deduplicação ou um motor de risco de produção.
- A presença dos scripts e SQL nesta matriz indica rastreabilidade documental, não execução bem-sucedida. O estado de cada etapa deve ser registrado somente com evidência produzida e verificada durante a homologação.

## Critério para registrar conclusão

Uma etapa só deve ser marcada como executada ou homologada quando houver evidência correspondente em `evidence/`, com contexto suficiente para identificar o comando/ação, o resultado observado e as limitações. Scripts preparados, instruções e resultados históricos de ambientes já excluídos não comprovam a execução no ambiente atual.
