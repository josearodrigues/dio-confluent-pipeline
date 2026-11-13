# Rastreabilidade da demonstração do mentor

Este documento relaciona as etapas observáveis da demonstração de referência aos artefatos correspondentes neste repositório. A matriz descreve o escopo implementado nos arquivos versionados; **não comprova que os recursos tenham sido provisionados ou que as etapas tenham sido executadas na Confluent Cloud**.

## Matriz de rastreabilidade

| Etapa da demonstração | Artefato do projeto | Papel no fluxo |
|---|---|---|
| Passo 3 — PostgreSQL como origem CDC | `database/init/` e `infrastructure/connectors/cdc/connector.json` | Prepara o schema/dataset e configura a origem PostgreSQL para captura CDC. |
| Passo 4 — Conector CDC | `infrastructure/connectors/cdc/connector.json` | Inclui customers, accounts, cards e transactions na captura CDC e publica os tópicos Kafka. |
| Passo 5 — Snapshot e CDC incremental | `operations/verify-cdc-snapshot.sh` e `operations/verify-cdc-incremental.sh` | Valida o snapshot inicial e, em seguida, observa INSERT, UPDATE e DELETE (eventos Debezium `c`, `u`, `d`) em tempo real. |
| Passo 6 — Contrato e compatibilidade Avro | `infrastructure/schema-registry.sh`, `schemas/transaction-v1.avsc`, `schemas/transaction-v2.avsc` | Registra v1, testa evolução compatível com campo opcional/default e confirma rejeição de campo obrigatório sem default no subject isolado `contract.transactions-value`. |
| Extensão do projeto — Evolução do schema CDC real | `infrastructure/schema-evolution.sh` | Após o CDC incremental, adiciona `channel` à tabela `payments.transactions` e valida a publicação da nova versão no subject real `payments.transactions-value`. Não é o teste isolado do Passo 6 do mentor. |
| Passo 7 — Startup offset e chaves Flink | `flink/sql/00-startup-transactions.sql`, `flink/sql/00-pk-accounts.sql`, `flink/sql/00-pk-transactions.sql` | Configura transactions para iniciar em `latest-offset` após o snapshot e declara as chaves `NOT ENFORCED` necessárias ao processamento. |
| Passo 7 — Temporal Join | `flink/sql/01-enrichment.sql` | Enriquece cada transação com a versão de accounts válida no rowtime do evento. |
| Passo 7 — Entrada append-only | `flink/sql/02-append-only.sql` | Configura o changelog append para a etapa de reconhecimento de padrão. |
| Passo 7 — Detecção de padrão temporal | `flink/sql/04-fraud-detection.sql` | Usa MATCH_RECOGNIZE para identificar três ou mais transações positivas por cartão em até 60 segundos de rowtime Kafka. |
| Passo 7 — Cenário positivo e saída | `database/seed/simulate-events.sql` e tópico Kafka `fraud_alerts` | Insere quatro transações positivas para o mesmo cartão e publica o resultado da detecção no tópico de alertas. O fluxo principal não inclui PostgreSQL Sink. |

## Ordem de execução no projeto e diferença em relação à demo

A sequência conceitual da demonstração é: **Passo 5 — CDC incremental; Passo 6 — evolução compatível/incompatível de contrato Avro em subject isolado; Passo 7 — Flink**.

Há duas diferenças de implementação que precisam permanecer explícitas:

1. Neste repositório, `infrastructure/schema-registry.sh` provisiona o Schema Registry **e executa** o teste de contrato isolado na etapa de preparação, antes do conector CDC. Portanto, o teste correspondente ao Passo 6 do mentor é executado antecipadamente no roteiro atual; não deve ser repetido depois, pois o script interrompe se o subject já existir.
2. `infrastructure/schema-evolution.sh` é uma validação adicional deste projeto: evolui o subject real produzido pelo CDC. Ela é executada depois do CDC incremental e antes do Flink, mas **não substitui nem corresponde ao teste isolado do Passo 6**.

Assim, a ordem operacional atual após o snapshot é: CDC incremental (Passo 5) → evolução adicional do subject CDC real → Flink (Passo 7). O teste isolado do contrato (Passo 6) já foi executado antes do CDC pela organização atual do script de Schema Registry.
## Adaptações em relação à demonstração

- O projeto utiliza o domínio `payments` e os nomes de tabelas `customers`, `accounts`, `cards` e `transactions`.
- A tabela auxiliar `merchants` não integra a publication CDC.
- O teste isolado de compatibilidade Avro e a evolução adicional do subject CDC real são validações distintas, com subjects e objetivos diferentes.
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
