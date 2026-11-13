# Plano de homologação

Este plano organiza a validação do laboratório a partir dos scripts e SQL versionados. Ele é um roteiro de execução, não evidência de que o ambiente foi provisionado ou de que os testes passaram.

> **Estado inicial:** trate cada reconstrução como ambiente novo. Confirme no console o que existe, a região e os créditos disponíveis. Não reutilize IDs, credenciais ou arquivos de estado de ambientes anteriores sem verificação.

## 1. Pré-verificação e preparação

Antes de executar qualquer script:

1. Confirme conta, região, créditos e limites de custo na Confluent Cloud.
2. Confirme que o PostgreSQL é dedicado ao laboratório, acessível pelo conector e aceita logical replication.
3. Prepare \`.env\` a partir de \`.env.example\`, mantendo credenciais somente localmente.
4. Crie/valide a role PostgreSQL dedicada com \`LOGIN\` e \`REPLICATION\`; \`infrastructure/connectors/cdc/connector.sh\` verifica esses atributos, mas não cria a role.
5. Revise os nomes de Environment, cluster, compute pool e os contextos selecionados na CLI.
6. Não execute a inicialização do banco sem confirmar host, database e o efeito de recriar o schema \`payments\`.

A inicialização do banco é destrutiva para o schema \`payments\` existente e exige confirmação explícita:

\`\`\`bash
CONFIRM_RESET=yes ./scripts/run-sql.sh database/init/00-database.sql
\`\`\`

Use somente em banco dedicado e após conferir a conexão.

## 2. Fundação Confluent

**Ação:** \`./infrastructure/foundation.sh\`

O script pode criar Environment, cluster Kafka Basic, service accounts, API keys e ACLs. Também seleciona o contexto da CLI, aguarda o cluster ficar \`UP\` e grava o estado local e a evidência.

**Evidência esperada:** \`evidence/cloud/01-foundation.txt\`.

**Critério:** confirmar IDs/nome do Environment e cluster, estado \`UP\`, bootstrap e ACLs esperadas. A conclusão do script não dispensa a leitura da evidência e a conferência do contexto.

## 3. Schema Registry e contrato Avro

**Ação:** \`./infrastructure/schema-registry.sh\`

O script configura compatibilidade \`BACKWARD\` e testa um subject de contrato isolado, \`contract.transactions-value\`: evolução com campo opcional/default aceita e campo obrigatório sem default rejeitado.

**Evidência esperada:** \`evidence/cloud/02-schema-registry.txt\`.

**Critério:** conferir os dois resultados de compatibilidade e as versões registradas. Esse teste de contrato não comprova, por si só, evolução do subject real produzido pelo conector CDC.

## 4. PostgreSQL CDC

Após preparar banco, tabelas, publication e role, execute:

\`\`\`bash
./infrastructure/connectors/cdc/connector.sh
\`\`\`

O script verifica a role, impede reutilização automática de um slot \`payments_slot\` existente, renderiza a configuração localmente e cria o conector \`payments-postgres-cdc\`.

**Evidência esperada:** \`evidence/cloud/03-cdc-connector.txt\`.

**Critério:** conector em \`RUNNING\` e identificação dos quatro tópicos: \`payments.customers\`, \`payments.accounts\`, \`payments.cards\` e \`payments.transactions\`. \`RUNNING\` não comprova que o snapshot terminou.

## 5. Snapshot inicial

Execute:

\`\`\`bash
./operations/verify-cdc-snapshot.sh
\`\`\`

O verificador consome os quatro tópicos desde o início e contabiliza eventos Debezium de snapshot (\`op=r\`).

**Evidência esperada:** diretório datado em \`evidence/homologation/\`, com logs por tópico e \`summary.txt\`.

**Contagens esperadas:**

| Tópico | Eventos de snapshot |
|---|---:|
| \`payments.customers\` | 200 |
| \`payments.accounts\` | 240 |
| \`payments.cards\` | 320 |
| \`payments.transactions\` | 2.000 |

**Critério:** cada contagem observada deve corresponder à esperada. Investigue divergências antes de iniciar os statements Flink.

## 6. Flink SQL

Somente após confirmar o snapshot, revise e execute:

\`\`\`bash
./flink/scripts/run.sh
\`\`\`

O script seleciona o contexto Flink, pode criar um compute pool se não encontrar um disponível com o nome configurado e submete, em sequência, os statements de startup offset, PKs, enrichment, append e detecção. Ele interrompe a sequência se um statement com o mesmo nome já existir; não sobrescreve automaticamente.

**Evidência esperada:** \`evidence/cloud/04-flink.txt\`.

**Critério mínimo:** verificar os estados esperados registrados pelo script:
- startup offset e declarações de PK: \`COMPLETED\`;
- enrichment e detecção: \`RUNNING\`;
- append: \`COMPLETED\`.

Esses estados confirmam o estado dos statements, não a correção funcional do Temporal Join nem a emissão de alertas. A validação funcional exige observar dados enriquecidos e mensagens de saída.

## 7. Cenário positivo de fraude

Somente após confirmar os statements necessários em execução:

\`\`\`bash
./scripts/run-sql.sh database/seed/simulate-events.sql
\`\`\`

O SQL insere quatro transações positivas para o cartão 1. O wrapper registra a execução em \`evidence/homologation/\`.

**Critério:** observar no tópico \`fraud_alerts\` o alerta correspondente ao padrão. Registrar a mensagem e o contexto de consumo como evidência. A execução dos INSERTs ou a consulta ao PostgreSQL não comprovam, isoladamente, a emissão do alerta.

## 8. Cenário negativo opcional

O arquivo \`database/seed/slow-pattern.sql\` insere três transações positivas para o cartão 4, com intervalos de aproximadamente 35 segundos entre elas; o primeiro e o terceiro eventos ficam separados por mais de 60 segundos.

Execute somente com os statements Flink ativos:

\`\`\`bash
./scripts/run-sql.sh database/seed/slow-pattern.sql
\`\`\`

**Critério:** observar o comportamento do tópico \`fraud_alerts\` e registrar a janela temporal e o resultado. Não declarar ausência de alerta sem observação suficiente do processamento/watermark. O arquivo prepara o cenário; não realiza sozinho a verificação do resultado.

## 9. Verificações complementares opcionais

### CDC incremental

\`\`\`bash
./operations/verify-cdc-incremental.sh
\`\`\`

O script cria um registro temporário exclusivo, executa INSERT/UPDATE/DELETE, procura os eventos \`c\`, \`u\` e \`d\` associados ao registro e tenta remover o registro no encerramento.

**Evidência esperada:** arquivo datado \`*-cdc-incremental.txt\` em \`evidence/homologation/\`.

### Evolução do schema real do CDC

\`\`\`bash
CONFIRM_SCHEMA_EVOLUTION=yes ./infrastructure/schema-evolution.sh
\`\`\`

Esta etapa é distinta do teste de compatibilidade isolado da etapa 3. Ela altera a tabela \`payments.transactions\`, adiciona a coluna \`channel\` e insere um registro para provocar nova versão no subject real \`payments.transactions-value\`. Exige confirmação explícita e deve ser executada somente após conferir o banco e o subject.

**Evidência esperada:** \`evidence/homologation/07-real-schema-evolution.txt\`.

## 10. Registro, custos e encerramento

- Preserve logs e resultados em \`evidence/\`; registre data, ambiente, escopo e resultado observado.
- Classifique cada etapa como aprovada, falhou, parcial, não executada ou bloqueada.
- Não inclua credenciais, API secrets, connection strings ou arquivos renderizados nas evidências.
- Consulte \`operations/cost-and-metrics.md\` antes, durante e após o laboratório.
- Não deixe cluster, compute pool ou connector ativos sem necessidade.
- Teardown é uma etapa separada: confira dependências e escopo destrutivo antes de qualquer limpeza. Não execute scripts de remoção automaticamente.

## Limites da homologação

- A janela de fraude usa \`$rowtime\` Kafka, não \`created_at\`.
- \`NOT ENFORCED\` declara metadados de chave no Flink; não verifica unicidade na origem.
- O changelog append não filtra exclusivamente INSERTs: eventos CDC de criação, snapshot e atualização podem ser tratados como novas linhas, enquanto exclusões são ignoradas, conforme o SQL versionado.
- \`MATCH_RECOGNIZE\` depende da semântica temporal/watermark do Flink; a janela de 60 segundos não deve ser descrita como garantia de emissão imediata em relógio de parede.
- Nenhum resultado histórico de ambientes já excluídos comprova homologação no ambiente atual.
