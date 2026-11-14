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

## Investigação complementar — disparo do padrão de fraude no vídeo

**Registro de 14/11/2026.** Esta seção separa o que foi efetivamente observado no trecho disponibilizado pelo usuário, as hipóteses de reconstrução e o que permanece sem comprovação. O mentor não disponibilizou o repositório/código-fonte; os comandos abaixo não são transcrição literal.

### Trecho observável do vídeo

O trecho recuperado mostra a seguinte sequência:

1. Um loop Bash executa três comandos docker run --rm postgres:16-alpine psql "$PGURL" -c "INSERT ..." em sequência.
2. O INSERT lista explicitamente as colunas transaction_id, account_id, card_id, merchant_id, channel, amount, mcc, uf_transacao e created_at.
3. O SELECT visível usa gen_random_uuid(), referencia payments.cards c e uma relação/alias m, e seleciona pelo menos c.account_id, c.card_id e m.merchant_id.
4. O texto do mentor descreve os três eventos como transações do mesmo cartão.
5. Em seguida, o mentor executa um quarto INSERT, descrito como uma transação “inocente”, para avançar a watermark.
6. Após aproximadamente 1–2 minutos, a consulta ao tópico desafio.fraud.detected deveria mostrar um alerta novo com txn_count = 3.

O material recuperado está cortado. Não foi possível observar integralmente o FROM, os JOIN, os filtros, os valores de todas as colunas, o comando completo da quarta transação nem a consulta de validação. Não atribuir ao mentor detalhes que não aparecem no trecho.

### Hipótese de reconstrução apresentada para os três INSERTs

O usuário propôs, como palpite e não como transcrição:

~~~bash
for i in 1 2 3; do
  docker run --rm postgres:16-alpine psql "$PGURL" -c "INSERT INTO payments.transactions (transaction_id, account_id, card_id, merchant_id, channel, amount, mcc, uf_transacao, created_at) SELECT gen_random_uuid(), c.account_id, c.card_id, m.merchant_id, 'POS', 100.00, m.mcc, 'SP', now() FROM payments.cards c, payments.merchants m ORDER BY c.card_id, m.merchant_id LIMIT 1;"
done
~~~

A hipótese seleciona repetidamente a combinação de menor card_id e menor merchant_id, mantendo o mesmo cartão nos três comandos, desde que os dados não mudem entre as execuções. Isso é coerente com a partição PARTITION BY card_id do reconhecimento de padrão. Os valores 'POS', 100.00, 'SP', o uso de m.mcc, now(), o produto cartesiano, o ORDER BY e o LIMIT 1 são suposições do usuário; não foram confirmados no vídeo.

### Hipótese de reconstrução apresentada para o quarto INSERT

O usuário propôs:

~~~bash
docker run --rm postgres:16-alpine psql "$PGURL" -c "INSERT INTO payments.transactions (transaction_id, account_id, card_id, merchant_id, channel, amount, mcc, uf_transacao, created_at) SELECT gen_random_uuid(), c.account_id, c.card_id, m.merchant_id, 'POS', 10.00, m.mcc, 'SP', now() FROM payments.cards c, payments.merchants m ORDER BY c.card_id DESC, m.merchant_id LIMIT 1;"
~~~

A intenção é selecionar o maior card_id, diferente do menor selecionado nos três primeiros INSERTs, e produzir um evento adicional. Com os dados atuais do projeto, há múltiplos cartões e merchants, mas essa consulta ainda é somente uma hipótese. Os valores e a razão exata pela qual o mentor chama o evento de “inocente” são desconhecidos.

### Confronto com o schema PostgreSQL do repositório público

O arquivo ativo database/init/01-database.sql define:

- transactions.transaction_id como BIGINT GENERATED BY DEFAULT AS IDENTITY, não UUID;
- colunas card_id, account_id, merchant_id, amount, currency, status e created_at;
- não define channel, mcc nem uf_transacao;
- merchants contém merchant_id, name, category e city; não contém mcc;
- merchants existe no PostgreSQL, mas fica fora da publication CDC.

Logo, os palpites do vídeo não podem ser executados sem adaptação contra o schema atual do repositório público. Em particular, gen_random_uuid() não é compatível com a coluna identity BIGINT, e as três colunas adicionais não existem no DDL atual.

Uma adaptação conceitual ao schema público poderia omitir transaction_id para deixar a identity gerar o valor e usar somente card_id, account_id, merchant_id, amount, currency, status e created_at. Não foi criada nem executada; não é transcrição do mentor nem alteração proposta ao arquivo de seed existente.

### Semântica do MATCH_RECOGNIZE e ressalva sobre a quarta transação

O SQL público flink/sql/04-fraud-detection.sql usa:

- PARTITION BY card_id;
- ORDER BY $rowtime;
- PATTERN (T{3,}?) WITHIN INTERVAL '60' SECOND;
- DEFINE T AS T.amount > 0.

Consequências:

- O critério explícito de elegibilidade de T é amount > 0. O SQL não classifica inocência por merchant, MCC, canal ou UF.
- Um quarto evento positivo em outro cartão não pertence à mesma partição das três primeiras transações, mas ainda pode ser um evento elegível em sua própria partição.
- Portanto, não se pode afirmar que a quarta transação fique fora do padrão apenas por ser chamada de “inocente” ou por ter outro card_id.
- O quantificador reluctant T{3,}? é compatível com a intenção de encerrar a correspondência no menor número de eventos que satisfaz o padrão; o comportamento concreto ainda depende do processamento temporal e da chegada dos eventos.

### Watermark: o que sabemos e o que falta verificar

O SQL de enrichment flink/sql/01-enrichment.sql usa o $rowtime de payments.transactions como tempo do evento e a saída enriquecida preserva o timestamp Kafka em uma coluna metadata rowtime. O SQL de fraude ordena por $rowtime, não por created_at.

O mentor atribui ao quarto INSERT o papel de avançar a watermark e liberar a emissão do alerta. Isso é uma explicação observada no vídeo, mas não está demonstrado pelos trechos de código disponíveis.

**Não é correto garantir que um evento de outro cartão avance a watermark global.** O resultado depende da atribuição de timestamps/watermarks na fonte, da distribuição entre partições Kafka, da propagação de watermarks e de eventual ociosidade de partições. O fato de o evento estar em outro card_id não permite concluir em qual partição Kafka ele será escrito nem qual watermark será emitida.

O plano atual aplica sql.tables.scan.idle-timeout aos statements contínuos, mas isso, isoladamente, não comprova toda a estratégia de watermark nem garante a emissão após o quarto INSERT.

### Estado desta investigação e próximo critério

- **Observado no vídeo:** três INSERTs em sequência para o mesmo cartão; quarto INSERT descrito como inocente e usado para avançar a watermark; consulta posterior espera alerta com txn_count = 3.
- **Hipótese do usuário:** comandos com gen_random_uuid(), payments.cards × payments.merchants, ORDER BY ... LIMIT 1, valores POS/amount/UF e seleção de outro cartão no quarto evento.
- **Conferido estaticamente:** incompatibilidades entre os campos/tipos do palpite e o DDL PostgreSQL público; condição amount > 0, partição por cartão, padrão reluctant e uso de $rowtime no SQL Flink público.
- **Não executado nesta investigação:** nenhum INSERT, SQL Flink, statement, consulta à Confluent Cloud ou alteração de ambiente. Neon e Confluent permanecem zerados conforme a confirmação mais recente do usuário.
- **Pendente:** antes de alterar o cenário de fraude, revisar a definição efetiva das tabelas Flink/timestamps, a estratégia de watermark e o particionamento do tópico de entrada; depois, em ambiente reconstruído e com autorização, testar separadamente o padrão de três eventos e o mecanismo de avanço da watermark, registrando os resultados observados.

Não substituir o cenário positivo versionado database/seed/simulate-events.sql por estes palpites. O arquivo atual usa quatro INSERTs positivos em uma única instrução para o mesmo cartão e continua sendo uma implementação distinta da sequência descrita no vídeo.
