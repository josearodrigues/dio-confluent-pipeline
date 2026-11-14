# Plano de homologação

Este plano organiza a validação do laboratório a partir dos scripts e SQL versionados. Ele é um roteiro de execução, não evidência de que o ambiente foi provisionado ou de que os testes passaram.

> **Estado inicial:** trate cada reconstrução como ambiente novo. Confirme no console o que existe, a região e os créditos disponíveis. Não reutilize IDs, credenciais ou arquivos de estado de ambientes anteriores sem verificação.

## 1. Pré-verificação e preparação

Antes de executar qualquer script:

1. Confirme conta, região, créditos e limites de custo na Confluent Cloud.
2. Confirme que o PostgreSQL é dedicado ao laboratório, acessível pelo conector e aceita logical replication.
3. Prepare `.env` a partir de `.env.example`, mantendo credenciais somente localmente.
4. Crie/valide a role PostgreSQL dedicada com `LOGIN` e `REPLICATION`; `infrastructure/connectors/cdc/connector.sh` verifica esses atributos, mas não cria a role.
5. Revise os nomes de Environment, cluster, compute pool e os contextos selecionados na CLI.
6. Não execute a inicialização do banco sem confirmar host, database e o efeito de recriar o schema `payments`.

A inicialização do banco é destrutiva para o schema `payments` existente e exige confirmação explícita:

```bash
CONFIRM_RESET=yes ./scripts/run-sql.sh database/init/01-database.sql
```

Use somente em banco dedicado e após conferir a conexão.

## 2. Fundação Confluent

**Ação:** `./infrastructure/foundation.sh`

O script pode criar Environment, cluster Kafka Basic, service accounts, API keys e ACLs. Também seleciona o contexto da CLI, aguarda o cluster ficar `UP` e grava o estado local e a evidência.

**Evidência esperada:** `evidence/cloud/01-foundation.txt`.

**Critério:** confirmar IDs/nome do Environment e cluster, estado `UP`, bootstrap e ACLs esperadas. A conclusão do script não dispensa a leitura da evidência e a conferência do contexto.

## 3. Schema Registry e contrato Avro

**Ação:** `./infrastructure/schema-registry.sh`

O script configura compatibilidade `BACKWARD` e testa um subject de contrato isolado, `contract.transactions-value`: evolução com campo opcional/default aceita e campo obrigatório sem default rejeitado.

**Evidência esperada:** `evidence/cloud/02-schema-registry.txt`.

**Critério:** conferir os dois resultados de compatibilidade e as versões registradas. Esse teste de contrato não comprova, por si só, evolução do subject real produzido pelo conector CDC.

## 4. PostgreSQL CDC

Após preparar banco, tabelas, publication e role, execute:

```bash
./infrastructure/connectors/cdc/connector.sh
```

O script verifica a role, impede reutilização automática de um slot `payments_slot` existente, renderiza a configuração localmente e cria o conector `payments-postgres-cdc`.

**Evidência esperada:** `evidence/cloud/03-cdc-connector.txt`.

**Critério:** conector em `RUNNING` e identificação dos quatro tópicos: `payments.customers`, `payments.accounts`, `payments.cards` e `payments.transactions`. `RUNNING` não comprova que o snapshot terminou.

## 5. Snapshot inicial

Execute:

```bash
./operations/verify-cdc-snapshot.sh
```

O verificador consome os quatro tópicos desde o início e contabiliza eventos Debezium de snapshot (`op=r`).

**Evidência esperada:** diretório datado em `evidence/homologation/`, com logs por tópico e `summary.txt`.

**Contagens esperadas:**

| Tópico | Eventos de snapshot |
|---|---:|
| `payments.customers` | 200 |
| `payments.accounts` | 240 |
| `payments.cards` | 320 |
| `payments.transactions` | 2.000 |

**Critério:** cada contagem observada deve corresponder à esperada. Na execução homologada de 13/11/2026, os quatro tópicos atingiram 2.760/2.760 eventos `op=r` com timeout de 2.400 segundos; resultado `target-reached`. Snapshot: **homologado**.

## 6. CDC incremental

Após o snapshot inicial completo e antes de iniciar o Flink, valide alterações em tempo real:

```bash
./operations/verify-cdc-incremental.sh
```

O script usa um consumer group exclusivo `snapshot-check-*` (ACL prefixada provisionada pela fundação), sem `--from-beginning`, cria um registro temporário, executa INSERT/UPDATE/DELETE e procura os eventos `c`, `u` e `d` associados ao marcador. A evidência deve preservar as mensagens Kafka e os offsets.

**Evidência esperada:** arquivo datado `*-cdc-incremental.txt` em `evidence/homologation/`.

**Critério:** correlacionar a linha de origem com os três eventos Debezium e confirmar `snapshot=false`. O teste deve ocorrer antes do Flink, como no Passo 5 da demonstração do mentor.

**Estado em 14/11/2026:** verificador oficial executado com sucesso. INSERT (`op=c`, offset 231), UPDATE (`op=u`, offset 232) e DELETE (`op=d`, offset 233) foram correlacionados ao mesmo marcador na partição 0, todos com `snapshot=false`. Evidência local: `evidence/homologation/20261114T005129Z-cdc-incremental.txt`. O resultado confirma a captura incremental do Debezium no Kafka nesta execução. O teste temporário anterior (offsets 223–225) permanece como evidência complementar, não como substituto da homologação pelo verificador oficial.

## 7. Evolução adicional do schema real do CDC (extensão do projeto)

Depois do teste incremental e antes de criar statements Flink, valide a evolução do subject efetivamente usado pelo conector:

```bash
CONFIRM_SCHEMA_EVOLUTION=yes ./infrastructure/schema-evolution.sh
```

Esta é uma extensão do projeto, não o Passo 6 da demonstração do mentor. O Passo 6 testa compatibilidade em um subject Avro isolado (`contract.transactions-value`), por meio de `infrastructure/schema-registry.sh`, atualmente executado na etapa 3 deste plano. Esta etapa adicional altera a tabela `payments.transactions`, adiciona a coluna `channel` e insere um registro para provocar nova versão no subject real `payments.transactions-value`. Exige confirmação explícita e deve ser executada somente após conferir o banco e o subject.

**Evidência esperada:** arquivo datado `evidence/homologation/<timestamp>-real-schema-evolution.txt`, gerado pelo próprio script.

**Critério:** comprovar a publicação do novo schema pelo CDC e a captura do evento no tópico. Não corresponde ao Passo 6 da demonstração; é uma validação adicional do subject real.

**Estado em 14/11/2026:** **homologado**. Execução de `infrastructure/schema-evolution.sh` concluída com sucesso. O subject `payments.transactions-value` evoluiu da versão 1 para a 2; a compatibilidade efetiva permaneceu `BACKWARD` e `is_compatible=true`; o schema novo contém `channel`; o conector `payments-postgres-cdc` permaneceu `RUNNING`; e o evento correlacionado `schema-evolution-20261114T012247Z-27263` foi observado no tópico `payments.transactions`, partição 0, offset 2000, com `op=c` e `snapshot=false`. Evidência versionada: `evidence/homologation/20261114T012247Z-real-schema-evolution.txt` (commit `22182874a9f3059e18c038ad4e778ee9fca3cac7`).
## 8. Flink SQL

Com snapshot, CDC incremental e evolução do schema real homologados por evidências da instância atual, a próxima etapa é revisar e, mediante autorização explícita, executar:

```bash
./flink/scripts/run.sh
```

O script seleciona o contexto Flink e reutiliza o compute pool configurado. Na primeira execução, se o pool de destino ainda não existir, reconhece somente o pool legado previamente identificado (`lfcp-2268w8o` / `default.env-mn3vgw.us-east-2`), valida ID, nome, environment, cloud, região, status e capacidade, e o renomeia para o nome configurado preservando o `max_cfu`. Não cria compute pools automaticamente; em caso de ausência, duplicidade ou incompatibilidade, interrompe. Depois, submete em sequência as PKs de `accounts` e `transactions`, enrichment, append e detecção. A propriedade `sql.tables.scan.idle-timeout` é aplicada às consultas contínuas de enrichment e detecção por `--property` da CLI, com padrão de `5 s`; isso reproduz o efeito do `SET` do mentor em cada statement criado separadamente. Não há statement separado de `SET`. Interrompe se um statement com o mesmo nome já existir; não sobrescreve automaticamente.

**Evidência registrada:** `evidence/homologation/20261114T052012Z-flink-fraud-detection-partial.md` (execução parcial de 14/11/2026).

**Estado em 14/11/2026 — execução parcial observada:** o pool existente `lfcp-2268w8o` (`desafio-pool`) foi validado pelo usuário. As declarações de PK de accounts e transactions foram observadas como `COMPLETED`; `payments-enrichment-test` permaneceu `RUNNING`; `payments-append-only-test` concluiu com `COMPLETED` e `Command completed successfully.`. O statement `payments-fraud-detection-test` foi aceito às 05:20:12 UTC e observado `RUNNING`. O catálogo SQL listou `fraud_alerts`; seu DDL expôs `$rowtime` como `ROWTIME` com `SOURCE_WATERMARK()`. Consulta resumida observou 641 linhas e `txn_count=3` em todas (mínimo e máximo iguais a 3). Evidência: `evidence/homologation/20261114T052012Z-flink-fraud-detection-partial.md`.

**Limites:** a consulta resumida gerou `AGGREGATION_WITHOUT_KEY`; foi exploratória e não deve permanecer ativa. Não houve correlação dos alertas com transações de origem, confirmação do cenário que os gerou, validação isolada da janela de 60 segundos/watermark nem confirmação de mensagens no tópico Kafka de saída. Portanto, o Passo 7 está parcialmente executado/observado, mas a detecção de fraude e a pipeline ponta a ponta ainda não estão homologadas.

**Critério mínimo:** verificar os estados esperados registrados pelo script:
- declarações de PK de `accounts` e `transactions`: `COMPLETED`;
- enrichment e detecção: `RUNNING`;
- append: `COMPLETED`.

Esses estados confirmam o estado dos statements, não a correção funcional do Temporal Join nem a emissão de alertas. A validação funcional exige observar dados enriquecidos e mensagens de saída.



**Atualização — preflight global de statements (14/11/2026):** depois de resolver e validar o compute pool, mas antes de submeter qualquer statement, o orquestrador consulta a lista do pool e verifica os cinco nomes planejados (payments-pk-accounts, payments-pk-transactions, payments-enrichment, payments-append-only, payments-fraud-detection). Se qualquer nome já existir, interrompe antes da primeira submissão; não exclui nem sobrescreve statements. A verificação individual em submit() permanece como proteção adicional. Alteração publicada no commit 4dfe1228f91f4cbd383c041cdbfcba9dabe3a58f. Ainda depende de sincronização e bash -n local. Nenhuma operação Cloud foi executada.

## 9. Cenário positivo de fraude — sequência do mentor

Somente após confirmar que o snapshot CDC terminou e que os statements de enrichment e detecção estão em `RUNNING`, execute:

```bash
./scripts/run-sql.sh database/seed/simulate-events.sql
```

O arquivo executa quatro INSERTs independentes, nesta ordem:
1. Três transações positivas de R$ 100,00 para o cartão 1 (conta 1, merchant 1).
2. Uma quarta transação de R$ 10,00 para outro cartão (cartão 320, conta 80, merchant 1).
3. Aguarda 90 segundos (`pg_sleep(90)`) como janela de observação indicada na demonstração.
4. Consulta as linhas inseridas; cada INSERT também retorna seu `transaction_id`.

Os IDs de cartão/conta/merchant foram conferidos no PostgreSQL do ambiente atual antes da preparação do cenário. O arquivo executa as instruções em sequência e o wrapper registra a saída em `evidence/homologation/`.

**Importante:** o SQL Flink atual usa `$rowtime`, não `created_at`, e define `T` apenas como `amount > 0`. A quarta transação, por usar outro `card_id`, não pertence à mesma partição do padrão dos três primeiros eventos. Seu possível efeito sobre a watermark precisa ser observado; os 90 segundos são uma janela de espera, não garantia de emissão.

**Critério:** após a execução, observar no tópico `fraud_alerts` um alerta novo correlacionável ao cartão 1, com `txn_count = 3`, e registrar a mensagem/offset/timestamps como evidência. A execução dos INSERTs, a espera ou a consulta ao PostgreSQL não comprovam, isoladamente, a emissão do alerta.

**Execução observada em 14/11/2026:** `database/seed/simulate-events.sql` foi executado com sucesso. Os INSERTs produziram `transaction_id` 2002, 2003 e 2004 para `card_id=1` (R$ 100,00 cada) e `transaction_id=2005` para `card_id=320` (R$ 10,00), todos às 06:11:29 UTC. Evidência: `evidence/homologation/20261114T061128Z-simulate-events.log`.

A consulta `SELECT * FROM fraud_alerts` foi executada às 06:52:41 UTC e retornou resultados, incluindo `card_id=1`, `txn_count=3`, com `first_txn=05:03:20.542` e `last_txn=05:03:22.758` UTC. Esses timestamps são anteriores aos INSERTs do cenário positivo (06:11:29 UTC); portanto, os resultados não podem ser atribuídos a esses quatro registros com a evidência disponível. Evidência: `evidence/homologation/20261114T065241Z-fraud-detected-query.txt`. **Classificação:** cenário positivo executado; consulta observada; correlação e homologação funcional pendentes. Não afirmar que a regra detectou o cenário positivo.

**Atualização da investigação — 14/11/2026:** consulta read-only a `enriched_payments` confirmou as transações 2002, 2003 e 2004 para `card_id=1`, mas seus `$rowtime` foram, respectivamente, `06:11:30.774`, `06:11:30.775` e `06:16:44.383` UTC. O intervalo entre a primeira e a terceira foi de 5 min 13,609 s, superior aos 60 segundos exigidos pelo padrão. A consulta a `fraud_alerts` retornou três registros históricos para `card_id=1`, todos com `txn_count=3` e timestamps entre `05:03:20.541` e `05:03:22.758` UTC; nenhum corresponde ao cenário das transações 2002–2004. **Pendência documentada:** ainda falta executar e comprovar um cenário positivo controlado em que pelo menos três eventos positivos do mesmo cartão cheguem ao Kafka em até 60 segundos e sejam correlacionados a um alerta novo (incluindo a evidência da saída/offset). Essa correção e nova homologação foram deliberadamente adiadas para depois da conclusão da observabilidade e do teardown. Até lá, não declarar a detecção funcional de fraude homologada.

**Validação e encerramento automatizados:** após executar o cenário positivo, informe ao verificador a janela UTC esperada dos timestamps Flink `$rowtime` (formato `YYYY-MM-DD HH:MM:SS.mmm`):

```bash
./operations/verify-fraud-alerts.sh '2026-11-14 06:11:00.000' '2026-11-14 06:20:00.000'
```

O exemplo de horário é ilustrativo; use os limites do cenário da execução atual. O verificador consulta somente os alertas do cartão 1, limita a leitura aos 100 registros mais recentes, exige `txn_count >= 3` e confere se `first_txn` e `last_txn` estão dentro da janela informada. A consulta é pontual e limitada; não substitui a leitura direta do tópico Kafka. Se não houver correspondência, o script reprova e orienta preservar os statements para diagnóstico.

Somente quando a evidência terminar com `VALIDAÇÃO APROVADA`, execute o encerramento passando o caminho exato do arquivo gerado:

```bash
./flink/scripts/stop-after-validation.sh evidence/homologation/<timestamp>-fraud-alerts-validation.txt
```

O encerrador exige evidência aprovada recente (até duas horas), identifica sem ambiguidade os statements de enrichment e fraude no compute pool configurado, solicita a parada e consulta novamente cada estado até confirmar `STOPPED`. Reconhece os nomes atuais e os aliases históricos terminados em `-test`; se encontrar ausência, duplicidade ou estado inesperado, aborta sem iniciar as paradas. A operação não exclui os statements. Se a validação falhar, não execute o encerrador.


**Estado operacional às 07:16:44 UTC:** cluster Kafka `UP`, conector `payments-postgres-cdc` `RUNNING`, compute pool `desafio-pool` `PROVISIONED` com `Current CFU=2`, `Max CFU=50`; evidência `evidence/cloud/20261114T071644Z-operational-state.txt`. A coleta registra inventário operacional e estados de statements; não comprova a correlação funcional nem a publicação do alerta no tópico Kafka.

## 10. Cenário negativo opcional

O arquivo `database/seed/slow-pattern.sql` insere três transações positivas para o cartão 4, com intervalos de aproximadamente 35 segundos entre elas; o primeiro e o terceiro eventos ficam separados por mais de 60 segundos.

Execute somente com os statements Flink ativos:

```bash
./scripts/run-sql.sh database/seed/slow-pattern.sql
```

**Critério:** observar o comportamento do tópico `fraud_alerts` e registrar a janela temporal e o resultado. Não declarar ausência de alerta sem observação suficiente do processamento/watermark. O arquivo prepara o cenário; não realiza sozinho a verificação do resultado.

## 11. Verificações complementares opcionais

A validação CDC incremental (Passo 5 do mentor) foi executada e confirmada em 14/11/2026: INSERT (`op=c`, offset 231), UPDATE (`op=u`, offset 232) e DELETE (`op=d`, offset 233), todos com `snapshot=false`, correlacionados pelo mesmo marcador na partição 0. Evidência: `evidence/homologation/20261114T005129Z-cdc-incremental.txt`. A evolução adicional do subject CDC real também foi homologada em 14/11/2026: subject da versão 1 para 2, `BACKWARD`, campo `channel` e evento `op=c` observado no tópico, partição 0, offset 2000. Evidência: `evidence/homologation/20261114T012247Z-real-schema-evolution.txt`. O teste isolado de contrato Avro (Passo 6 do mentor) é distinto e executado pelo script da etapa 3.

## 12. Registro, custos e encerramento

- Preserve logs e resultados em `evidence/`; registre data, ambiente, escopo e resultado observado.
- Classifique cada etapa como aprovada, falhou, parcial, não executada ou bloqueada.
- Não inclua credenciais, API secrets, connection strings ou arquivos renderizados nas evidências.
- Consulte `operations/cost-and-metrics.md` antes, durante e após o laboratório.
- Não deixe cluster, compute pool ou connector ativos sem necessidade.
- Teardown é uma etapa separada: confira dependências e escopo destrutivo antes de qualquer limpeza. Não execute scripts de remoção automaticamente.

## Limites da homologação

- A janela de fraude usa `$rowtime` Kafka, não `created_at`.
- `NOT ENFORCED` declara metadados de chave no Flink; não verifica unicidade na origem.
- O changelog append não filtra exclusivamente INSERTs: eventos CDC de criação, snapshot e atualização podem ser tratados como novas linhas, enquanto exclusões são ignoradas, conforme o SQL versionado.
- `MATCH_RECOGNIZE` depende da semântica temporal/watermark do Flink; a janela de 60 segundos não deve ser descrita como garantia de emissão imediata em relógio de parede.
- Nenhum resultado histórico de ambientes já excluídos comprova homologação no ambiente atual.


## Ordem didática e operacional

A sequência conceitual da demonstração é: Passo 5 (CDC incremental: INSERT/UPDATE/DELETE), Passo 6 (compatibilidade de contrato Avro em subject isolado) e Passo 7 (Flink: Temporal Join, append e MATCH_RECOGNIZE). Neste projeto, o teste isolado do Passo 6 é executado na etapa 3, antes do CDC, porque `schema-registry.sh` combina provisionamento e teste de contrato. A evolução do subject CDC real, executada após o CDC incremental e antes do Flink, é uma extensão adicional e não deve ser chamada de Passo 6.

## Método de trabalho para a etapa Flink: um arquivo por vez

A etapa Flink será conduzida pelo mesmo método incremental já utilizado no projeto privado de estudos. **Não executar a sequência inteira de uma vez antes de revisar e compreender cada unidade.**

Para cada script ou arquivo SQL, seguir este ciclo:

1. **Localizar e ler:** consultar o arquivo ativo no GitHub e os documentos técnicos relacionados. Consultar também o arquivo equivalente no projeto privado de estudos, quando existir.
2. **Comparar:** identificar diferenças entre a versão ativa e a versão de estudos; preservar o comportamento já exercitado e as decisões do projeto atual. Não copiar nem reescrever automaticamente.
3. **Analisar:** explicar o objetivo, pré-requisitos, efeitos, riscos, dependências e critérios de sucesso. Separar claramente a transcrição do mentor das adaptações do projeto.
4. **Propor correção mínima:** se houver problema comprovado, apresentar o diff e a justificativa antes de alterar. Não refatorar por preferência.
5. **Validar localmente:** após a sincronização pelo usuário, executar as verificações estáticas pertinentes (por exemplo, `bash -n` para Bash). Sintaxe válida não significa execução operacional válida.
6. **Executar uma unidade:** somente após revisão e autorização explícita, o usuário executa o script/comando daquela unidade. Não iniciar a próxima unidade na mesma tacada.
7. **Inspecionar:** analisar a saída, consultar o estado efetivo do recurso/statement e conferir a evidência produzida. Em caso de falha, interromper a sequência e diagnosticar antes de tentar novamente.
8. **Registrar:** classificar como preparado, validado estaticamente, executado, observado, homologado, falhou ou bloqueado, conforme a evidência disponível. Só então avançar.

### Aplicação ao Passo 7

A unidade inicial é a revisão do orquestrador `flink/scripts/run.sh` e dos SQLs que ele submete. A revisão deve identificar dependências e ordem de submissão, mas não implica executar o orquestrador.

Os arquivos SQL devem ser tratados individualmente, na ordem de dependência definida após a revisão. A existência de um statement ou recurso na Confluent Cloud deve ser consultada antes de qualquer criação; não presumir que o ambiente está vazio ou que um comando pode ser repetido com segurança.

O script `flink/scripts/run.sh` pode renomear o pool legado previamente identificado e submeter vários statements em sequência; não cria compute pools. Portanto, **permanece fora de execução até que seus efeitos sejam compreendidos e que cada unidade relevante tenha sido revisada**. Nenhuma operação Cloud/Neon está autorizada por este registro.

### Critério para avançar

Só avançar quando a unidade atual tiver: (a) revisão concluída; (b) execução autorizada e realizada, se aplicável; (c) resultado observado; e (d) evidência/estado registrado. Uma falha interrompe o avanço. Não repetir etapas já homologadas de snapshot, CDC incremental ou evolução real do schema.

## Automação das verificações operacionais

O script `operations/verify-metrics.sh` foi preparado para coletar a métrica `io.confluent.kafka.server/received_bytes` pela Metrics API, filtrada pelo cluster configurado, e salvar a resposta original e um resumo datado em `evidence/observability/`. A operação é somente leitura e reutiliza as credenciais locais existentes. Consulte `operations/cost-and-metrics.md`.

**Estado:** verificador sincronizado, validado sintaticamente e executado pelo usuário em 14/11/2026; evidências versionadas em `evidence/observability/20261114T100354Z-metrics.{json,md}`. A coleta automatizada está validada nesta execução. ACLs e role binding MetricsViewer foram verificados e aprovados; o verificador de Billing está preparado, aguardando validação local e execução; confiabilidade permanece pendente. O orquestrador `operations/verify-observability.sh` será criado após os verificadores individuais. Não executar qualquer rotina que altere permissões ou recursos.

## 13. Observabilidade — coleta automatizada de métricas (14/11/2026)

O verificador `operations/verify-metrics.sh` foi sincronizado pelo usuário, passou em `bash -n` e foi executado com sucesso. A execução consultou somente leitura a Confluent Metrics API e gerou:

- JSON original: `evidence/observability/20261114T100354Z-metrics.json`.
- Resumo: `evidence/observability/20261114T100354Z-metrics.md`.

A resposta contém dois pontos para `io.confluent.kafka.server/received_bytes`, cluster `lkc-rrwvjjk`, janela solicitada `PT6H/now` e granularidade `PT1H`: 133.168 bytes às 05:00 UTC (≈130,05 KiB) e 1.250 bytes às 06:00 UTC (≈1,22 KiB). A métrica é um contador em bytes agregado na granularidade; representa tráfego recebido pelo cluster, sem identificar isoladamente o produtor. A ausência de outros pontos não significa tráfego zero nem comprova cobertura contínua da janela.

**Classificação:** coleta automatizada executada e evidência observada. O funcionamento deste verificador está validado para esta execução; o Passo 8 de observabilidade permanece parcial: ACLs/RBAC foram aprovados; Billing está preparado, mas aguarda execução; confiabilidade ainda precisa de verificação própria. Nenhuma permissão ou recurso Cloud foi alterado.


## 14. Observabilidade — preparação do verificador de Billing

Foi preparado `operations/verify-billing.sh`, que consulta a Costs API por meio da Confluent CLI em modo JSON, sem alterar recursos ou permissões. A consulta exige sessão autenticada na CLI e papel `OrganizationAdmin` ou `BillingAdmin`.

Uso padrão: `bash operations/verify-billing.sh`. O padrão consulta o mês UTC corrente até três dias antes da data atual para reduzir a defasagem de até 72 horas. Também aceita duas datas explícitas: `./operations/verify-billing.sh YYYY-MM-DD YYYY-MM-DD`, com início inclusivo e fim exclusivo, limitado a um mês.

O script foi corrigido para normalizar os valores monetários que a CLI retorna como strings com prefixo `# Plano de homologação

Este plano organiza a validação do laboratório a partir dos scripts e SQL versionados. Ele é um roteiro de execução, não evidência de que o ambiente foi provisionado ou de que os testes passaram.

> **Estado inicial:** trate cada reconstrução como ambiente novo. Confirme no console o que existe, a região e os créditos disponíveis. Não reutilize IDs, credenciais ou arquivos de estado de ambientes anteriores sem verificação.

## 1. Pré-verificação e preparação

Antes de executar qualquer script:

1. Confirme conta, região, créditos e limites de custo na Confluent Cloud.
2. Confirme que o PostgreSQL é dedicado ao laboratório, acessível pelo conector e aceita logical replication.
3. Prepare `.env` a partir de `.env.example`, mantendo credenciais somente localmente.
4. Crie/valide a role PostgreSQL dedicada com `LOGIN` e `REPLICATION`; `infrastructure/connectors/cdc/connector.sh` verifica esses atributos, mas não cria a role.
5. Revise os nomes de Environment, cluster, compute pool e os contextos selecionados na CLI.
6. Não execute a inicialização do banco sem confirmar host, database e o efeito de recriar o schema `payments`.

A inicialização do banco é destrutiva para o schema `payments` existente e exige confirmação explícita:

```bash
CONFIRM_RESET=yes ./scripts/run-sql.sh database/init/01-database.sql
```

Use somente em banco dedicado e após conferir a conexão.

## 2. Fundação Confluent

**Ação:** `./infrastructure/foundation.sh`

O script pode criar Environment, cluster Kafka Basic, service accounts, API keys e ACLs. Também seleciona o contexto da CLI, aguarda o cluster ficar `UP` e grava o estado local e a evidência.

**Evidência esperada:** `evidence/cloud/01-foundation.txt`.

**Critério:** confirmar IDs/nome do Environment e cluster, estado `UP`, bootstrap e ACLs esperadas. A conclusão do script não dispensa a leitura da evidência e a conferência do contexto.

## 3. Schema Registry e contrato Avro

**Ação:** `./infrastructure/schema-registry.sh`

O script configura compatibilidade `BACKWARD` e testa um subject de contrato isolado, `contract.transactions-value`: evolução com campo opcional/default aceita e campo obrigatório sem default rejeitado.

**Evidência esperada:** `evidence/cloud/02-schema-registry.txt`.

**Critério:** conferir os dois resultados de compatibilidade e as versões registradas. Esse teste de contrato não comprova, por si só, evolução do subject real produzido pelo conector CDC.

## 4. PostgreSQL CDC

Após preparar banco, tabelas, publication e role, execute:

```bash
./infrastructure/connectors/cdc/connector.sh
```

O script verifica a role, impede reutilização automática de um slot `payments_slot` existente, renderiza a configuração localmente e cria o conector `payments-postgres-cdc`.

**Evidência esperada:** `evidence/cloud/03-cdc-connector.txt`.

**Critério:** conector em `RUNNING` e identificação dos quatro tópicos: `payments.customers`, `payments.accounts`, `payments.cards` e `payments.transactions`. `RUNNING` não comprova que o snapshot terminou.

## 5. Snapshot inicial

Execute:

```bash
./operations/verify-cdc-snapshot.sh
```

O verificador consome os quatro tópicos desde o início e contabiliza eventos Debezium de snapshot (`op=r`).

**Evidência esperada:** diretório datado em `evidence/homologation/`, com logs por tópico e `summary.txt`.

**Contagens esperadas:**

| Tópico | Eventos de snapshot |
|---|---:|
| `payments.customers` | 200 |
| `payments.accounts` | 240 |
| `payments.cards` | 320 |
| `payments.transactions` | 2.000 |

**Critério:** cada contagem observada deve corresponder à esperada. Na execução homologada de 13/11/2026, os quatro tópicos atingiram 2.760/2.760 eventos `op=r` com timeout de 2.400 segundos; resultado `target-reached`. Snapshot: **homologado**.

## 6. CDC incremental

Após o snapshot inicial completo e antes de iniciar o Flink, valide alterações em tempo real:

```bash
./operations/verify-cdc-incremental.sh
```

O script usa um consumer group exclusivo `snapshot-check-*` (ACL prefixada provisionada pela fundação), sem `--from-beginning`, cria um registro temporário, executa INSERT/UPDATE/DELETE e procura os eventos `c`, `u` e `d` associados ao marcador. A evidência deve preservar as mensagens Kafka e os offsets.

**Evidência esperada:** arquivo datado `*-cdc-incremental.txt` em `evidence/homologation/`.

**Critério:** correlacionar a linha de origem com os três eventos Debezium e confirmar `snapshot=false`. O teste deve ocorrer antes do Flink, como no Passo 5 da demonstração do mentor.

**Estado em 14/11/2026:** verificador oficial executado com sucesso. INSERT (`op=c`, offset 231), UPDATE (`op=u`, offset 232) e DELETE (`op=d`, offset 233) foram correlacionados ao mesmo marcador na partição 0, todos com `snapshot=false`. Evidência local: `evidence/homologation/20261114T005129Z-cdc-incremental.txt`. O resultado confirma a captura incremental do Debezium no Kafka nesta execução. O teste temporário anterior (offsets 223–225) permanece como evidência complementar, não como substituto da homologação pelo verificador oficial.

## 7. Evolução adicional do schema real do CDC (extensão do projeto)

Depois do teste incremental e antes de criar statements Flink, valide a evolução do subject efetivamente usado pelo conector:

```bash
CONFIRM_SCHEMA_EVOLUTION=yes ./infrastructure/schema-evolution.sh
```

Esta é uma extensão do projeto, não o Passo 6 da demonstração do mentor. O Passo 6 testa compatibilidade em um subject Avro isolado (`contract.transactions-value`), por meio de `infrastructure/schema-registry.sh`, atualmente executado na etapa 3 deste plano. Esta etapa adicional altera a tabela `payments.transactions`, adiciona a coluna `channel` e insere um registro para provocar nova versão no subject real `payments.transactions-value`. Exige confirmação explícita e deve ser executada somente após conferir o banco e o subject.

**Evidência esperada:** arquivo datado `evidence/homologation/<timestamp>-real-schema-evolution.txt`, gerado pelo próprio script.

**Critério:** comprovar a publicação do novo schema pelo CDC e a captura do evento no tópico. Não corresponde ao Passo 6 da demonstração; é uma validação adicional do subject real.

**Estado em 14/11/2026:** **homologado**. Execução de `infrastructure/schema-evolution.sh` concluída com sucesso. O subject `payments.transactions-value` evoluiu da versão 1 para a 2; a compatibilidade efetiva permaneceu `BACKWARD` e `is_compatible=true`; o schema novo contém `channel`; o conector `payments-postgres-cdc` permaneceu `RUNNING`; e o evento correlacionado `schema-evolution-20261114T012247Z-27263` foi observado no tópico `payments.transactions`, partição 0, offset 2000, com `op=c` e `snapshot=false`. Evidência versionada: `evidence/homologation/20261114T012247Z-real-schema-evolution.txt` (commit `22182874a9f3059e18c038ad4e778ee9fca3cac7`).
## 8. Flink SQL

Com snapshot, CDC incremental e evolução do schema real homologados por evidências da instância atual, a próxima etapa é revisar e, mediante autorização explícita, executar:

```bash
./flink/scripts/run.sh
```

O script seleciona o contexto Flink e reutiliza o compute pool configurado. Na primeira execução, se o pool de destino ainda não existir, reconhece somente o pool legado previamente identificado (`lfcp-2268w8o` / `default.env-mn3vgw.us-east-2`), valida ID, nome, environment, cloud, região, status e capacidade, e o renomeia para o nome configurado preservando o `max_cfu`. Não cria compute pools automaticamente; em caso de ausência, duplicidade ou incompatibilidade, interrompe. Depois, submete em sequência as PKs de `accounts` e `transactions`, enrichment, append e detecção. A propriedade `sql.tables.scan.idle-timeout` é aplicada às consultas contínuas de enrichment e detecção por `--property` da CLI, com padrão de `5 s`; isso reproduz o efeito do `SET` do mentor em cada statement criado separadamente. Não há statement separado de `SET`. Interrompe se um statement com o mesmo nome já existir; não sobrescreve automaticamente.

**Evidência registrada:** `evidence/homologation/20261114T052012Z-flink-fraud-detection-partial.md` (execução parcial de 14/11/2026).

**Estado em 14/11/2026 — execução parcial observada:** o pool existente `lfcp-2268w8o` (`desafio-pool`) foi validado pelo usuário. As declarações de PK de accounts e transactions foram observadas como `COMPLETED`; `payments-enrichment-test` permaneceu `RUNNING`; `payments-append-only-test` concluiu com `COMPLETED` e `Command completed successfully.`. O statement `payments-fraud-detection-test` foi aceito às 05:20:12 UTC e observado `RUNNING`. O catálogo SQL listou `fraud_alerts`; seu DDL expôs `$rowtime` como `ROWTIME` com `SOURCE_WATERMARK()`. Consulta resumida observou 641 linhas e `txn_count=3` em todas (mínimo e máximo iguais a 3). Evidência: `evidence/homologation/20261114T052012Z-flink-fraud-detection-partial.md`.

**Limites:** a consulta resumida gerou `AGGREGATION_WITHOUT_KEY`; foi exploratória e não deve permanecer ativa. Não houve correlação dos alertas com transações de origem, confirmação do cenário que os gerou, validação isolada da janela de 60 segundos/watermark nem confirmação de mensagens no tópico Kafka de saída. Portanto, o Passo 7 está parcialmente executado/observado, mas a detecção de fraude e a pipeline ponta a ponta ainda não estão homologadas.

**Critério mínimo:** verificar os estados esperados registrados pelo script:
- declarações de PK de `accounts` e `transactions`: `COMPLETED`;
- enrichment e detecção: `RUNNING`;
- append: `COMPLETED`.

Esses estados confirmam o estado dos statements, não a correção funcional do Temporal Join nem a emissão de alertas. A validação funcional exige observar dados enriquecidos e mensagens de saída.



**Atualização — preflight global de statements (14/11/2026):** depois de resolver e validar o compute pool, mas antes de submeter qualquer statement, o orquestrador consulta a lista do pool e verifica os cinco nomes planejados (payments-pk-accounts, payments-pk-transactions, payments-enrichment, payments-append-only, payments-fraud-detection). Se qualquer nome já existir, interrompe antes da primeira submissão; não exclui nem sobrescreve statements. A verificação individual em submit() permanece como proteção adicional. Alteração publicada no commit 4dfe1228f91f4cbd383c041cdbfcba9dabe3a58f. Ainda depende de sincronização e bash -n local. Nenhuma operação Cloud foi executada.

## 9. Cenário positivo de fraude — sequência do mentor

Somente após confirmar que o snapshot CDC terminou e que os statements de enrichment e detecção estão em `RUNNING`, execute:

```bash
./scripts/run-sql.sh database/seed/simulate-events.sql
```

O arquivo executa quatro INSERTs independentes, nesta ordem:
1. Três transações positivas de R$ 100,00 para o cartão 1 (conta 1, merchant 1).
2. Uma quarta transação de R$ 10,00 para outro cartão (cartão 320, conta 80, merchant 1).
3. Aguarda 90 segundos (`pg_sleep(90)`) como janela de observação indicada na demonstração.
4. Consulta as linhas inseridas; cada INSERT também retorna seu `transaction_id`.

Os IDs de cartão/conta/merchant foram conferidos no PostgreSQL do ambiente atual antes da preparação do cenário. O arquivo executa as instruções em sequência e o wrapper registra a saída em `evidence/homologation/`.

**Importante:** o SQL Flink atual usa `$rowtime`, não `created_at`, e define `T` apenas como `amount > 0`. A quarta transação, por usar outro `card_id`, não pertence à mesma partição do padrão dos três primeiros eventos. Seu possível efeito sobre a watermark precisa ser observado; os 90 segundos são uma janela de espera, não garantia de emissão.

**Critério:** após a execução, observar no tópico `fraud_alerts` um alerta novo correlacionável ao cartão 1, com `txn_count = 3`, e registrar a mensagem/offset/timestamps como evidência. A execução dos INSERTs, a espera ou a consulta ao PostgreSQL não comprovam, isoladamente, a emissão do alerta.

**Execução observada em 14/11/2026:** `database/seed/simulate-events.sql` foi executado com sucesso. Os INSERTs produziram `transaction_id` 2002, 2003 e 2004 para `card_id=1` (R$ 100,00 cada) e `transaction_id=2005` para `card_id=320` (R$ 10,00), todos às 06:11:29 UTC. Evidência: `evidence/homologation/20261114T061128Z-simulate-events.log`.

A consulta `SELECT * FROM fraud_alerts` foi executada às 06:52:41 UTC e retornou resultados, incluindo `card_id=1`, `txn_count=3`, com `first_txn=05:03:20.542` e `last_txn=05:03:22.758` UTC. Esses timestamps são anteriores aos INSERTs do cenário positivo (06:11:29 UTC); portanto, os resultados não podem ser atribuídos a esses quatro registros com a evidência disponível. Evidência: `evidence/homologation/20261114T065241Z-fraud-detected-query.txt`. **Classificação:** cenário positivo executado; consulta observada; correlação e homologação funcional pendentes. Não afirmar que a regra detectou o cenário positivo.

**Atualização da investigação — 14/11/2026:** consulta read-only a `enriched_payments` confirmou as transações 2002, 2003 e 2004 para `card_id=1`, mas seus `$rowtime` foram, respectivamente, `06:11:30.774`, `06:11:30.775` e `06:16:44.383` UTC. O intervalo entre a primeira e a terceira foi de 5 min 13,609 s, superior aos 60 segundos exigidos pelo padrão. A consulta a `fraud_alerts` retornou três registros históricos para `card_id=1`, todos com `txn_count=3` e timestamps entre `05:03:20.541` e `05:03:22.758` UTC; nenhum corresponde ao cenário das transações 2002–2004. **Pendência documentada:** ainda falta executar e comprovar um cenário positivo controlado em que pelo menos três eventos positivos do mesmo cartão cheguem ao Kafka em até 60 segundos e sejam correlacionados a um alerta novo (incluindo a evidência da saída/offset). Essa correção e nova homologação foram deliberadamente adiadas para depois da conclusão da observabilidade e do teardown. Até lá, não declarar a detecção funcional de fraude homologada.

**Validação e encerramento automatizados:** após executar o cenário positivo, informe ao verificador a janela UTC esperada dos timestamps Flink `$rowtime` (formato `YYYY-MM-DD HH:MM:SS.mmm`):

```bash
./operations/verify-fraud-alerts.sh '2026-11-14 06:11:00.000' '2026-11-14 06:20:00.000'
```

O exemplo de horário é ilustrativo; use os limites do cenário da execução atual. O verificador consulta somente os alertas do cartão 1, limita a leitura aos 100 registros mais recentes, exige `txn_count >= 3` e confere se `first_txn` e `last_txn` estão dentro da janela informada. A consulta é pontual e limitada; não substitui a leitura direta do tópico Kafka. Se não houver correspondência, o script reprova e orienta preservar os statements para diagnóstico.

Somente quando a evidência terminar com `VALIDAÇÃO APROVADA`, execute o encerramento passando o caminho exato do arquivo gerado:

```bash
./flink/scripts/stop-after-validation.sh evidence/homologation/<timestamp>-fraud-alerts-validation.txt
```

O encerrador exige evidência aprovada recente (até duas horas), identifica sem ambiguidade os statements de enrichment e fraude no compute pool configurado, solicita a parada e consulta novamente cada estado até confirmar `STOPPED`. Reconhece os nomes atuais e os aliases históricos terminados em `-test`; se encontrar ausência, duplicidade ou estado inesperado, aborta sem iniciar as paradas. A operação não exclui os statements. Se a validação falhar, não execute o encerrador.


**Estado operacional às 07:16:44 UTC:** cluster Kafka `UP`, conector `payments-postgres-cdc` `RUNNING`, compute pool `desafio-pool` `PROVISIONED` com `Current CFU=2`, `Max CFU=50`; evidência `evidence/cloud/20261114T071644Z-operational-state.txt`. A coleta registra inventário operacional e estados de statements; não comprova a correlação funcional nem a publicação do alerta no tópico Kafka.

## 10. Cenário negativo opcional

O arquivo `database/seed/slow-pattern.sql` insere três transações positivas para o cartão 4, com intervalos de aproximadamente 35 segundos entre elas; o primeiro e o terceiro eventos ficam separados por mais de 60 segundos.

Execute somente com os statements Flink ativos:

```bash
./scripts/run-sql.sh database/seed/slow-pattern.sql
```

**Critério:** observar o comportamento do tópico `fraud_alerts` e registrar a janela temporal e o resultado. Não declarar ausência de alerta sem observação suficiente do processamento/watermark. O arquivo prepara o cenário; não realiza sozinho a verificação do resultado.

## 11. Verificações complementares opcionais

A validação CDC incremental (Passo 5 do mentor) foi executada e confirmada em 14/11/2026: INSERT (`op=c`, offset 231), UPDATE (`op=u`, offset 232) e DELETE (`op=d`, offset 233), todos com `snapshot=false`, correlacionados pelo mesmo marcador na partição 0. Evidência: `evidence/homologation/20261114T005129Z-cdc-incremental.txt`. A evolução adicional do subject CDC real também foi homologada em 14/11/2026: subject da versão 1 para 2, `BACKWARD`, campo `channel` e evento `op=c` observado no tópico, partição 0, offset 2000. Evidência: `evidence/homologation/20261114T012247Z-real-schema-evolution.txt`. O teste isolado de contrato Avro (Passo 6 do mentor) é distinto e executado pelo script da etapa 3.

## 12. Registro, custos e encerramento

- Preserve logs e resultados em `evidence/`; registre data, ambiente, escopo e resultado observado.
- Classifique cada etapa como aprovada, falhou, parcial, não executada ou bloqueada.
- Não inclua credenciais, API secrets, connection strings ou arquivos renderizados nas evidências.
- Consulte `operations/cost-and-metrics.md` antes, durante e após o laboratório.
- Não deixe cluster, compute pool ou connector ativos sem necessidade.
- Teardown é uma etapa separada: confira dependências e escopo destrutivo antes de qualquer limpeza. Não execute scripts de remoção automaticamente.

## Limites da homologação

- A janela de fraude usa `$rowtime` Kafka, não `created_at`.
- `NOT ENFORCED` declara metadados de chave no Flink; não verifica unicidade na origem.
- O changelog append não filtra exclusivamente INSERTs: eventos CDC de criação, snapshot e atualização podem ser tratados como novas linhas, enquanto exclusões são ignoradas, conforme o SQL versionado.
- `MATCH_RECOGNIZE` depende da semântica temporal/watermark do Flink; a janela de 60 segundos não deve ser descrita como garantia de emissão imediata em relógio de parede.
- Nenhum resultado histórico de ambientes já excluídos comprova homologação no ambiente atual.


## Ordem didática e operacional

A sequência conceitual da demonstração é: Passo 5 (CDC incremental: INSERT/UPDATE/DELETE), Passo 6 (compatibilidade de contrato Avro em subject isolado) e Passo 7 (Flink: Temporal Join, append e MATCH_RECOGNIZE). Neste projeto, o teste isolado do Passo 6 é executado na etapa 3, antes do CDC, porque `schema-registry.sh` combina provisionamento e teste de contrato. A evolução do subject CDC real, executada após o CDC incremental e antes do Flink, é uma extensão adicional e não deve ser chamada de Passo 6.

## Método de trabalho para a etapa Flink: um arquivo por vez

A etapa Flink será conduzida pelo mesmo método incremental já utilizado no projeto privado de estudos. **Não executar a sequência inteira de uma vez antes de revisar e compreender cada unidade.**

Para cada script ou arquivo SQL, seguir este ciclo:

1. **Localizar e ler:** consultar o arquivo ativo no GitHub e os documentos técnicos relacionados. Consultar também o arquivo equivalente no projeto privado de estudos, quando existir.
2. **Comparar:** identificar diferenças entre a versão ativa e a versão de estudos; preservar o comportamento já exercitado e as decisões do projeto atual. Não copiar nem reescrever automaticamente.
3. **Analisar:** explicar o objetivo, pré-requisitos, efeitos, riscos, dependências e critérios de sucesso. Separar claramente a transcrição do mentor das adaptações do projeto.
4. **Propor correção mínima:** se houver problema comprovado, apresentar o diff e a justificativa antes de alterar. Não refatorar por preferência.
5. **Validar localmente:** após a sincronização pelo usuário, executar as verificações estáticas pertinentes (por exemplo, `bash -n` para Bash). Sintaxe válida não significa execução operacional válida.
6. **Executar uma unidade:** somente após revisão e autorização explícita, o usuário executa o script/comando daquela unidade. Não iniciar a próxima unidade na mesma tacada.
7. **Inspecionar:** analisar a saída, consultar o estado efetivo do recurso/statement e conferir a evidência produzida. Em caso de falha, interromper a sequência e diagnosticar antes de tentar novamente.
8. **Registrar:** classificar como preparado, validado estaticamente, executado, observado, homologado, falhou ou bloqueado, conforme a evidência disponível. Só então avançar.

### Aplicação ao Passo 7

A unidade inicial é a revisão do orquestrador `flink/scripts/run.sh` e dos SQLs que ele submete. A revisão deve identificar dependências e ordem de submissão, mas não implica executar o orquestrador.

Os arquivos SQL devem ser tratados individualmente, na ordem de dependência definida após a revisão. A existência de um statement ou recurso na Confluent Cloud deve ser consultada antes de qualquer criação; não presumir que o ambiente está vazio ou que um comando pode ser repetido com segurança.

O script `flink/scripts/run.sh` pode renomear o pool legado previamente identificado e submeter vários statements em sequência; não cria compute pools. Portanto, **permanece fora de execução até que seus efeitos sejam compreendidos e que cada unidade relevante tenha sido revisada**. Nenhuma operação Cloud/Neon está autorizada por este registro.

### Critério para avançar

Só avançar quando a unidade atual tiver: (a) revisão concluída; (b) execução autorizada e realizada, se aplicável; (c) resultado observado; e (d) evidência/estado registrado. Uma falha interrompe o avanço. Não repetir etapas já homologadas de snapshot, CDC incremental ou evolução real do schema.

## Automação das verificações operacionais

O script `operations/verify-metrics.sh` foi preparado para coletar a métrica `io.confluent.kafka.server/received_bytes` pela Metrics API, filtrada pelo cluster configurado, e salvar a resposta original e um resumo datado em `evidence/observability/`. A operação é somente leitura e reutiliza as credenciais locais existentes. Consulte `operations/cost-and-metrics.md`.

**Estado:** verificador sincronizado, validado sintaticamente e executado pelo usuário em 14/11/2026; evidências versionadas em `evidence/observability/20261114T100354Z-metrics.{json,md}`. A coleta automatizada está validada nesta execução. ACLs e role binding MetricsViewer foram verificados e aprovados; o verificador de Billing está preparado, aguardando validação local e execução; confiabilidade permanece pendente. O orquestrador `operations/verify-observability.sh` será criado após os verificadores individuais. Não executar qualquer rotina que altere permissões ou recursos.

## 13. Observabilidade — coleta automatizada de métricas (14/11/2026)

O verificador `operations/verify-metrics.sh` foi sincronizado pelo usuário, passou em `bash -n` e foi executado com sucesso. A execução consultou somente leitura a Confluent Metrics API e gerou:

- JSON original: `evidence/observability/20261114T100354Z-metrics.json`.
- Resumo: `evidence/observability/20261114T100354Z-metrics.md`.

A resposta contém dois pontos para `io.confluent.kafka.server/received_bytes`, cluster `lkc-rrwvjjk`, janela solicitada `PT6H/now` e granularidade `PT1H`: 133.168 bytes às 05:00 UTC (≈130,05 KiB) e 1.250 bytes às 06:00 UTC (≈1,22 KiB). A métrica é um contador em bytes agregado na granularidade; representa tráfego recebido pelo cluster, sem identificar isoladamente o produtor. A ausência de outros pontos não significa tráfego zero nem comprova cobertura contínua da janela.

**Classificação:** coleta automatizada executada e evidência observada. O funcionamento deste verificador está validado para esta execução; o Passo 8 de observabilidade permanece parcial: ACLs/RBAC foram aprovados; Billing está preparado, mas aguarda execução; confiabilidade ainda precisa de verificação própria. Nenhuma permissão ou recurso Cloud foi alterado.


## 14. Observabilidade — preparação do verificador de Billing

Foi preparado `operations/verify-billing.sh`, que consulta a Costs API por meio da Confluent CLI em modo JSON, sem alterar recursos ou permissões. A consulta exige sessão autenticada na CLI e papel `OrganizationAdmin` ou `BillingAdmin`.

Uso padrão: `bash operations/verify-billing.sh`. O padrão consulta o mês UTC corrente até três dias antes da data atual para reduzir a defasagem de até 72 horas. Também aceita duas datas explícitas: `./operations/verify-billing.sh YYYY-MM-DD YYYY-MM-DD`, com início inclusivo e fim exclusivo, limitado a um mês.

A normalização aceita valores monetários JSON numéricos, strings decimais e strings com prefixo `$<decimal>`, validando estritamente o formato antes da conversão. `amount` é obrigatório; `original_amount` e `discount_amount` nulos/ausentes não são convertidos silenciosamente em zero. Se qualquer item não trouxer `discount_amount`, o resumo não apresenta uma soma global incompleta. O JSON original é preservado.

**Correção do formato de campos da CLI (14/11/2026):** a execução real retornou 12 itens: em 4, `resource` e `environment` eram `null`; em 8, ambos eram strings. O parser anterior tentava acessar `.id` diretamente e falhou durante a renderização. O script foi corrigido para verificar o tipo antes de acessar propriedades, usando strings diretamente e deixando campos nulos vazios. O bloco jq foi testado isoladamente com jq 1.7 em amostras sintéticas com campos nulos, strings e objetos; os quatro formatos foram renderizados sem erro. Esse teste não equivale a `bash -n` no clone do usuário nem a nova execução contra a conta. O JSON bruto da consulta anterior foi preservado pelo script antes da falha, conforme a saída observada; o resumo Markdown não foi concluído.

**Estado após correção:** script atualizado no commit `3a15aa0334be0fc6c9f397883f0118e2c0b8d2d4`; ainda depende de sincronização local, `bash -n operations/verify-billing.sh` e nova execução pelo usuário para validar a geração do resumo. Não repetir a consulta por iniciativa própria.

A Costs API agrega valores diariamente e pode atrasar até 72 horas. O resumo não representa necessariamente os encargos correntes do Console e não informa o saldo restante dos créditos promocionais. A evidência deve ser classificada como coleta automatizada de custos agregados, sem confundir com a Invoice Breakdown do Console. Referências: [Retrieve Invoices and Costs](https://docs.confluent.io/cloud/current/billing/invoices-and-costs.html), [CLI billing cost list](https://docs.confluent.io/confluent-cli/current/command-reference/billing/cost/confluent_billing_cost_list.html) e [Legacy Billing](https://docs.confluent.io/cloud/current/billing/legacy-billing.html).

A consulta real anterior foi somente leitura e terminou com falha na renderização do resumo. Nenhuma operação Cloud foi executada para aplicar a correção do parser. A etapa de Billing permanece pendente até a nova execução e revisão da evidência.


### Execução do verificador de Billing (14/11/2026)

O usuário executou a consulta para 2026-11-01 a 2026-11-11 (fim exclusivo). A Costs API retornou 12 itens e foram gerados os arquivos `20261114T112054Z-billing.json` e `20261114T112054Z-billing.md` em `evidence/observability/`. O resumo apresentou total original e líquido de USD `-0.00009999999999976694`; a soma global de descontos não foi consolidada porque 4 de 12 itens tinham `discount_amount` nulo/ausente. Foram observados lançamentos CONNECT, FLINK, KAFKA e PROMO_CREDIT em dois IDs de ambiente distintos. A evidência ainda precisa ser revisada e commitada pelo usuário.

A execução também revelou uma falha de quoting na linha explicativa do Markdown: Bash tentou executar `discount_amount` como comando, gerando a mensagem `discount_amount: comando não encontrado` e omitindo o nome do campo naquela frase. Os dados da tabela e totais foram gerados; a frase do relatório deve ser corrigida antes do commit. O script foi corrigido no commit `e65d9415bcfe67501515ce55186010be2302de9f`, escapando os backticks. Ainda não foi validado localmente com `bash -n`. A etapa de Billing tem coleta executada e evidência local gerada, mas o registro versionado permanece pendente.
