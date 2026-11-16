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

Antes de submeter statements, crie ou valide o compute pool configurado:

```bash
bash infrastructure/flink-compute-pool.sh
```

O script carrega as configurações locais, valida o Environment e a identidade do pool, cria o recurso quando necessário (ou renomeia somente o pool legado previamente identificado e validado), aguarda `PROVISIONED`/`RUNNING`, salva `FLINK_POOL_ID` em `.state.env` e grava `evidence/cloud/04-flink-compute-pool.txt`. Não cria um segundo pool quando o pool de destino já existe e interrompe diante de divergências ou ambiguidades.

Com snapshot, CDC incremental e evolução do schema real homologados por evidências da instância atual e compute pool disponível, a próxima etapa é revisar e, mediante autorização explícita, executar:

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


## 15. PostgreSQL Sink — persistência dos alertas

**Estado atualizado em 14/11/2026:** configuração, DDL, script de criação, auditoria read-only e teardown estão preparados. Conforme resultados de Schema Registry e PostgreSQL apresentados pelo usuário, a compatibilidade estrutural do contrato Avro efetivo de `fraud_alerts` com `public.fraud_alerts` foi confirmada. O conector ainda não foi criado, executado ou homologado. A confirmação estrutural não comprova conectividade independente, privilégios efetivos suficientes nem conversão temporal em execução.

### Inspeção da saída Kafka

Antes de criar o Sink, execute:

```bash
bash operations/preflight-fraud-alerts-sink.sh
```

O script carrega as configurações locais, seleciona Environment e cluster, valida o tópico `fraud_alerts` e inspeciona a primeira mensagem Avro desde o início. O relatório classifica o preflight como **APROVADO** quando a mensagem é recebida e contém `card_id`, `first_txn`, `last_txn` e `txn_count`; **REPROVADO** quando a mensagem não corresponde a esses campos; **INCONCLUSIVO** quando nenhuma mensagem chega dentro do limite; ou **FALHA** quando ocorre erro de leitura. Se nenhuma mensagem chegar, encerra após 7 segundos por padrão; o limite pode ser ajustado por `INSPECT_TIMEOUT_SECONDS`. Ao capturar a primeira mensagem, fecha o consumidor automaticamente e suprime o aviso interativo de `Ctrl+C`. A saída, com partição, offset, conteúdo e resultado, é registrada em `evidence/homologation/`. A inspeção é somente leitura e não cria nem altera recursos Cloud. A aprovação comprova apenas leitura/desserialização e presença dos campos esperados; mensagens antigas não comprovam que o cenário positivo controlado gerou um alerta novo nem que houve persistência no PostgreSQL.

### Contrato e pré-requisitos

- A saída Flink definida em `flink/sql/04-fraud-detection.sql` contém `card_id`, `first_txn`, `last_txn` e `txn_count`.
- **Contrato Avro e tabela — compatibilidade estrutural confirmada em 14/11/2026, conforme resultados apresentados pelo usuário:** `card_id` Avro `int` ↔ PostgreSQL `integer`; `first_txn` e `last_txn` Avro `long` com `logicalType=timestamp-millis` ↔ PostgreSQL `timestamp with time zone`; `txn_count` Avro `long` ↔ PostgreSQL `bigint`. Os quatro campos são obrigatórios no Avro e `NOT NULL` no Neon. O tópico `fraud_alerts` foi reportado com 6 partições e fator de replicação 3. A configuração do Sink usa a chave composta `(card_id, first_txn)`, correspondente à chave prevista no destino. Não repetir a comparação estrutural.
- Confirmar que `public.fraud_alerts` existe com PK `(card_id, first_txn)` e colunas compatíveis. A inicialização recria essa tabela.
- A auditoria read-only de `./scripts/run-sql.sh database/verify/alerts.sql` foi executada em 16/11/2026: a role `alerts_sink` não possui atributos elevados nem memberships; tem CONNECT, USAGE no schema e SELECT/INSERT/UPDATE na tabela, sem CREATE ou DELETE. O teste direto com a credencial da própria role confirmou autenticação e privilégios efetivos de INSERT e UPDATE. Não é necessário repetir essa auditoria entre a inspeção Kafka e a criação do Sink.
- A role criada pela inicialização recebe CONNECT, USAGE em `public` e SELECT/INSERT/UPDATE na tabela. Se a role já existir, a senha é atualizada, mas atributos elevados e memberships preexistentes não são normalizados. Não presumir privilégio mínimo sem auditar o resultado.
- A documentação Confluent lista como pré-requisitos gerais DDL CREATE/ALTER/DROP e DML SELECT/INSERT/UPDATE/DELETE. Como esta configuração desabilita `auto.create` e `auto.evolve`, a suficiência dos privilégios reduzidos concedidos pelo SQL não está comprovada; não ampliar permissões sem teste controlado e justificativa. Referência: https://docs.confluent.io/cloud/current/connectors/cc-postgresql-sink.html.
- A Service Account `app-reader` recebe leitura no tópico `fraud_alerts` e autorização READ no grupo `connect-` por prefixo. A fundação aplica essa ACL de forma idempotente.
- `ssl.mode=require` cifra a conexão PostgreSQL, mas não valida hostname/cadeia do certificado. Não afirmar validação de identidade TLS.

### Configuração preparada

`infrastructure/connectors/sink/connector.json` define `PostgresSink`, formato de entrada Avro, tópico `fraud_alerts`, autenticação Kafka pela Service Account `app-reader`, role PostgreSQL `alerts_sink`, UPSERT com chave composta `(card_id, first_txn)`, tabela pré-criada e `auto.create=false`/`auto.evolve=false`.

A configuração corresponde aos aliases existentes no SQL Flink público. Não foram adicionados campos de saída nem alterada a regra Flink para acomodar o Sink.

### Execução e critérios de homologação

Com a compatibilidade estrutural, a auditoria read-only e o teste direto da role `alerts_sink` já confirmados nesta instância, inspecione o tópico Kafka e siga diretamente para a criação do Sink:

```bash
bash ./infrastructure/connectors/sink/connector.sh
```

O script seleciona Environment e cluster explicitamente, valida o formato JSON da listagem, interrompe se o conector `payments-fraud-alerts-sink` já existir, verifica a existência do tópico, cria o Sink e aguarda até dez minutos por `RUNNING`. Não sobrescreve nem exclui um conector preexistente. O ID observado é salvo em `.state.env` para o teardown.

Depois de produzir um alerta conhecido, execute:

```bash
./scripts/run-sql.sh database/verify/alerts.sql
```

**Homologação exige:** schema Avro compatível; role e privilégios aprovados; conector e task sem falhas; mensagem Kafka identificada por chave/partition/offset; registro correspondente em `public.fraud_alerts`; e reentrega controlada atualizando a mesma chave sem duplicação. Estado `RUNNING` não comprova persistência. A documentação Confluent declara entrega at-least-once para o conector; UPSERT com chave estável pode tornar reentregas idempotentes no destino, mas não prova exactly-once ponta a ponta. Referência: https://docs.confluent.io/cloud/current/connectors/cc-postgresql-sink.html.

### Limpeza

O `teardown/cleanup.sh` valida o nome do Sink pelo ID persistido e remove o conector antes do CDC e do cluster. A limpeza PostgreSQL remove `public.fraud_alerts` e tenta remover as roles configuradas em `PG_CDC_USER` e `PG_SINK_USER` após revogar os privilégios conhecidos do laboratório. Não usa `DROP OWNED CASCADE`: dependências adicionais podem impedir `DROP ROLE`, exigindo investigação antes de qualquer remoção manual.

## 16. Investigação de `$rowtime` e encerramento do debate (14/11/2026)

### Conclusão técnica

A documentação consultada da Confluent apresenta duas orientações que parecem conflitantes: a página de restrições de metadata columns alerta para usos downstream, enquanto o exemplo oficial de `MATCH_RECOGNIZE` utiliza `$rowtime` em `ORDER BY` e nas medidas. A divergência documental não foi resolvida formalmente nesta investigação.

A evidência do próprio ambiente, porém, permite uma conclusão operacional mais limitada e suficiente para decidir o próximo passo:

- O statement `payments-fraud-detection-test` foi aceito em 14/11/2026 às 05:20:12 UTC e observado em `RUNNING`, usando o SQL atual de `flink/sql/04-fraud-detection.sql`.
- Não foi encontrado, nos registros pesquisados, erro de submissão/rejeição que atribua falha ao uso de `$rowtime`.
- O catálogo SQL listou `fraud_alerts`; `DESCRIBE EXTENDED fraud_alerts` mostrou `$rowtime` como coluna `ROWTIME`, com watermark `SOURCE_WATERMARK()`.
- A consulta resumida retornou 641 linhas, todas com `txn_count=3`. Isso não identifica a origem dessas linhas nem prova que o cenário positivo controlado tenha sido detectado.
- O warning `AGGREGATION_WITHOUT_KEY` veio de uma consulta exploratória de agregação global, não do statement de fraude.

**Decisão:** não alterar o SQL atual com base apenas na divergência documental. Não afirmar que o uso de `$rowtime` está formalmente liberado para todo contexto, nem que está proibido em `MATCH_RECOGNIZE`. Para este projeto, a evidência disponível comprova aceitação e execução observada do statement, mas não homologação funcional.

### Resultado do cenário positivo já executado

As transações 2002, 2003 e 2004 do cartão 1 foram encontradas em `enriched_payments`, com os seguintes `$rowtime`:

| Transação | `$rowtime` UTC |
|---:|---|
| 2002 | 06:11:30.774 |
| 2003 | 06:11:30.775 |
| 2004 | 06:16:44.383 |

O intervalo entre o primeiro e o terceiro evento foi de **5 min 13,609 s**, superior à janela configurada de 60 segundos. Os registros consultados em `fraud_alerts` para o cartão 1 tinham timestamps entre 05:03:20.541 e 05:03:22.758 UTC, anteriores ao cenário. Não há correlação comprovada entre esses alertas históricos e as transações 2002–2004.

**Classificação:** cenário positivo executado; enrichment observado para os três registros; alerta correspondente não comprovado; homologação funcional pendente. A consulta de 641 linhas e o estado `RUNNING` não alteram essa classificação.

### Continuidade e limites operacionais

A investigação sobre a proibição de `$rowtime` está encerrada para fins de decisão: não há base para reescrever o SQL, e novas consultas exploratórias sobre o mesmo ponto não são justificadas agora.

A nova homologação positiva foi deliberadamente adiada até depois da conclusão da observabilidade e do teardown, conforme a ordem de trabalho acordada. Não repetir o cenário, não submeter statements e não executar operações de parada, exclusão ou provisionamento neste momento. Não executar Sink nem consumidor Python por inferência: o Sink permanece preparado no repositório, sem execução/homologação, e sua necessidade deve ser decidida separadamente dentro do escopo simplificado.

Ao retomar a validação funcional, será necessário um cenário controlado com pelo menos três eventos positivos do mesmo cartão chegando ao Kafka em até 60 segundos, seguido da correlação com um alerta novo e evidência direta da mensagem/offset no tópico de saída. A consulta ao catálogo ou à tabela materializada, isoladamente, não substitui essa evidência.



## Teardown — encerramento do laboratório

O teardown contém duas operações destrutivas com confirmações independentes: nenhuma confirmação autoriza a outra. A execução, entretanto, segue uma dependência obrigatória: a etapa Neon só pode começar após a listagem da Confluent CLI confirmar que o Environment configurado está ausente. Se essa condição não puder ser verificada, o script bloqueia a limpeza Neon; a ausência de dados/CLI Confluent não libera a etapa seguinte.

### Confluent

1. Confira `ENV_ID` e `CC_ENV_NAME` no ambiente local.
2. O script valida a correspondência exata e solicita confirmação literal `ENV_ID/CC_ENV_NAME`.
3. A exclusão do Environment é a operação principal; remove clusters Kafka, Schema Registry, Connect e Flink Compute Pools associados.
4. Service Accounts só são removidas quando `SA_WRITER_CREATED`, `SA_READER_CREATED` ou `SA_METRICS_CREATED` indicam criação pelo laboratório e após confirmação literal independente `IDENTITIES/ENV_ID`. O vínculo MetricsViewer só é removido quando `METRICS_RBAC_CREATED=yes`.
5. A exclusão de Service Account remove suas API keys associadas. Confira a página de API Keys após a exclusão: chaves Flink regionais devem ser verificadas individualmente. A exclusão de uma Service Account remove as API keys associadas a ela; chaves ligadas a usuários ou outras identidades devem ser avaliadas separadamente.
6. O script verifica o desaparecimento do Environment e das Service Accounts removidas. Falha de consulta não é convertida em zero.

### Neon

1. O projeto Neon é criado/configurado manualmente antes do laboratório. O teardown não exclui projeto, branch, compute, endpoint ou banco.
2. Confira `NEON_PROJECT_ID`, `NEON_BRANCH`, `PG_HOST`, `PG_DATABASE`, `PG_CDC_USER` e `PG_SINK_USER`.
3. O script exibe o alvo e exige confirmação literal `NEON_PROJECT_ID/NEON_BRANCH/PG_DATABASE`.
4. O SQL remove `payments_slot`, `payments_pub`, schema `payments`, tabela `public.fraud_alerts` e as roles indicadas em `PG_CDC_USER` e `PG_SINK_USER` (padrões `cdc` e `alerts_sink`).
5. O SQL revoga privilégios conhecidos e executa `DROP ROLE` sem `DROP OWNED CASCADE`. Dependências adicionais fazem o PostgreSQL interromper a remoção da role; investigue antes de qualquer ação manual.
6. O banco e o projeto permanecem existentes. Código de saída diferente de zero significa que a limpeza SQL não foi concluída integralmente.

Confirmar a exclusão do Environment não confirma a limpeza Neon, que exige sua própria confirmação literal. A etapa Neon só é liberada quando a ausência do Environment é confirmada por listagem, antes ou depois da solicitação de exclusão. Se a exclusão for recusada, o Environment continuar listado, a CLI/dados Confluent estiverem indisponíveis ou a verificação falhar, a limpeza Neon é bloqueada; corrija a situação e execute novamente o teardown. Não execute em projeto ou banco compartilhado.


## Revisão estática adicional do teardown — 15/11/2026

A rotina de remoção de identidades foi reforçada para propagar explicitamente falhas de listagem, exclusão e verificação das Service Accounts. O vínculo `MetricsViewer`, quando marcado como criado pelo laboratório, também é consultado antes e depois da exclusão; sua ausência só é registrada após nova listagem. O relatório registra separadamente o estado das identidades próprias e sinaliza que uma falha pode ter deixado remoção parcial. A confirmação literal `IDENTITIES/ENV_ID` permanece independente da confirmação do Environment.

**Validação:** o usuário sincronizou a `main` até `c2d424a` e informou que `bash -n teardown/cleanup.sh` e `bash -n scripts/run-sql.sh` terminaram sem erros. Isso valida apenas sintaxe Bash; não foram executados comandos Confluent, SQL, Neon ou operações Cloud.


## Registro operacional — Neon após limpeza (15/11/2026)

O operador confirmou a remoção manual das roles auxiliares `cdc` e `alerts_sink`. A role administrativa/owner pertencente ao projeto Neon (neste ambiente, `payments_owner`) foi preservada por fazer parte do ciclo de vida do projeto. Permanecem preservados o projeto, branch, compute, endpoint e database `payments`. Não executar novamente o teardown Neon nem tentar excluir isoladamente o database para remover a role do projeto.

As variáveis iniciais de identificação do Neon serão mantidas nos arquivos de referência do projeto. Os arquivos locais `.env` e `.state.env` serão saneados pelo operador para retirar credenciais e estado operacional que não serão usados no teste final; não versionar nem compartilhar seu conteúdo.
