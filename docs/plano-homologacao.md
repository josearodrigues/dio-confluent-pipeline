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

O script seleciona o contexto Flink e reutiliza o compute pool configurado. Na primeira execução, se o pool de destino ainda não existir, reconhece somente o pool legado previamente identificado (`lfcp-2268w8o` / `default.env-mn3vgw.us-east-2`), valida ID, nome, environment, cloud, região, status e capacidade, e o renomeia para o nome configurado preservando o `max_cfu`. Não cria compute pools automaticamente; em caso de ausência, duplicidade ou incompatibilidade, interrompe. Depois, submete em sequência os statements de startup offset, PKs, enrichment, append e detecção. Interrompe se um statement com o mesmo nome já existir; não sobrescreve automaticamente.

**Evidência esperada:** `evidence/cloud/04-flink.txt`.

**Estado em 13/11/2026:** revisão estática do script e dos SQLs concluída; `bash -n` aprovado sobre o conteúdo remoto final. Nenhum compute pool ou statement Flink foi criado/executado nesta instância. A execução permanece pendente de autorização.

**Atualização em 14/11/2026:** o orquestrador foi ajustado para reutilizar o pool existente e, somente na primeira execução, renomear o pool legado identificado após validar identidade, environment, cloud, região, status e `max_cfu`. A capacidade atual é preservada explicitamente. O script não cria pools automaticamente e interrompe em situações ambíguas. Alteração estática publicada; ainda depende de `git pull`, revisão do diff e `bash -n` local. Nenhuma operação Cloud foi executada; renomeação e submissão de statements continuam pendentes de autorização.

**Critério mínimo:** verificar os estados esperados registrados pelo script:
- startup offset e declarações de PK: `COMPLETED`;
- enrichment e detecção: `RUNNING`;
- append: `COMPLETED`.

Esses estados confirmam o estado dos statements, não a correção funcional do Temporal Join nem a emissão de alertas. A validação funcional exige observar dados enriquecidos e mensagens de saída.



**Atualização — preflight global de statements (14/11/2026):** depois de resolver e validar o compute pool, mas antes de submeter qualquer statement, o orquestrador consulta a lista do pool e verifica os cinco nomes planejados (payments-transactions-startup, payments-pk-accounts, payments-enrichment, payments-append-only, payments-fraud-detection). Se qualquer nome já existir, interrompe antes da primeira submissão; não exclui nem sobrescreve statements. A verificação individual em submit() permanece como proteção adicional. Alteração publicada no commit 4dfe1228f91f4cbd383c041cdbfcba9dabe3a58f. Ainda depende de sincronização e bash -n local. Nenhuma operação Cloud foi executada.

## 9. Cenário positivo de fraude

Somente após confirmar os statements necessários em execução:

```bash
./scripts/run-sql.sh database/seed/simulate-events.sql
```

O SQL insere quatro transações positivas para o cartão 1. O wrapper registra a execução em `evidence/homologation/`.

**Critério:** observar no tópico `fraud_alerts` o alerta correspondente ao padrão. Registrar a mensagem e o contexto de consumo como evidência. A execução dos INSERTs ou a consulta ao PostgreSQL não comprovam, isoladamente, a emissão do alerta.

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

O script `flink/scripts/run.sh` pode criar um compute pool e submeter vários statements em sequência. Portanto, **permanece fora de execução até que seus efeitos sejam compreendidos e que cada unidade relevante tenha sido revisada**. Nenhuma operação Cloud/Neon está autorizada por este registro.

### Critério para avançar

Só avançar quando a unidade atual tiver: (a) revisão concluída; (b) execução autorizada e realizada, se aplicável; (c) resultado observado; e (d) evidência/estado registrado. Uma falha interrompe o avanço. Não repetir etapas já homologadas de snapshot, CDC incremental ou evolução real do schema.

