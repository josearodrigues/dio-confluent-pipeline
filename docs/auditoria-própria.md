# Auditoria técnica própria

- Repositório: `josearodrigues/dio-confluent-pipeline`
- Branch analisada: `main`
- Commit de referência da revisão inicial: `0c1a8406af9f893a127f0d6b44a8f08928017250`
- Atualização da matriz e correções estáticas: 15/11/2026, branch `main`.
- Natureza: revisão estática do código e confronto com a documentação/evidências versionadas.
- Execuções realizadas nesta auditoria: nenhuma. Não foram executados scripts, SQL, comandos Confluent/Neon nem operações Cloud.
- Escopo: scripts Bash de preparação, provisionamento, execução, verificações e teardown; SQL PostgreSQL; configuração dos conectores CDC/Sink; SQL Flink; schemas Avro; README, decisões técnicas, plano e evidências relevantes.

## Síntese

A implementação tem boas proteções operacionais em pontos importantes: confirmação explícita para operações destrutivas, pré-validação do PostgreSQL antes do reset, armazenamento restrito de estado local, renderização de configurações com permissão restrita, verificação de recursos por nome/ID e separação entre configuração preparada e execução demonstrada.

A revisão encontrou, porém, inconsistências concretas que merecem correção antes de considerar os scripts reproduzíveis como conjunto. As mais importantes estão no timeout de idleness do Flink, na cobertura e no relatório do teardown, e na divergência entre ACLs criadas e ACLs esperadas pelo verificador.

Também há uma divergência documental relevante: o relatório de confiabilidade versionado descreve garantias do PostgreSQL Sink e comportamento exactly-once semântico no destino, embora o próprio README, o plano e as decisões técnicas indiquem que o Sink não foi executado e que a detecção de fraude não foi demonstrada funcionalmente.

## Achados

### 1. Valor padrão de idleness do Flink diverge da documentação

- Classificação: inconsistência comprovada entre código e documentação.
- Arquivos: `flink/scripts/run.sh`, `README.md`.
- Evidência no código: `FLINK_IDLE_TIMEOUT="${FLINK_IDLE_TIMEOUT-5 s}"`.
- O operador `-` define `5 s` quando a variável não está definida. Portanto, a execução sem configuração explícita aplica timeout fixo de cinco segundos aos statements de enrichment e fraude.
- O README descreve o comportamento progressivo da Confluent como padrão e orienta definir `FLINK_IDLE_TIMEOUT="5 s"` apenas para reproduzir o timeout fixo do mentor.
- Impacto: a execução real do script pode usar uma política de watermark diferente da que o README leva o operador a esperar. Isso é especialmente relevante para a regra temporal de fraude.
- Recomendação inicial: alinhar o valor padrão do script e a documentação.
- **Estado:** corrigido em `flink/scripts/run.sh`; timeout fixo só é aplicado quando informado explicitamente.

### 2. Teardown não remove todos os recursos criados pela fundação

- Classificação: lacuna comprovada no código.
- Arquivos: `infrastructure/foundation.sh`, `teardown/cleanup.sh`.
- A fundação cria a Service Account `metrics-reader`, a API key `METRICS_API_KEY` e o vínculo RBAC `MetricsViewer`.
- O teardown remove as API keys de writer, reader e Schema Registry, mas não inclui `METRICS_API_KEY`. Também remove apenas as Service Accounts writer e reader, sem tratar `SA_METRICS_ID`/`SA_METRICS_CREATED` nem o vínculo `MetricsViewer`.
- Há ainda divergência no nome do statement de chave de transactions: o executor cria `payments-pk-transactions`, enquanto o teardown tenta excluir `payments-transactions-startup`. O statement atual pode permanecer sem ser removido individualmente.
- Impacto: a limpeza pode deixar credenciais, identidade e vínculo RBAC residuais; statements podem permanecer até a remoção do Environment ou sobreviver caso a sequência de exclusão falhe.
- Recomendação inicial: inventariar os recursos criados e alinhar seus identificadores.
- **Estado:** nome do statement corrigido; teardown passou a tratar API key, RBAC e Service Account de métricas com base nos marcadores de propriedade. A execução da limpeza ainda não foi realizada.

### 3. O relatório do teardown pode registrar contagens incorretas

- Classificação: defeito comprovado na lógica de contabilização.
- Arquivo: `teardown/cleanup.sh`.
- A consulta de Environment usa jq dentro de aspas simples com `.id=="$ENV_ID"`. Como a variável Bash não é expandida dentro do programa jq, o filtro compara o ID com o texto literal `$ENV_ID`; a contagem tende a resultar em zero independentemente do estado real.
- Em `post_counts`, falhas nas consultas são convertidas em zero. Assim, indisponibilidade, contexto inválido ou falha de autorização podem ser apresentados como ausência de recursos.
- `NEON_AFTER` é definido como zero sem consultar o banco. Além disso, a limpeza Neon é opcional e sua falha é suprimida com `|| true`.
- O texto final afirma que todas as contagens devem ser zero, mas os valores apresentados não comprovam isso.
- Impacto: o relatório pode declarar uma limpeza completa sem ter verificado os recursos, inclusive quando o banco não foi limpo.
- Recomendação inicial: usar `jq --arg` e não converter falhas em zero.
- **Estado:** filtro corrigido; consultas posteriores agora mostram “não consultado” em caso de falha, e Neon é identificado como não verificado. O conteúdo do banco não é auditado por este relatório.

### 4. ACLs de grupos esperadas pelo verificador não são criadas pela fundação

- Classificação: inconsistência comprovada entre provisionamento e verificação.
- Arquivos: `infrastructure/foundation.sh`, `operations/verify-acls.sh`.
- A função `ensure_group_acl` cria somente a operação `READ` para os grupos `app-reader`, `connect-` e `snapshot-check-`.
- O verificador inclui também uma ACL `DESCRIBE` para o grupo prefixado `connect-`. A tabela explicativa do próprio verificador apresenta permissões de grupo `READ` e `DESCRIBE` de forma mais ampla.
- Impacto: a verificação estrita pode apontar ACL ausente mesmo após a fundação terminar com sucesso. Também fica pouco claro se a permissão adicional é requisito real do conector ou apenas expectativa excessiva do relatório.
- **Estado:** o verificador foi alinhado às ACLs `READ` efetivamente criadas. Nenhuma permissão adicional foi concedida; a suficiência em runtime permanece fora do escopo desta revisão estática.

### 5. Relatório de confiabilidade afirma garantias de um trecho ainda não executado

- Classificação: afirmações documentais não sustentadas pelo estado registrado no próprio repositório.
- Arquivo: `evidence/observability/20261115T005300Z-reliability.md`.
- O relatório descreve o fluxo completo até PostgreSQL Sink, afirma que o Sink está configurado com UPSERT e conclui por “Exactly-Once semântico no destino relacional”.
- O README, `docs/plano-homologacao.md` e `docs/decisoes-tecnicas.md` informam que o Sink está preparado no código, mas não foi criado/executado nem teve persistência validada. A detecção de fraude também não foi demonstrada funcionalmente.
- A configuração de UPSERT e uma chave composta podem ser analisadas estaticamente, mas não provam que o conector iniciou, que a role tem privilégios suficientes, que a conversão Avro/PostgreSQL funciona em execução ou que reentregas foram testadas.
- Impacto: o relatório transmite uma garantia ponta a ponta que o projeto ainda não demonstrou.
- **Estado:** documento reescrito como análise conceitual; a conclusão de exactly-once semântico foi removida e o Sink é identificado como preparado, não executado.

### 6. Privilégios da role do Sink permanecem como ponto de verificação

- Classificação: risco/ponto a verificar, não defeito comprovado.
- Arquivos: `database/init/01-database.sql`, `database/verify/alerts.sql`, `infrastructure/connectors/sink/connector.json`, `docs/plano-homologacao.md`.
- O SQL concede à role do Sink CONNECT, USAGE no schema `public` e SELECT/INSERT/UPDATE na tabela. A configuração desabilita `auto.create` e `auto.evolve`.
- A auditoria SQL foi preparada para examinar atributos, memberships e privilégios efetivos, mas o repositório registra que o resultado de memberships não foi documentado. A documentação do conector lista pré-requisitos mais amplos em termos gerais; não se deve concluir apenas daí que todos são necessários nesta configuração específica.
- Impacto: uma role preexistente pode conservar atributos ou memberships que ampliem privilégios; por outro lado, os privilégios concedidos podem não ser suficientes para o conector na configuração escolhida.
- Recomendação: manter a auditoria read-only como pré-condição e só ajustar privilégios após identificar uma falha concreta ou requisito documentado. Não executar o Sink como parte desta revisão.

### 7. O cenário positivo não garante, por si só, a chegada dentro da janela de 60 segundos

- Classificação: limitação operacional do cenário de teste.
- Arquivos: `database/seed/simulate-events.sql`, `flink/sql/04-fraud-detection.sql`.
- O cenário insere três transações sequencialmente e usa `now()` em cada INSERT. A regra, porém, usa `$rowtime` associado ao stream Kafka, e não o timestamp de negócio `created_at`.
- A execução registrada anteriormente encontrou os três eventos em `enriched_payments`, mas o primeiro e o terceiro `$rowtime` ficaram separados por mais de cinco minutos. Portanto, não satisfizeram a janela de 60 segundos.
- O script não mede nem impõe a distância temporal efetiva entre os eventos recebidos pelo Flink; o `pg_sleep(90)` posterior apenas aguarda antes da consulta.
- Impacto: inserir três linhas no PostgreSQL não basta para provar que a regra deveria emitir alerta. A correlação precisa usar os tempos observados no stream e a saída Kafka.
- Recomendação: documentar o critério com clareza e, em uma eventual nova execução autorizada, medir os timestamps/offsets recebidos e correlacionar um alerta novo no tópico. Não repetir o cenário durante esta auditoria.

## Aspectos bem encaminhados

- `scripts/run-sql.sh` exige confirmação para o reset, valida conexão, `wal_level=logical`, atributos da role CDC e autenticação da role antes de executar o SQL destrutivo.
- `database/init/01-database.sql` carrega dados determinísticos, configura `REPLICA IDENTITY FULL`, cria a publication com quatro tabelas e inclui asserções de contagem dentro da transação.
- `scripts/_common.sh` centraliza carregamento de configuração, persistência de estado com permissão restrita e renderização de templates sem imprimir credenciais.
- O conector CDC verifica a existência prévia do conector e do replication slot, renderiza a configuração em arquivo temporário com permissão restrita e espera o estado RUNNING.
- A evolução do subject CDC real verifica a versão anterior, a compatibilidade, o evento correlacionado e o estado do conector. A operação é explicitamente confirmada e o registro de teste tem limpeza compensatória.
- O SQL Flink separa Temporal Join, append e MATCH_RECOGNIZE, e as decisões técnicas reconhecem limitações de `NOT ENFORCED`, append e `$rowtime`.
- O projeto distingue corretamente, em vários documentos, a existência de configuração/script da comprovação de execução real.

## Estado técnico que a documentação deve preservar

1. Snapshot CDC, CDC incremental e evolução do subject real possuem evidências versionadas específicas.
2. As chaves do Flink foram concluídas; enrichment e statement de fraude foram observados em execução.
3. O cenário positivo anterior não comprovou a detecção: os tempos observados não respeitaram a janela de 60 segundos e não houve correlação demonstrada com mensagem nova no tópico `fraud_alerts`.
4. O PostgreSQL Sink está preparado no código, mas não foi executado nem validado.
5. Não há comprovação de persistência no PostgreSQL, nem de garantia exactly-once ponta a ponta.
6. As evidências e o estado documentado não autorizam afirmar que o pipeline completo foi demonstrado de ponta a ponta.

## Limites desta auditoria

Esta foi uma análise estática dos arquivos listados no escopo, usando a versão da branch `main` indicada no cabeçalho. Não foram executados testes locais, análise ShellCheck, validação de SQL contra instância real, chamadas à Confluent Cloud ou ao Neon, nem provisionamento/remoção de recursos. Assim, compatibilidade de versões da CLI, comportamento de APIs, permissões efetivas e funcionamento em runtime permanecem pontos a verificar.

Na revisão inicial, não foram alterados código, configurações ou recursos Cloud/Neon. As correções estáticas posteriores estão discriminadas na matriz abaixo. Nenhuma operação Cloud/Neon foi executada. Este arquivo permanece o registro da análise própria; o relatório independente externo continua separado em `docs/auditoria-independente-externa.md`.


## Matriz consolidada e situação das correções — 15/11/2026

| Tema | Classificação | Situação nesta atualização |
|---|---|---|
| Padrão de idleness em `flink/scripts/run.sh` | Inconsistência confirmada entre script e README | **Corrigido no código:** o padrão agora fica vazio; timeout fixo só é aplicado quando configurado explicitamente. |
| Teardown: nomes dos statements Flink | Cobertura incompleta para nomes atuais e aliases históricos documentados | **Corrigido no código:** a lista inclui `payments-pk-transactions` e os aliases históricos `payments-enrichment-test` e `payments-fraud-detection-test`, além dos nomes atuais. A remoção ainda não foi executada. |
| Teardown: vínculo e identidade de métricas | Lacuna confirmada | **Corrigido parcialmente no código:** a fundação registra se criou o vínculo MetricsViewer; o teardown remove a API key, o vínculo e a Service Account de métricas somente quando pertencem ao projeto. A limpeza ainda não foi executada. |
| Teardown: contagens posteriores | Defeito confirmado de contabilização | **Corrigido no código:** falhas de consulta deixam de ser convertidas em zero; Environment usa o ID via `jq --arg`; Neon aparece como não verificado. |
| ACLs de grupos | Divergência entre fundação e verificador | **Corrigido no verificador:** removida a expectativa de DESCRIBE não provisionada. Nenhuma permissão adicional foi concedida. |
| Relatório de confiabilidade | Afirmações não sustentadas por execução | **Corrigido no documento:** agora é análise conceitual; Sink e detecção de fraude permanecem explicitamente não demonstrados. |
| Uso de `$rowtime` em `MATCH_RECOGNIZE` | Suspeita não confirmada | **Sem alteração**, conforme decisão técnica registrada: statement aceito/observado, mas detecção funcional não comprovada. |
| Bug de Markdown no Billing | Achado histórico já corrigido | **Não reaberto:** a versão atual já contém a correção. |
| Guarda de duplicidade do conector CDC | Falso positivo parcial da auditoria externa | **Não tratado como defeito:** o script verifica conector e replication slot; isso evita duplicação, mas não equivale a reexecução idempotente completa. |
| Confirmação do reset Neon | Falso positivo da auditoria externa | **Não tratado como defeito:** o script exige a confirmação literal prevista, não qualquer texto não vazio. |
| Privilégios efetivos da role do Sink | Ponto a verificar | **Pendente:** não alterar permissões sem resultado da auditoria read-only ou requisito específico confirmado. |
| Detecção funcional de fraude | Limitação comprovada pelas evidências disponíveis | **Pendente:** os três eventos observados não ficaram dentro da janela de 60 segundos; não há correlação comprovada com mensagem nova em `fraud_alerts`. |

### Próximas prioridades

1. A sintaxe Bash dos quatro scripts e, posteriormente, da alteração isolada de `teardown/cleanup.sh` foi informada pelo usuário como válida (`bash -n`); isso não comprova comportamento operacional.
2. Manter role do Sink, detecção de fraude e execução do Sink como pendências separadas; não ampliar o escopo automaticamente.

### Limites desta atualização

As alterações foram feitas estaticamente no GitHub. Não foram executados Bash, ShellCheck, comandos Confluent, SQL, operações Neon, criação ou remoção de recursos. Portanto, os ajustes estão **preparados no código**, mas ainda não foram testados em execução.

## Validação local de sintaxe Bash — 15/11/2026

O usuário executou no clone local o comando abaixo e informou o retorno `Sintaxe Bash OK`:

```bash
bash -n flink/scripts/run.sh \
  infrastructure/foundation.sh \
  teardown/cleanup.sh \
  operations/verify-acls.sh \
  && echo "Sintaxe Bash OK"
```

**Resultado reportado:** a verificação de sintaxe passou para os quatro scripts listados. Esta validação não executa os scripts nem comprova comportamento em runtime, permissões, acesso à Confluent Cloud ou ao Neon. Nenhuma operação Cloud/Neon foi realizada nesta etapa.


## Validação local da alteração do teardown — 15/11/2026

Após sincronizar a alteração que inclui os aliases históricos dos statements Flink, o usuário executou:

```bash
bash -n teardown/cleanup.sh && echo "Sintaxe Bash OK"
```

**Resultado reportado:** `Sintaxe Bash OK`. A validação confirma somente a sintaxe Bash do arquivo `teardown/cleanup.sh`. Não executa o script, não testa as chamadas à Confluent CLI, não comprova a remoção de recursos e não autoriza a limpeza. O teardown permanece não executado.


## Revisão do teardown — 15/11/2026

A decisão de encerramento foi atualizada após confirmação do usuário. O escopo Neon inclui remover as roles configuradas pelo laboratório (`PG_CDC_USER` e `PG_SINK_USER`, normalmente `cdc` e `alerts_sink`), além de publication, replication slot, schema e tabela de destino. Projeto, branch, compute, endpoint e banco permanecem preservados.

A remoção das roles não utiliza `DROP OWNED CASCADE`. O SQL revoga os privilégios conhecidos concedidos pelo init e tenta `DROP ROLE`; dependências externas, ownerships ou grants adicionais fazem o PostgreSQL interromper a operação. Essa é uma proteção deliberada, não uma limpeza completa presumida.

No lado Confluent, o Environment é removido como operação principal. Service Accounts e vínculo MetricsViewer são tratados somente quando os marcadores de estado indicam criação pelo projeto e após confirmação literal independente `IDENTITIES/ENV_ID`. API keys Flink regionais não são removidas pela exclusão do Environment; chaves vinculadas a identidades não removidas exigem revisão residual. A confirmação destrutiva Neon é independente, mas sua execução depende da confirmação por listagem de que o Environment Confluent está ausente. Se essa ausência não puder ser verificada, o teardown bloqueia a etapa Neon.

**Estado desta atualização:** alterações estáticas preparadas no GitHub; nenhuma execução Bash, SQL, Confluent Cloud ou Neon foi realizada. A sintaxe e o comportamento dos scripts precisam ser validados localmente antes de qualquer execução destrutiva.


## Correção adicional de confirmação Confluent — 15/11/2026

Na revisão após a validação local de sintaxe reportada pelo usuário, foi identificado que a ramificação em que o Environment já estava ausente chamava a remoção de Service Accounts sem uma confirmação própria. O teardown foi corrigido para exigir a confirmação literal `IDENTITIES/ENV_ID` antes de remover qualquer identidade marcada como criada pelo laboratório. A mesma confirmação é exigida após a exclusão do Environment. O README, o plano de homologação e as decisões técnicas foram alinhados.

O usuário reportou `bash -n teardown/cleanup.sh` e `bash -n scripts/run-sql.sh` sem mensagens de erro antes desta correção. A alteração posterior ainda precisa dessa validação local. Nenhum script, SQL ou operação Cloud/Neon foi executado.


## Correção de propagação de falhas no teardown — 15/11/2026

### Achado

A remoção de identidades era chamada dentro de uma lista OR (`confirm_and_delete_owned_identities || ...`). Nesse contexto, `errexit` não oferece a proteção presumida para todos os comandos internos da função. A rotina podia continuar após falha de remoção de uma Service Account e terminar com status de sucesso. O vínculo `MetricsViewer` também era considerado removido após o comando de exclusão, sem consulta posterior que confirmasse a ausência.

### Correção estática

- Comandos de listagem, exclusão e verificação agora têm tratamento explícito de erro e retornam falha ao chamador.
- O vínculo `MetricsViewer` é consultado novamente após a exclusão; só é classificado como removido quando a listagem retorna zero vínculos.
- O estado de identidades próprias é registrado separadamente, incluindo preservação por falta de confirmação e possibilidade de remoção parcial.
- O relatório não confunde falha parcial com remoção concluída.

### Estado e limites

Alteração publicada no GitHub. Aguarda sincronização e validação local de sintaxe pelo usuário. Não foram executados scripts, comandos Confluent, SQL, operações Neon ou exclusões Cloud. A revisão é estática e não comprova comportamento operacional.


## Verificação final do teardown Neon — 15/11/2026

Na revisão estática posterior ao commit `12e4c6d`, foi identificado que o SQL de limpeza apresentava as existências dos objetos ao final, mas não convertia uma eventual permanência em código de erro. Como o wrapper classifica a limpeza pelo código de saída do `psql`, isso poderia registrar sucesso sem uma asserção explícita sobre todos os alvos.

**Correção estática:** o SQL agora calcula uma condição conjunta de ausência para schema `payments`, tabela `public.fraud_alerts`, publication `payments_pub`, slot `payments_slot` e roles configuradas. Se algum alvo continuar presente, o script encerra com código 4; a consulta detalhada permanece para evidência.

**Limites:** alteração preparada no GitHub; ainda não validada localmente nem executada contra PostgreSQL/Neon. A sintaxe Bash já reportada pelo usuário não valida sintaxe SQL/psql. Nenhuma operação Cloud/Neon foi realizada.


## Sincronização local e sintaxe Bash — 15/11/2026

O usuário informou que sincronizou a branch local `main` até `c2d424a`. Em seguida, executou `bash -n teardown/cleanup.sh` e `bash -n scripts/run-sql.sh`; ambos terminaram sem mensagens de erro. Resultado classificado como validação de sintaxe Bash, não como teste funcional ou validação SQL/psql. Nenhuma operação Confluent Cloud ou Neon foi executada.

## Dependência entre as etapas Confluent e Neon — 15/11/2026

### Achado

A revisão identificou que o fluxo anterior permitia iniciar a limpeza Neon mesmo quando a etapa Confluent havia sido ignorada, recusada ou não podia confirmar a exclusão do Environment. Isso contrariava a ordem operacional planejada e poderia deixar ativo o connector CDC enquanto o SQL tentava remover o replication slot.

### Correção estática publicada

- `teardown/cleanup.sh` mantém um indicador explícito de confirmação da ausência do Environment.
- A ausência é considerada confirmada somente quando a listagem da Confluent CLI não encontra o Environment, antes da execução ou após a solicitação de exclusão.
- Se os dados/CLI Confluent estiverem ausentes, a confirmação for recusada, o Environment continuar listado ou a verificação posterior falhar, a limpeza Neon é bloqueada.
- O relatório registra o bloqueio e orienta corrigir a condição e executar novamente o teardown.
- `README.md` e o plano de homologação explicam a dependência; as confirmações destrutivas continuam independentes.

Commits publicados: `11ee3fda` (gate da etapa Neon), `e57c8e82` (README), `1cb753d8` (mensagem de falha alinhada) e `26a93122` (plano de homologação).

**Estado:** alteração revisada estaticamente e conteúdo conferido no GitHub. Ainda não foi executado `bash -n` após essas mudanças, nem teste funcional do fluxo. Nenhum comando Confluent, SQL, Neon ou operação destrutiva foi executado.

 
## Proteção do replication slot no teardown — 15/11/2026

- Arquivo: `database/cleanup/postgres-cleanup.sql`.
- Alteração: removida a tentativa automática de `pg_terminate_backend` e a espera fixa de dois segundos.
- Antes de qualquer DDL destrutivo, o SQL consulta `pg_replication_slots.active_pid` para `payments_slot`. Se houver backend ativo, encerra com código SQL-script 5 e orienta parar/excluir o connector CDC na Confluent; nenhum objeto do laboratório é removido nessa tentativa.
- Se o slot estiver ausente, a limpeza prossegue. Se existir e estiver inativo, solicita sua remoção.
- Limite: a consulta é uma verificação pontual; não substitui a confirmação de que o Environment/connector CDC foi parado. Uma reconexão concorrente ainda pode fazer a remoção do slot falhar, mas o erro interrompe a execução com `ON_ERROR_STOP=1`.
- Natureza: correção estática publicada; não foi executada contra Neon. A validação operacional depende do pull local e da execução autorizada do teardown.
