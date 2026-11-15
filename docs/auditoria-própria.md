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
| Teardown: nome do statement de chave de transactions | Divergência confirmada | **Corrigido no código:** o teardown usa `payments-pk-transactions`, igual ao executor. |
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

1. Revisar com calma os limites restantes do teardown, especialmente o que só pode ser confirmado após consulta real aos recursos.
2. Fazer validação local de sintaxe dos scripts alterados antes de qualquer uso operacional.
3. Manter role do Sink, detecção de fraude e execução do Sink como pendências separadas; não ampliar o escopo automaticamente.

### Limites desta atualização

As alterações foram feitas estaticamente no GitHub. Não foram executados Bash, ShellCheck, comandos Confluent, SQL, operações Neon, criação ou remoção de recursos. Portanto, os ajustes estão **preparados no código**, mas ainda não foram testados em execução.