# Decisões técnicas

Este documento registra as decisões refletidas nos artefatos versionados do projeto. A existência de um script ou SQL não comprova que o recurso foi executado ou homologado na Confluent Cloud.

## Decisões da solução

| Decisão | Motivo e consequência |
|---|---|
| PostgreSQL como origem | Permite demonstrar logical replication e captura CDC. A instância precisa aceitar logical replication e ser acessível pelo conector gerenciado. |
| Confluent PostgreSQL CDC Source V2 | Captura snapshot inicial e alterações incrementais por Debezium. A compatibilidade da origem deve ser confirmada para o provedor escolhido. |
| Avro com Schema Registry | Mantém contratos tipados para os tópicos de origem e permite demonstrar compatibilidade/evolução de schema. |
| Quatro tabelas CDC | O conector inclui customers, accounts, cards e transactions. merchants é tabela auxiliar do banco e não integra a publication CDC. |
| Snapshot antes do Flink | O snapshot inicial é validado antes de iniciar os statements, evitando misturar a carga histórica com o cenário de transações novas. |
| transactions em latest-offset | Após o snapshot, o statement de startup altera o offset de transactions para latest, para que a regra de fraude considere os eventos posteriores à ativação. |
| accounts como dimensão temporal | A dimensão permanece disponível para o Temporal Join; não recebe a mesma alteração de startup aplicada a transactions. |
| Temporal Join com accounts | Enriquecimento de cada transação com a versão da conta válida no `$rowtime` do evento. |
| Chaves primárias NOT ENFORCED | Declaram metadados de chave ao Flink; não verificam unicidade dos registros na origem. |
| Append após enriquecimento | Reproduz a etapa didática do mentor e habilita a entrada append-only para o padrão. Não equivale a filtrar exclusivamente INSERTs. |
| MATCH_RECOGNIZE por card_id | Detecta três ou mais registros positivos do mesmo cartão em até 60 segundos de `$rowtime`. |
| PostgreSQL Sink como destino downstream | O tópico `fraud_alerts` alimenta um Sink Avro com UPSERT na tabela pré-criada `public.fraud_alerts`; chave composta `(card_id, first_txn)`. |

## Ordem de homologação

Na demonstração do mentor, a sequência é: Passo 5 (CDC incremental com INSERT, UPDATE e DELETE), Passo 6 (compatibilidade de contrato Avro em subject isolado, aceitando evolução compatível e rejeitando incompatível) e Passo 7 (Flink: Temporal Join, append e MATCH_RECOGNIZE).

O projeto implementa duas validações diferentes:
- O teste isolado do Passo 6 é executado atualmente junto do provisionamento do Schema Registry, antes do CDC, por `infrastructure/schema-registry.sh`. É uma diferença de ordem em relação à demo; o script não deve ser repetido depois porque o subject já existe.
- A evolução do subject real `payments.transactions-value` é uma extensão do projeto, executada depois do CDC incremental e antes do Flink. Não é o teste isolado do Passo 6.

Na execução atual, não iniciar Flink antes de concluir snapshot, CDC incremental e a validação adicional do subject real.

## Semântica e limitações

- `$rowtime`/`rowtime` usado na regra representa o timestamp associado ao evento Kafka, não o campo de negócio `created_at`.
- `latest-offset` evita que o histórico de transactions seja consumido como novos eventos pelo statement iniciado depois do snapshot; não altera a leitura da dimensão accounts.
- O append configurado no SQL trata eventos CDC de criação (`c`), snapshot (`r`) e atualização (`u`) como novas linhas e ignora exclusões (`d`), conforme documentado no SQL do projeto. Assim, UPDATE pode contribuir para a contagem do padrão. Isso não é uma reconstrução do estado atual nem uma contagem estrita de INSERTs de negócio.
- A regra positiva usa INSERTs novos para demonstrar o padrão, mas essa escolha não remove a limitação geral do append.
- `NOT ENFORCED` não valida integridade referencial ou unicidade real.
- O cenário é didático: não implementa deduplicação/reconciliação completa nem motor de risco. O Sink foi preparado para persistência downstream, mas ainda não foi executado ou homologado.
- A janela é baseada no tempo de evento Kafka. Não deve ser descrita como janela baseada em `created_at` ou como garantia de emissão imediata em relógio de parede.
- A execução de `MATCH_RECOGNIZE` depende do comportamento temporal/watermark do Flink e deve ser observada na homologação.

## Segurança operacional

- Arquivos `.env`, `.state.env`, configurações renderizadas e segredos não devem ser versionados.
- O Sink reutiliza a Service Account `app-reader`, com ACL de leitura no tópico `fraud_alerts` e autorização READ no grupo `connect-` por prefixo.
- O Sink autentica no PostgreSQL com a role dedicada `alerts_sink`; a configuração desabilita criação e evolução automáticas da tabela.
- A inicialização não normaliza atributos privilegiados nem memberships de uma role `alerts_sink` preexistente; a auditoria read-only deve preceder a criação do conector.
- A documentação Confluent declara entrega at-least-once para o PostgreSQL Sink. UPSERT com chave estável pode tornar reentregas idempotentes no destino, sem equivaler a exactly-once ponta a ponta. Referência: https://docs.confluent.io/cloud/current/connectors/cc-postgresql-sink.html.
- A senha `PG_SINK_PASSWORD` é lida do ambiente pelo `psql`, sem ser incluída nos argumentos do processo.
- A role PostgreSQL de replicação deve ter apenas os privilégios exigidos pela origem e pelo conector.
- Scripts de fundação, execução e teardown podem criar, alterar ou remover recursos. Revisar variáveis, contexto e escopo antes de executá-los.
- Recursos Confluent podem gerar custos enquanto provisionados/ativos; consultar Billing e os limites reais antes e depois do laboratório.

## Estado de validação

As decisões acima descrevem o desenho e os artefatos atuais. A matriz de rastreabilidade documenta a diferença entre a ordem da demo e a ordem atual dos scripts do projeto. O Sink está configurado no repositório, mas sua execução, compatibilidade do schema Avro de saída, suficiência dos privilégios e persistência continuam pendentes de evidência real. A entrega at-least-once é declarada pela documentação do conector; UPSERT não é garantia exactly-once ponta a ponta. Os resultados cloud devem ser classificados somente com evidências produzidas e verificadas durante a execução real.

## Investigação de `$rowtime` em `MATCH_RECOGNIZE` — decisão de 14/11/2026

A documentação da Confluent consultada durante a revisão apresenta orientações aparentemente conflitantes: a página de restrições de metadata columns alerta para determinados usos downstream, enquanto o exemplo oficial de `MATCH_RECOGNIZE` utiliza `$rowtime` em `ORDER BY` e nas medidas. A divergência não foi resolvida formalmente.

A evidência operacional do projeto mostra que o statement `payments-fraud-detection-test`, criado às 05:20:12 UTC de 14/11/2026, foi aceito e observado em `RUNNING` com o SQL atual. Não foi encontrado erro de rejeição por metadata column nos registros pesquisados. O catálogo listou `fraud_alerts`, cujo `DESCRIBE EXTENDED` expôs `$rowtime` como `ROWTIME` com `SOURCE_WATERMARK()`. Isso comprova aceitação e estado observado, não correção funcional nem autorização geral para qualquer uso downstream.

**Decisão:** manter `flink/sql/04-fraud-detection.sql` sem alteração por esta suspeita. Não declarar que `$rowtime` é proibido em `MATCH_RECOGNIZE`, tampouco que a contradição documental foi resolvida. O warning `AGGREGATION_WITHOUT_KEY` identificado pertenceu a uma consulta exploratória de agregação global, não ao statement de fraude.

A consulta resumida de `fraud_alerts` observou 641 linhas com `txn_count=3`, mas não estabeleceu origem nem correlação. No cenário positivo, as transações 2002, 2003 e 2004 do cartão 1 chegaram a `enriched_payments` com `$rowtime` 06:11:30.774, 06:11:30.775 e 06:16:44.383 UTC. O intervalo de 5 min 13,609 s excedeu a janela de 60 segundos. Os alertas históricos consultados eram anteriores ao cenário. Portanto, a detecção funcional continua sem homologação.

A investigação sobre a proibição de `$rowtime` está encerrada para fins de decisão: não reescrever o SQL nem repetir consultas exploratórias sobre esse ponto. A nova homologação positiva foi adiada para depois da observabilidade e do teardown. Nenhuma ação Cloud foi executada como parte desta investigação. Consulte também a seção 16 de `docs/plano-homologacao.md`.



## Decisão de teardown — 15/11/2026

O encerramento é assimétrico porque os recursos são criados de formas diferentes:

- **Confluent:** o projeto provisiona o Environment e recursos associados. O teardown exclui o Environment como operação principal e verifica seu desaparecimento. Service Accounts e vínculo MetricsViewer são tratados separadamente somente quando os marcadores de estado comprovam criação pelo laboratório e após confirmação literal independente `IDENTITIES/ENV_ID`. A exclusão do Environment remove clusters, Schema Registry, Connect, ksqlDB e Flink Compute Pools. API keys Flink regionais não são removidas pela exclusão do Environment e precisam ser verificadas/removidas separadamente, após confirmar sua identidade e uso. API keys Flink regionais devem ser verificadas individualmente após a operação; a exclusão de uma Service Account remove suas API keys associadas, enquanto chaves vinculadas a outras identidades exigem avaliação separada. Service Accounts preexistentes são preservadas.
- **Neon:** o usuário cria o projeto PostgreSQL manualmente antes da execução e fornece os parâmetros no `.env`. O teardown preserva projeto, branch, compute, endpoint e banco. Remove `payments_slot`, `payments_pub`, schema `payments`, tabela `public.fraud_alerts` e as roles `PG_CDC_USER`/`PG_SINK_USER`.

A remoção das roles usa revogação dos privilégios conhecidos e `DROP ROLE`. Não é usado `DROP OWNED CASCADE`, pois poderia eliminar objetos não pertencentes ao laboratório. Dependências adicionais devem bloquear o `DROP ROLE` e ser investigadas. As confirmações Confluent e Neon são independentes. O relatório diferencia recurso removido, preservado, falha e estado não verificado.

Referências oficiais:
- Confluent, Environments on Confluent Cloud: https://docs.confluent.io/cloud/current/security/access-control/hierarchy/cloud-environments.html
- Confluent, Manage API Keys: https://docs.confluent.io/cloud/current/security/authenticate/workload-identities/service-accounts/api-keys/manage-api-keys.html
- Confluent, Service Accounts: https://docs.confluent.io/cloud/current/security/authenticate/workload-identities/service-accounts/overview.html
- Confluent API, https://docs.confluent.io/cloud/current/ccloud/delete-org-v-2-environment/
- Confluent API, https://docs.confluent.io/cloud/current/ccloud/delete-iam-v-2-service-account/
- PostgreSQL, https://www.postgresql.org/docs/current/sql-droprole.html
- PostgreSQL, https://www.postgresql.org/docs/17/role-removal.html


## Decisão complementar de teardown — 15/11/2026

Falhas em consultas, exclusões ou verificações de Service Accounts e do vínculo `MetricsViewer` devem ser propagadas explicitamente, sem depender apenas de `set -e` dentro de funções chamadas em condicionais. O vínculo só é reportado como removido após nova listagem confirmar sua ausência. O relatório distingue identidades preservadas por falta de confirmação, ausência de identidades marcadas e falha possivelmente parcial. A remoção continua limitada aos recursos cujo estado local registra criação pelo laboratório e exige confirmação literal independente `IDENTITIES/ENV_ID`.
