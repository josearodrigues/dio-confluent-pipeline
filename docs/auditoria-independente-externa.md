# Auditoria Técnica Independente – Projeto `dio‑confluent‑pipeline`

**Objetivo:** análise estática do código versionado, identificação de consistência, riscos e conhecimentos a preservar.  
**Escopo:** scripts Bash, SQL, conectores CDC/Sink, schemas Avro, Flink, segurança, observabilidade e custos.

---

## 1. Visão geral da implementação encontrada

| Área | Conteúdo principal | Observação |
|------|-------------------|------------|
| **Bash‑scripts** | `_common.sh`, `00-setup-neon.sh`, `infrastructure/foundation.sh`, `infrastructure/connectors/cdc/connector.sh`, `flink/scripts/run.sh`, `flink/scripts/checks.sh`, `operations/*` | Estrutura modular – `_common.sh` carrega `.env` e `.state.env`, fornece `require`, `save_state`, `save_api_key_credentials` e `render_template`. |
| **SQL – PostgreSQL** | `database/init/01-database.sql`, `database/seed/simulate-events.sql`, `database/verify/alerts.sql` | Cria schema `payments`, tabelas CDC, pre‑popula com 2 000 transações determinísticas. |
| **CDC & Sink** | `infrastructure/connectors/cdc/connector.json` + `connector.sh`; `infrastructure/connectors/sink/connector.json` + `connector.sh` | Conector PostgreSQL CDC V2 e PostgreSQL Sink configurados via JSON; credenciais são salvas dinamicamente via `save_api_key_credentials`. |
| **Schemas Avro** | `schemas/transaction-v1.avsc`, `schemas/transaction-v2.avsc` | Evolução demonstrada (campo `channel` adicionado na v2). |
| **Flink** | `flink/sql/*` (enrichment, append‑only, fraud‑detection) e scripts `run.sh`, `stop‑after‑validation.sh`, `checks.sh` | Usa **Temporal Join** (`payments‑enrichment.sql`), `append‑only` (CTAS), e `MATCH_RECOGNIZE` (detecção de fraude). |
| **Observabilidade / Custos** | `operations/cost‑and‑metrics.md`, `operations/verify‑metrics.sh`, `verify‑billing.sh`, `verify‑acls.sh` | Scripts read‑only que gravam JSON + resumo em `evidence/observability`. |
| **Teardown** | `teardown/cleanup.sh` – remove recursos Cloud (fora de escopo). |
| **Documentação** | `README.md`, `docs/*` (plano‑homologacao, decisões‑técnicas, matriz‑rastreabilidade) | Descrevem fluxos, pré‑requisitos e ordem de execução. |

---

## 2. Arquitetura identificada no código
```
PostgreSQL (Neon) ──► Debezium CDC (connector) ──► Kafka (topics) ──►
   Flink SQL   (Temporal Join → Append‑only → MATCH_RECOGNIZE) ──►
   Kafka topic fraud_alerts ──► PostgreSQL Sink (table public.fraud_alerts)
```
- **Environment** (`CONFLUENT_ENV_ID`) contém o cluster Kafka, o compute‑pool Flink (`lfcp-2268w8o`) e os conectores.
- **Schema Registry** armazena Avro schemas (`transaction‑v1/‑v2`).
- **ACLs** são criadas por `foundation.sh` (evidências em `evidence/cloud/03‑cdc‑connector.txt`).
- **Observabilidade** coleta métricas (Metrics API) e custos (Costs API).

---

## 3. Componentes e responsabilidades
| Componente | Responsabilidade | Artefatos principais |
|------------|------------------|----------------------|
| **Neon DB** | Fonte de dados transacionais | `database/init/01-database.sql` |
| **CDC Connector** | Captura mudanças e publica no Kafka | `infrastructure/connectors/cdc/connector.json`, `connector.sh` |
| **Schema Registry** | Versiona Avro schemas | `schemas/*.avsc` |
| **Flink Statements** | 1) `payments‑enrichment` (temporal join); 2) `payments‑append‑only` (CTAS → append); 3) `payments‑fraud‑detection` (MATCH_RECOGNIZE) | `flink/sql/*.sql`, `flink/scripts/run.sh` |
| **Kafka Topic `fraud_alerts`** | Recebe alertas de fraude | `flink/sql/04-fraud-detection.sql` |
| **Sink Connector** | Persiste alerts em PostgreSQL (`public.fraud_alerts`) | `infrastructure/connectors/sink/connector.json` |
| **Observabilidade** | Métricas, ACLs, billing | `operations/verify‑*.sh`, `evidence/observability/*` |
| **Teardown** | Remove recursos Cloud (não usado aqui) | `teardown/cleanup.sh` |

---

## 4. Pontos tecnicamente consistentes (sem defeitos)
- Carregamento seguro de variáveis (`_common.sh` usa `chmod 600` no arquivo de estado). 
- `save_state` é idempotente – sobrescreve chaves existentes. 
- Uso correto de `FOR SYSTEM_TIME AS OF` em `01‑enrichment.sql`. 
- Sequência de fases (snapshot → incremental → schema‑evolution → Flink) está documentada e refletida nos scripts. 
- Evolução de schema Avro está comprovada por evidência `20261114T012247Z‑real‑schema‑evolution.txt`. 
- Scripts de observabilidade validam JSON antes de gerar markdown. 
- `stop‑after‑validation.sh` verifica nome único, estado `RUNNING/STOPPED` e aguarda confirmação de `STOPPED`.

---

## 5. Defeitos, riscos e inconsistências encontrados
| Arquivo / Local | Classificação | Descrição | Justificativa | Impacto | Recomendação (não executada) |
|-----------------|--------------|-----------|---------------|---------|------------------------------|
| `flink/sql/01-enrichment.sql` (linhas 12‑13) | **Inconsistência** | O `SELECT` não projeta `$rowtime`, mas `MATCH_RECOGNIZE` assume sua existência. | Confluent restringe uso de metadata columns em downstream queries. | Possível erro runtime (`metadata column(s): '$rowtime' may cause wrong result`). | Documentar explicitamente que `$rowtime` está disponível apenas como coluna de sistema ou incluí‑lo no `SELECT`. |
| `flink/sql/04-fraud-detection.sql` (não exibido integralmente) | **Risco** | Usa `$rowtime` em `ORDER BY` e `MEASURES`. | Confluent “metadata columns restriction” pode impedir a query ou gerar resultados imprecisos. | Alertas de fraude podem ser incorretos. | Inserir nota de advertência ou testar com `DESCRIBE EXTENDED` antes de produção. |
| `operations/verify-billing.sh` (linhas 84‑90) | **Defeito** | Markdown contém “discount_amount: comando não encontrado” devido a backticks não escapados. | O script gera saída truncada. | Evidência de billing incompleta/confusa. | Escapar backticks ou usar `printf` ao gerar markdown. |
| `docs/plano-homologacao.md` (seções 165‑168) | **Inconsistência documental** | Atribui que a validação de fraude será adiada até “observabilidade e teardown”, mas `stop‑after‑validation.sh` exige validação aprovada antes da parada. | Cria loop lógico – não é possível parar statements sem validação, e a validação só ocorre depois do teardown. | Usuário pode ficar bloqueado. | Incluir procedimento alternativo (parada manual via CLI). |
| `infrastructure/connectors/cdc/connector.sh` | **Risco de idempotência** | Não verifica se o conector já existe antes de criar; criação repetida pode gerar erro 409. | Falta de checagem de existência. | Execução repetida pode falhar e deixar estado inconsistente. | Adicionar verificação (`confluent connect connector list … | grep …`). |
| `scripts/00-setup-neon.sh` | **Segurança** | Aceita qualquer valor não‑vazio como confirmação (`CONFIRM_RESET`). | Usuário pode confirmar acidentalmente. | Possível destruição de dados em ambiente real. | Exigir string literal `yes`/`y`. |
| `flink/scripts/checks.sh` | **Cobertura de teste** | Não valida `watermark`/`idle-timeout` antes de executar statements. | Watermarks podem estagnar se partições ociosas. | Perda de alertas de fraude. | Incluir verificação de `scan.idle-timeout` e `source-watermark` via `SHOW CREATE TABLE`. |
| `operations/verify-acls.sh` | **Potencial vazamento** | Grava saída completa de `confluent acl list` em markdown; pode conter identidades sensíveis. | Dados de ACLs são sensíveis. | Violação de privacidade. | Gravar apenas campos necessários ou remover arquivo de commit. |

---

## 6. Questões que dependem de execução para confirmação
| Questão | Descrição | Onde está registrado | Necessidade de teste |
|--------|-----------|----------------------|----------------------|
| Comportamento de `$rowtime` em `MATCH_RECOGNIZE` | Se o planner aceita `$rowtime` apesar da restrição, quais são os efeitos nos resultados? | Evidência `20261114T052012Z‑flink‑fraud‑detection‑partial.md`; logs não mostram erro fatal. | Executar a query com `EXPLAIN EXTENDED` para observar plano e mensagens de warning. |
| Idempotência do conector CDC | Criar o conector duas vezes gera erro 409 ou é ignorado? | `connector.sh` não faz verificação. | Rodar `confluent connect connector create …` duas vezes e observar código de retorno. |
| Efeito da política `scan.idle-timeout` | Script `run.sh` define `FLINK_IDLE_TIMEOUT`; não há verificação se a configuração afeta a watermark. | Comentários em `README.md` e `decisoes‑tecnicas.md`. | Executar `SHOW CREATE TABLE payments.transactions` antes e depois de mudar o timeout. |
| Persistência correta do `append‑only` changelog | O statement `02‑append‑only.sql` converte a tabela para `append` mode; não há teste de que apenas `INSERT`s são propagados. | `evidence/homologation/20261114T061128Z‑simulate‑events.log` mostra inserts. | Verificar se um `DELETE` no source gera linha de tombstone no `fraud_alerts`. |
| Conformidade dos ACLs com princípio *least‑privilege* | `foundation.sh` cria ACLs amplas (`*`). Não há teste de que somente as service accounts necessárias têm acesso. | `evidence/cloud/03‑cdc‑connector.txt`. | Listar ACLs (`confluent acl list`) e comparar com matriz de privilégios. |

---

## 7. Conhecimentos técnicos que merecem ser preservados
| Tema | Por que é valioso | Onde estão os trechos relevantes |
|------|-------------------|----------------------------------|
| Uso de Temporal Join com `$rowtime` | Demonstra como combinar eventos CDC com dimensões versionadas. | Comentário linhas 5‑13 de `flink/sql/01‑enrichment.sql`. |
| Evolução de Avro schema em ambiente CDC | Mostra como mudar o schema (`channel` field) sem interromper o pipeline. | Evidência `20261114T012247Z‑real‑schema‑evolution.txt`. |
| Separação de snapshot e incremental CDC | Evita que dados históricos poluam a lógica de janela temporal. | `docs/plano‑homologacao.md` §§ 5‑7, `evidence/homologation/20261113‑cdc‑snapshot‑validation.md`. |
| Configuração de `FLINK_IDLE_TIMEOUT` para lidar com partições ociosas | Estratégia para garantir progressão de watermarks em clusters com baixa taxa de eventos. | `README.md` linhas 135‑139; `decisoes‑tecnicas.md` § 34. |
| Modelo de auditoria de custos via Metrics API | Como capturar métricas de bytes recebidos e gerar relatórios legíveis. | `operations/verify‑metrics.sh`, evidências `metrics.json/md`. |
| Procedimento de parada segura (`stop‑after‑validation.sh`) | Garante que apenas statements alvo sejam interrompidos e que o estado final seja explicitamente verificado. | Script `flink/scripts/stop‑after‑validation.sh`. |
| Persistência de credenciais via `.state.env` com `chmod 600` | Boa prática de segurança local. | `_common.sh` linhas 24‑33. |
| Uso de `render_template` com `jq` para injetar variáveis no JSON | Permite criar dinamicamente arquivos de configuração sem expor segredos. | `_common.sh` linhas 51‑60. |

---

## 8. Recomendações priorizadas
| Prioridade | Categoria | Recomendações resumidas |
|-----------|-----------|------------------------|
| **Alta** | **Metadata columns** | Documentar (ou corrigir) a ausência de `$rowtime` no `SELECT` de `01‑enrichment.sql` e rever `04‑fraud‑detection.sql` para assegurar que a referência a `$rowtime` seja suportada ou substituída por uma coluna timestamp explícita. |
| **Alta** | **Documentação do fluxo de parada** | Inserir no `plano‑homologacao.md` um procedimento alternativo (parada manual via CLI) para o caso em que a validação de fraude não foi concluída, evitando o loop lógico. |
| **Média** | **Bug de markdown em `verify‑billing.sh`** | Escapar backticks ou usar `printf` ao gerar a frase “discount_amount”. |
| **Média** | **Idempotência dos conectores** | Adicionar verificação pré‑criação (`confluent connect connector list`) nos scripts `connector.sh`. |
| **Média** | **Segurança de confirmação** | Exigir confirmação explícita (`yes`/`y`) em scripts destrutivos (`00‑setup‑neon.sh`). |
| **Baixa** | **Verificação de `scan.idle-timeout`** | Incluir passo de inspeção (`SHOW CREATE TABLE …`) no script `checks.sh`. |
| **Baixa** | **Redução de escopo das ACLs** | Revisar ACLs criadas por `foundation.sh` e restringir a apenas as contas necessárias. |
| **Baixa** | **Sanitização de logs de ACLs** | Remover informações sensíveis antes de gravar arquivos de evidência. |

---

## 9. Avaliação objetiva da qualidade técnica
| Critério | Avaliação |
|----------|-----------|
| **Correctness (conformidade funcional)** | Em grande parte a solução segue boas práticas de CDC + Flink; porém a utilização de `$rowtime` em `MATCH_RECOGNIZE` entra em conflito com documentação oficial, indicando risco de resultados incorretos. |
| **Robustness (tratamento de erro, idempotência)** | Scripts têm verificações básicas (`require`, `save_state`), mas alguns (connectors, CDC setup) carecem de guardas contra repetição ou falhas de API. |
| **Security (exposição de credenciais, permissões)** | Uso de `.state.env` com `chmod 600` é adequado; porém logs de ACLs podem vazar identidades e o `setup‑neon.sh` aceita qualquer valor como confirmação. |
| **Operational safety (parada/controlos)** | `stop‑after‑validation.sh` fornece mecanismo seguro, mas depende de evidência que pode não existir; falta procedimento de fallback. |
| **Observability / Cost awareness** | Scripts de métricas e billing são bem estruturados, porém há bug no markdown e ainda falta monitoramento de confiabilidade (latência, falhas). |
| **Documentation vs Reality** | A documentação descreve a ordem correta, mas há divergências (adiamento da validação, necessidade de evidência) que podem confundir o operador. |
| **Overall maturity** | **Média‑Alta** para um laboratório didático. O código demonstra compreensão de conceitos avançados (temporal join, schema evolution), mas ainda carece de garantias de produção (idempotência, validação de metadata columns, controle de ACLs). |

---

## 10. Próxima ação recomendada (única)
**Criar um documento de “Procedimento Alternativo de Parada Manual”** que:
1. Descreva a sequência de comandos CLI (list → describe → stop → wait → evidência).
2. Especifique critérios de abortar (nome ambíguo, estado inesperado, timeout).
3. Defina o formato de evidência (Markdown com data/hora, IDs, estados antes/depois, ressalva de fraude não homologada).

Este documento permitirá interromper os statements imediatamente, sem gerar artificialmente a marca `VALIDAÇÃO APROVADA`, mantendo rastreabilidade e respeitando a decisão de que a fraude permanece não homologada.

---

*Relatório gerado por auditor independente, baseado exclusivamente nos artefatos versionados localmente.*