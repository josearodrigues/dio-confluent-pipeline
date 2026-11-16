# Decisões técnicas e aprendizados

Este documento registra decisões duradouras do desenho e limites observados. O roteiro reproduzível está no [README](../README.md); os resultados e evidências estão em [Homologação e aprendizados](plano-homologacao.md) e em `evidence/`.

## Decisões de arquitetura

| Tema | Decisão | Consequência |
|---|---|---|
| Origem | PostgreSQL com logical replication. | Permite snapshot inicial e captura incremental por CDC. |
| CDC | Confluent PostgreSQL CDC Source V2 / Debezium. | Publica eventos das tabelas customers, accounts, cards e transactions. |
| Contrato | Avro com Schema Registry. | Contratos tipados e compatibilidade explícita. |
| Snapshot | Validar o snapshot antes de iniciar a lógica de fraude. | Evita misturar deliberadamente a carga inicial com o cenário novo. |
| Startup de transactions | Usar `latest-offset` depois da validação do snapshot. | Evita consumir o histórico como novos eventos pelo statement iniciado posteriormente. |
| Enriquecimento | Temporal Join com accounts usando `$rowtime`. | Associa a transação à versão temporal da dimensão. |
| Chaves Flink | Declarar PK como `NOT ENFORCED`. | Informa metadados ao Flink, sem validar unicidade real. |
| Detecção | `MATCH_RECOGNIZE` por `card_id`, com três ou mais eventos positivos em até 60 segundos. | Regra demonstrativa baseada no tempo de evento Kafka. |
| Saída | Tópico Kafka `fraud_alerts` e PostgreSQL Sink downstream. | Separa geração do alerta e persistência. |
| Persistência | Sink Avro com UPSERT e chave `(card_id, first_txn)`. | Reentregas com a mesma chave podem ser idempotentes no destino, sem garantia exactly-once ponta a ponta. |

## Aprendizados e limites

### Tempo e CDC

- `$rowtime` é o tempo de evento associado ao stream Kafka; não é `created_at`.
- A janela de 60 segundos é avaliada no tempo de evento e depende de watermarks. Aguardar 60 ou 90 segundos em relógio de parede não é critério suficiente de aprovação.
- `changelog.mode=append` não é um filtro universal de operações Debezium. Não deve ser descrito como “somente INSERT”: eventos de atualização podem contribuir para o padrão como novas linhas.
- PK `NOT ENFORCED` não verifica unicidade ou integridade referencial no PostgreSQL.

### Detecção e fraude

- A condição `amount > 0` é deliberadamente simples e não representa análise de risco ou fraude financeira.
- Alertas sobre dados sintéticos são saídas de uma regra, não evidência de fraude real.
- Para homologar o cenário, é necessário correlacionar as transações de entrada com o alerta, preferencialmente preservando identificadores de origem, offsets e timestamps. Filtrar apenas por cartão, contagem e faixa temporal pode aceitar alertas anteriores ou não relacionados.
- A evidência atual confirma geração de alertas, mas não permite atribuir inequivocamente todos os resultados aos INSERTs do cenário positivo.
- O cenário negativo não foi homologado porque o script de teste falhou ao inserir sem preencher `created_at`, coluna obrigatória.

### Sink e garantias

- Estado `RUNNING` do conector/task não comprova persistência.
- Uma consulta que retorna registros no destino comprova que há dados persistidos, mas a homologação ponta a ponta requer correlação de uma mensagem específica do Kafka com a linha correspondente no PostgreSQL.
- A documentação do PostgreSQL Sink declara entrega at-least-once. UPSERT com chave estável reduz duplicações lógicas no destino, mas não equivale a exactly-once ponta a ponta.
- Não afirmar garantias de recuperação, ordenação global ou ausência de duplicatas sem testes próprios.

### Segurança e operação

- Manter `.env`, `.state.env`, credenciais e arquivos renderizados fora do Git.
- Usar identidades separadas e menor privilégio para CDC, Sink e métricas.
- Não remover identidades preexistentes. Excluir somente recursos cuja criação pelo laboratório esteja comprovada e cuja identidade tenha sido confirmada.
- Environment Confluent e projeto Neon têm ciclos de vida distintos; o sucesso da limpeza de um não comprova o outro.
- Não usar `DROP OWNED CASCADE` para forçar remoção de roles: dependências desconhecidas devem bloquear a operação e ser investigadas.
- Billing deve ser filtrado pelo Environment do laboratório; não confundir custo do Environment com custo total da organização.
- Custos recentes podem demorar até 72 horas para aparecer na Costs API.

## Resultado registrado

- Snapshot CDC, CDC incremental e evolução do subject real foram comprovados por evidências.
- ACLs/RBAC foram aprovados; métricas e Billing foram coletados.
- Statements Flink foram parados; o tópico de alertas foi lido; o Sink foi observado RUNNING e o destino retornou registros.
- A correlação completa do cenário de fraude não foi comprovada.
- O teardown Confluent foi confirmado. A limpeza Neon falhou ao remover a role `cdc`; portanto, o teardown integral não está comprovado.

Referência do conector: https://docs.confluent.io/cloud/current/connectors/cc-postgresql-sink.html
