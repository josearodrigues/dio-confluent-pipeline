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
| Tópico fraud_alerts como saída | O fluxo termina no Kafka. PostgreSQL Sink não faz parte do escopo principal. |

## Ordem de homologação

A ordem operacional acompanha a demonstração do mentor e é requisito do roteiro: (1) snapshot CDC completo; (2) CDC incremental com INSERT, UPDATE e DELETE; (3) evolução compatível/incompatível do schema real do CDC; (4) Flink SQL com Temporal Join, append e MATCH_RECOGNIZE; (5) cenários funcionais e verificações de saída. O teste isolado de contrato Avro/Schema Registry é uma preparação e não substitui a evolução do subject real. Não iniciar Flink antes de concluir as etapas 2 e 3.

## Semântica e limitações

- `$rowtime`/`rowtime` usado na regra representa o timestamp associado ao evento Kafka, não o campo de negócio `created_at`.
- `latest-offset` evita que o histórico de transactions seja consumido como novos eventos pelo statement iniciado depois do snapshot; não altera a leitura da dimensão accounts.
- O append configurado no SQL trata eventos CDC de criação (`c`), snapshot (`r`) e atualização (`u`) como novas linhas e ignora exclusões (`d`), conforme documentado no SQL do projeto. Assim, UPDATE pode contribuir para a contagem do padrão. Isso não é uma reconstrução do estado atual nem uma contagem estrita de INSERTs de negócio.
- A regra positiva usa INSERTs novos para demonstrar o padrão, mas essa escolha não remove a limitação geral do append.
- `NOT ENFORCED` não valida integridade referencial ou unicidade real.
- O cenário é didático: não implementa deduplicação/reconciliação completa, motor de risco ou persistência em banco de destino.
- A janela é baseada no tempo de evento Kafka. Não deve ser descrita como janela baseada em `created_at` ou como garantia de emissão imediata em relógio de parede.
- A execução de `MATCH_RECOGNIZE` depende do comportamento temporal/watermark do Flink e deve ser observada na homologação.

## Segurança operacional

- Arquivos `.env`, `.state.env`, configurações renderizadas e segredos não devem ser versionados.
- O conector usa identidade Kafka dedicada e credenciais PostgreSQL fornecidas por ambiente/configuração local.
- A role PostgreSQL de replicação deve ter apenas os privilégios exigidos pela origem e pelo conector.
- Scripts de fundação, execução e teardown podem criar, alterar ou remover recursos. Revisar variáveis, contexto e escopo antes de executá-los.
- Recursos Confluent podem gerar custos enquanto provisionados/ativos; consultar Billing e os limites reais antes e depois do laboratório.

## Estado de validação

As decisões acima descrevem o desenho e os artefatos atuais. A ordem de execução está alinhada aos passos 5, 6 e 7 da demonstração do mentor. Os resultados cloud devem ser classificados somente com evidências produzidas e verificadas durante a execução real.
