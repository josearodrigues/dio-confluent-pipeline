# Relatório de Teardown

- Data UTC: seg. 16 nov 2026 02:27:48 UTC
- Environment esperado: desafio-env
- Environment ID: env-zz29p0
- Environment encontrado após operação: 0
- Service Accounts removidas (marcador de criação): metrics-reader
- Vínculo MetricsViewer: MetricsViewer removido e ausência confirmada por listagem
- Estado das identidades próprias: remoção solicitada e verificações posteriores concluídas

## Confluent

**Estado:** Environment ausente na verificação posterior

A listagem confirmou que o Environment não existe mais.

A exclusão do Environment remove recursos associados, incluindo clusters Kafka, Schema Registry, Connect e Flink Compute Pools. Confira a página de API Keys após a exclusão: chaves Flink regionais podem exigir verificação específica. A exclusão das Service Accounts criadas pelo laboratório também remove suas API keys associadas. Preserve identidades preexistentes.

## Neon

**Estado:** falha durante a limpeza SQL

Projeto: round-rain-76547215  
Branch: production  
Host: ep-round-moon-bexugixe.c-9.us-east-2.aws.neon.tech  
Database: payments

Alvo Neon informado: projeto=round-rain-76547215 branch=production
Alvo PostgreSQL: host=ep-round-moon-bexugixe.c-9.us-east-2.aws.neon.tech database=payments user=payments_owner
Roles a remover: CDC=cdc; Sink=alerts_sink
Confirmação recebida pelo wrapper; limpeza seletiva iniciada.
 pg_drop_replication_slot 
--------------------------
 
(1 row)

DROP PUBLICATION
ALTER DEFAULT PRIVILEGES
psql:database/cleanup/postgres-cleanup.sql:51: NOTICE:  drop cascades to 5 other objects
DETAIL:  drop cascades to table payments.customers
drop cascades to table payments.accounts
drop cascades to table payments.cards
drop cascades to table payments.merchants
drop cascades to table payments.transactions
DROP SCHEMA
DROP TABLE
REVOKE
REVOKE
REVOKE
psql:database/cleanup/postgres-cleanup.sql:70: ERROR:  permission denied to drop role
DETAIL:  Only roles with the CREATEROLE attribute and the ADMIN option on role "cdc" may drop this role.
Log: evidence/local/20261116T022745Z-postgres-cleanup.log

Projeto, branch, compute, endpoint e banco não são excluídos. A limpeza SQL remove somente os objetos indicados. Dependências adicionais podem impedir DROP ROLE; não é usado DROP OWNED CASCADE.

## Limites

O Environment é considerado ausente somente após verificação por listagem. Service Accounts são registradas como removidas somente após nova listagem. A limpeza Neon é considerada executada somente quando o wrapper SQL termina com código zero. Estados não consultados não equivalem a zero.
