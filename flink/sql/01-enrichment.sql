-- ============================================================
-- Camada 4a — Enriquecimento com TEMPORAL JOIN
-- transactions ⋈ accounts (versão da dimensão no instante do evento)
--
-- Sobre o relógio usado (`$rowtime`):
--  * `$rowtime` é o timestamp do REGISTRO KAFKA — não o horário de negócio da
--    transação (`created_at`, vindo do Postgres).
--  * O Temporal Join usa `$rowtime` dos dois lados de propósito: as versões das
--    dimensões também são datadas pelo relógio do Kafka, então a comparação é
--    consistente. Usar `created_at` aqui descartaria transações da carga inicial.
--  * `created_at` segue na tabela como `tx_created_at` para auditoria.
--
-- Tópicos viram tabelas com o nome do tópico: usar crase em `payments.transactions`.
-- Esta versão segue o enriquecimento accounts-only demonstrado na referência.
-- ============================================================
CREATE TABLE enriched_transactions AS
SELECT
  t.transaction_id,
  t.card_id,
  t.account_id,
  t.merchant_id,
  t.amount,
  t.currency,
  t.status,
  t.created_at AS tx_created_at,
  a.client_id,
  a.account_type,
  a.status AS account_status
FROM `payments.transactions` AS t
JOIN `payments.accounts` FOR SYSTEM_TIME AS OF t.`$rowtime` AS a
  ON t.account_id = a.account_id;
