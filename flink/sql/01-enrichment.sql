-- ============================================================
-- Camada 4a — Enriquecimento com TEMPORAL JOIN
-- transactions ⋈ accounts (versão da dimensão no instante do evento)
--
-- Adaptação da sequência demonstrada pelo mentor:
-- 1. materializa a tabela enriquecida por Temporal Join;
-- 2. em 02-append-only.sql, altera seu changelog para append antes do
--    MATCH_RECOGNIZE.
--
-- Mantemos os nomes e o contrato do projeto (`payments.*` e
-- `enriched_payments`), sem copiar os identificadores do ambiente do mentor.
-- `$rowtime` da fonte CDC é usado somente como instante do Temporal Join;
-- não é projetado explicitamente como coluna da tabela de saída.
-- ============================================================
CREATE TABLE enriched_payments AS
SELECT
  t.transaction_id,
  t.card_id,
  t.amount,
  a.account_id,
  a.status AS account_status
FROM `payments.transactions` AS t
JOIN `payments.accounts` FOR SYSTEM_TIME AS OF t.`$rowtime` AS a
  ON t.account_id = a.account_id;
