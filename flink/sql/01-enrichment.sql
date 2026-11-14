-- ============================================================
-- Camada 4a — Enriquecimento com TEMPORAL JOIN
-- transactions ⋈ accounts (versão da dimensão no instante do evento)
--
-- O atributo temporal da fonte CDC (`t.$rowtime`) é usado somente como
-- referência do instante no Temporal Join. Não o projetamos como metadata
-- na tabela de saída: essa propagação esteve associada à falha do planner.
-- A saída mantém o contrato do projeto (`enriched_payments` e seus campos).
-- ============================================================
CREATE TABLE enriched_payments AS
SELECT
  t.transaction_id,
  t.card_id,
  t.amount,
  t.account_id,
  a.status AS account_status
FROM `payments.transactions` AS t
JOIN `payments.accounts` FOR SYSTEM_TIME AS OF t.`$rowtime` AS a
  ON t.account_id = a.account_id;
