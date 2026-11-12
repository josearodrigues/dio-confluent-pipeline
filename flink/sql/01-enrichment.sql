-- Temporal Join: cada transação recebe a versão da conta válida no rowtime do evento.
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
