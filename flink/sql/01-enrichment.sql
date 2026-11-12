-- Temporal Join: cada transação recebe a versão da conta válida no rowtime do evento.
-- A coluna metadata timestamp preserva o timestamp Kafka de origem no tópico derivado,
-- para que o MATCH_RECOGNIZE posterior ordene pelo mesmo instante de chegada.
CREATE TABLE enriched_payments (
  rowtime TIMESTAMP_LTZ(3) NOT NULL METADATA FROM 'timestamp',
  transaction_id BIGINT,
  card_id INT,
  amount DECIMAL(15,2),
  account_id INT,
  account_status STRING
)
AS
SELECT
  t.`$rowtime` AS rowtime,
  t.transaction_id,
  t.card_id,
  t.amount,
  a.account_id,
  a.status AS account_status
FROM `payments.transactions` AS t
JOIN `payments.accounts` FOR SYSTEM_TIME AS OF t.`$rowtime` AS a
  ON t.account_id = a.account_id;
