-- Três ou mais transações positivas do mesmo cartão em até 60s de rowtime Kafka.
-- O relógio é a chegada ao Kafka ($rowtime), não transactions.created_at.
CREATE TABLE fraud_alerts AS
SELECT *
FROM enriched_payments
MATCH_RECOGNIZE (
  PARTITION BY card_id
  ORDER BY `$rowtime`
  MEASURES
    FIRST(T.`$rowtime`) AS first_txn,
    LAST(T.`$rowtime`) AS last_txn,
    COUNT(T.transaction_id) AS txn_count
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (T{3,}?) WITHIN INTERVAL '60' SECOND
  DEFINE T AS T.amount > 0
);
