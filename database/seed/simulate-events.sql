-- Cenário positivo: quatro INSERTs no mesmo cartão em poucos segundos.
-- Execute depois de o snapshot CDC terminar e de os statements Flink estarem RUNNING.
-- A regra Flink usa rowtime Kafka; created_at é informado para satisfazer o DDL
-- e manter o dado de negócio determinístico.
INSERT INTO payments.transactions
  (card_id, account_id, merchant_id, amount, currency, status, created_at)
SELECT c.card_id, c.account_id, 7, v.amount, 'BRL', 'approved',
       TIMESTAMPTZ '2026-11-13 12:00:00+00' + (v.n * INTERVAL '1 second')
FROM payments.cards c
CROSS JOIN (VALUES
  (1, 49.90::numeric),
  (2, 120.00),
  (3, 310.50),
  (4, 89.99)
) AS v(n, amount)
WHERE c.card_id = 1;

-- Resultado esperado: ao menos um padrão para card_id=1, sujeito à janela de 60s
-- medida pelo timestamp de chegada Kafka ($rowtime), e não por created_at.
SELECT transaction_id, card_id, amount, created_at
FROM payments.transactions
WHERE card_id = 1
ORDER BY transaction_id DESC
LIMIT 4;
