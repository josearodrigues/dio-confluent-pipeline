-- Cenário positivo: quatro INSERTs no mesmo cartão em poucos segundos.
-- Execute depois de o snapshot CDC terminar e de os statements Flink estarem RUNNING.
INSERT INTO payments.transactions
  (card_id, account_id, merchant_id, amount, currency, status)
SELECT c.card_id, c.account_id, 7, v.amount, 'BRL', 'approved'
FROM payments.cards c
CROSS JOIN (VALUES (49.90::numeric), (120.00), (310.50), (89.99)) AS v(amount)
WHERE c.card_id = 1;

-- Resultado esperado: ao menos um padrão para card_id=1, sujeito à janela de 60s
-- medida pelo timestamp de chegada Kafka ($rowtime), e não por created_at.
SELECT transaction_id, card_id, amount, created_at
FROM payments.transactions
WHERE card_id = 1
ORDER BY transaction_id DESC
LIMIT 4;
