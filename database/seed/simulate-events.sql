-- Cenário positivo: reproduz a sequência demonstrada pelo mentor.
-- Três INSERTs independentes para o mesmo cartão, seguidos por um quarto
-- INSERT para outro cartão. Aguardar 90 segundos antes da validação no Kafka.
--
-- Pré-requisitos:
--   - snapshot CDC concluído;
--   - statements Flink de enrichment e fraude em RUNNING;
--   - conferir que os IDs abaixo correspondem aos dados do ambiente.
--
-- A regra Flink usa $rowtime (tempo do stream), não created_at.
-- A quarta transação usa outro card_id e não integra a mesma partição do
-- MATCH_RECOGNIZE. Seu possível efeito sobre a watermark deve ser observado,
-- não presumido. O SQL atual define T apenas como amount > 0.

-- Evento 1: cartão 1, account 1, merchant 1.
INSERT INTO payments.transactions
  (card_id, account_id, merchant_id, amount, currency, status, created_at)
VALUES
  (1, 1, 1, 100.00, 'BRL', 'approved', now())
RETURNING transaction_id, card_id, account_id, merchant_id, amount, created_at;

-- Evento 2: mesmo cartão e merchant.
INSERT INTO payments.transactions
  (card_id, account_id, merchant_id, amount, currency, status, created_at)
VALUES
  (1, 1, 1, 100.00, 'BRL', 'approved', now())
RETURNING transaction_id, card_id, account_id, merchant_id, amount, created_at;

-- Evento 3: mesmo cartão e merchant.
INSERT INTO payments.transactions
  (card_id, account_id, merchant_id, amount, currency, status, created_at)
VALUES
  (1, 1, 1, 100.00, 'BRL', 'approved', now())
RETURNING transaction_id, card_id, account_id, merchant_id, amount, created_at;

-- Evento 4: outro cartão, conforme o cenário do mentor.
INSERT INTO payments.transactions
  (card_id, account_id, merchant_id, amount, currency, status, created_at)
VALUES
  (320, 80, 1, 10.00, 'BRL', 'approved', now())
RETURNING transaction_id, card_id, account_id, merchant_id, amount, created_at;

-- Janela de observação indicada na demonstração: aproximadamente 1–2 minutos.
SELECT pg_sleep(90);

-- Conferência das quatro linhas recém-inseridas (IDs também aparecem nos RETURNING).
SELECT transaction_id, card_id, account_id, merchant_id, amount, created_at
FROM payments.transactions
WHERE created_at >= now() - INTERVAL '3 minutes'
  AND (
    (card_id = 1 AND account_id = 1 AND merchant_id = 1 AND amount = 100.00)
    OR
    (card_id = 320 AND account_id = 80 AND merchant_id = 1 AND amount = 10.00)
  )
ORDER BY transaction_id DESC
LIMIT 10;
