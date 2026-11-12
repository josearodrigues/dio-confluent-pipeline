-- Cenário negativo opcional: três INSERTs para o mesmo cartão, separados por 35s.
-- A distância entre o primeiro e o terceiro excede a janela de 60s.
-- Execute somente após iniciar os statements Flink.
INSERT INTO payments.transactions (card_id, account_id, merchant_id, amount)
SELECT card_id, account_id, 9, 25.00 FROM payments.cards WHERE card_id = 4;
SELECT pg_sleep(35);
INSERT INTO payments.transactions (card_id, account_id, merchant_id, amount)
SELECT card_id, account_id, 9, 26.00 FROM payments.cards WHERE card_id = 4;
SELECT pg_sleep(35);
INSERT INTO payments.transactions (card_id, account_id, merchant_id, amount)
SELECT card_id, account_id, 9, 27.00 FROM payments.cards WHERE card_id = 4;
