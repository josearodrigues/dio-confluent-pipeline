-- Verificação do banco de origem e dos registros usados no cenário.
-- Os alertas são publicados no tópico Kafka fraud_alerts; não há Sink PostgreSQL no fluxo.
SELECT 'customers' AS tabela, count(*) AS registros FROM payments.customers
UNION ALL SELECT 'accounts', count(*) FROM payments.accounts
UNION ALL SELECT 'cards', count(*) FROM payments.cards
UNION ALL SELECT 'transactions', count(*) FROM payments.transactions;
