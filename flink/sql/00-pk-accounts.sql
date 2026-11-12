-- Chave necessária no lado dimensional do Temporal Join.
ALTER TABLE `payments.accounts` ADD PRIMARY KEY (account_id) NOT ENFORCED;
