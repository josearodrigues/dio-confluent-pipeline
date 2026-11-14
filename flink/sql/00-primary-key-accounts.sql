-- Declara a chave da dimensão usada pelo Temporal Join.
ALTER TABLE `payments.accounts` ADD PRIMARY KEY (account_id) NOT ENFORCED;
