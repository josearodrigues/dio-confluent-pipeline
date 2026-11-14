-- Declara a chave da tabela de transações, conforme a preparação do mentor.
ALTER TABLE `payments.transactions` ADD PRIMARY KEY (transaction_id) NOT ENFORCED;
