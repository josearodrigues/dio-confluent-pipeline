-- A carga inicial CDC deve estar concluída antes desta configuração.
-- A tabela accounts permanece no startup padrão para carregar a dimensão.
-- A tabela transactions começa no offset mais recente para que a regra de fraude
-- considere os INSERTs posteriores à ativação, não a rajada do snapshot histórico.
ALTER TABLE `payments.transactions` SET ('scan.startup.mode' = 'latest-offset');
