-- Etapa demonstrada pelo mentor para habilitar a entrada do MATCH_RECOGNIZE.
-- Atenção: append não equivale a filtrar somente op=c. Updates/deletes do CDC podem
-- ser representados como novas linhas; por isso o teste de fraude usa INSERTs novos.
ALTER TABLE enriched_payments SET ('changelog.mode' = 'append');
