-- Habilita a entrada append-only exigida pelo MATCH_RECOGNIZE.
-- Append não é um filtro de operações Debezium:
-- INSERT/READ/UPDATE são tratados como novas linhas; DELETE é ignorado.
-- Portanto, updates de entidades CDC podem contar como eventos adicionais no padrão.
ALTER TABLE enriched_payments SET ('changelog.mode' = 'append');
