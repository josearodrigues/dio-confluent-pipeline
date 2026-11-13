-- Habilita a representação append-only exigida pelo MATCH_RECOGNIZE.
-- Esta configuração altera o changelog consumido pelo operador seguinte.
-- Não equivale a filtrar universalmente operações Debezium (c/r/u/d).
-- A forma como updates/retractions são representados deve ser homologada no plano/runtime.
ALTER TABLE enriched_payments SET ('changelog.mode' = 'append');
