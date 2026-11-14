-- Auditoria somente leitura do destino PostgreSQL Sink.
-- Execute após a inicialização do banco e, para validar entrega, após um alerta conhecido.
SELECT card_id, first_txn, last_txn, txn_count
FROM public.fraud_alerts
ORDER BY first_txn DESC
LIMIT 100;

-- UPSERT deve manter uma única linha por chave composta.
SELECT card_id, first_txn, count(*) AS occurrences
FROM public.fraud_alerts
GROUP BY card_id, first_txn
HAVING count(*) > 1;

-- Atributos da role, privilégios efetivos e propriedade da tabela.
SELECT
  r.rolname,
  r.rolsuper,
  r.rolcreatedb,
  r.rolcreaterole,
  r.rolreplication,
  r.rolbypassrls,
  r.rolinherit,
  has_database_privilege(r.oid, current_database(), 'CONNECT') AS can_connect,
  has_schema_privilege(r.oid, 'public', 'USAGE') AS schema_usage,
  has_schema_privilege(r.oid, 'public', 'CREATE') AS schema_create,
  CASE WHEN c.oid IS NOT NULL THEN has_table_privilege(r.oid, c.oid, 'SELECT') END AS table_select,
  CASE WHEN c.oid IS NOT NULL THEN has_table_privilege(r.oid, c.oid, 'INSERT') END AS table_insert,
  CASE WHEN c.oid IS NOT NULL THEN has_table_privilege(r.oid, c.oid, 'UPDATE') END AS table_update,
  CASE WHEN c.oid IS NOT NULL THEN has_table_privilege(r.oid, c.oid, 'DELETE') END AS table_delete,
  pg_get_userbyid(c.relowner) AS table_owner,
  CASE WHEN c.oid IS NOT NULL THEN pg_has_role(r.oid, c.relowner, 'USAGE') END AS owner_privileges_available
FROM pg_roles r
LEFT JOIN pg_class c ON c.oid = to_regclass('public.fraud_alerts')
WHERE r.rolname = 'alerts_sink';

-- Memberships podem conceder privilégios herdados além dos GRANTs diretos.
SELECT parent.rolname AS granted_role, member.rolname AS member_role,
       am.admin_option
FROM pg_auth_members am
JOIN pg_roles parent ON parent.oid = am.roleid
JOIN pg_roles member ON member.oid = am.member
WHERE member.rolname = 'alerts_sink'
ORDER BY parent.rolname;
