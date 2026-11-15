-- DESTRUTIVO: somente no banco dedicado ao laboratório.
-- Preserva projeto, branch, compute, endpoint e banco Neon.
\getenv cdc_role PG_CDC_USER
\getenv sink_role PG_SINK_USER
\if :{?cdc_role}
\else
  \echo 'ERRO: PG_CDC_USER ausente.'
  \quit 3
\endif
\if :{?sink_role}
\else
  \echo 'ERRO: PG_SINK_USER ausente.'
  \quit 3
\endif
\if :{?cleanup_ok}
  \echo 'Confirmação recebida pelo wrapper; limpeza seletiva iniciada.'
\else
  \echo 'ERRO: use CONFIRM_CLEANUP=yes após conferir o alvo.'
  \quit 3
\endif

-- Não tenta encerrar sessões: no Neon, pg_terminate_backend pode não
-- ter privilégio sobre outra role membro de neon_superuser.
-- Se o slot estiver ativo, interrompe antes de qualquer alteração destrutiva.
SELECT EXISTS (
  SELECT 1
  FROM pg_replication_slots
  WHERE slot_name = 'payments_slot'
    AND active_pid IS NOT NULL
) AS payments_slot_active
\gset

\if :payments_slot_active
  \echo 'ERRO: payments_slot está ativo. Pare/exclua o connector CDC na Confluent e repita a limpeza.'
  \quit 5
\endif

-- Slot ausente é aceito; slot existente só é removido quando inativo.
SELECT pg_drop_replication_slot('payments_slot')
WHERE EXISTS (
  SELECT 1
  FROM pg_replication_slots
  WHERE slot_name = 'payments_slot'
    AND active_pid IS NULL
);

DROP PUBLICATION IF EXISTS payments_pub;

-- Remove o default privilege específico concedido à role CDC pelo init.
SELECT format('ALTER DEFAULT PRIVILEGES IN SCHEMA payments REVOKE SELECT ON TABLES FROM %I', :'cdc_role')
WHERE EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'payments')
  AND EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'cdc_role')
\gexec

DROP SCHEMA IF EXISTS payments CASCADE;
DROP TABLE IF EXISTS public.fraud_alerts;

-- Revoga somente privilégios conhecidos concedidos pelo laboratório.
-- Não usa DROP OWNED/CASCADE: dependências adicionais devem bloquear DROP ROLE.
SELECT format('REVOKE ALL PRIVILEGES ON DATABASE %I FROM %I', current_database(), :'cdc_role')
WHERE EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'cdc_role')
\gexec
SELECT format('REVOKE ALL PRIVILEGES ON DATABASE %I FROM %I', current_database(), :'sink_role')
WHERE EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'sink_role')
\gexec
SELECT format('REVOKE USAGE ON SCHEMA public FROM %I', :'sink_role')
WHERE EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'sink_role')
\gexec

SELECT format('DROP ROLE %I', rolname)
FROM pg_roles
WHERE rolname IN (:'cdc_role', :'sink_role')
  AND rolname <> current_user
\gexec

-- A limpeza só termina com sucesso se todos os alvos estiverem ausentes.
SELECT
  NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'payments')
  AND NOT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relname = 'fraud_alerts')
  AND NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'payments_pub')
  AND NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'payments_slot')
  AND NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'cdc_role')
  AND NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'sink_role')
  AS cleanup_verified
\gset

\if :cleanup_verified
  \echo 'Verificação final aprovada: todos os objetos e roles do laboratório estão ausentes.'
\else
  \echo 'ERRO: a verificação final encontrou objetos ou roles ainda existentes.'
  \quit 4
\endif

SELECT
  EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'payments') AS schema_payments_exists,
  EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relname = 'fraud_alerts') AS fraud_alerts_exists,
  EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'payments_pub') AS publication_exists,
  EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'payments_slot') AS replication_slot_exists,
  EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'cdc_role') AS cdc_role_exists,
  EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'sink_role') AS sink_role_exists;
