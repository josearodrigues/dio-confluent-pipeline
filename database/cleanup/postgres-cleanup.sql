-- DESTRUTIVO: use somente no banco dedicado ao laboratório.
SELECT pg_terminate_backend(active_pid)
FROM pg_replication_slots
WHERE slot_name='payments_slot' AND active_pid IS NOT NULL;
SELECT pg_sleep(2);
SELECT pg_drop_replication_slot('payments_slot')
WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='payments_slot');
DROP PUBLICATION IF EXISTS payments_pub;
DROP SCHEMA IF EXISTS payments CASCADE;
