SELECT pg_catalog.pg_extension_config_dump('qt_model', '');
SELECT pg_catalog.pg_extension_config_dump('consumer', '');
SELECT pg_catalog.pg_extension_config_dump('dmq', '');
SELECT pg_catalog.pg_extension_config_dump('queue', '');
SELECT pg_catalog.pg_extension_config_dump('trigger', '');

alter extension mbus drop sequence seq;
alter extension mbus drop sequence qt_model_id_seq;
alter extension mbus drop sequence consumer_id_seq;
alter extension mbus drop sequence queue_id_seq;
grant usage on schema mbus to public;