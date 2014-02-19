update pg_catalog.pg_extension set extconfig=t.config, extcondition=t.condition
  from (
    select array_agg((schemaname || '.' || tablename)::regclass::oid) as config,
           array_agg(''::text) as condition
      from pg_catalog.pg_tables where schemaname='mbus' and tablename in ('qt_model','consumer','dmq','queue','trigger')
    ) as t
where extname='mbus';

alter extension mbus drop sequence mbus.seq;
alter extension mbus drop sequence mbus.qt_model_id_seq;
alter extension mbus drop sequence mbus.consumer_id_seq;
alter extension mbus drop sequence mbus.queue_id_seq;
grant usage on schema mbus to public;