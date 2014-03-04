update pg_catalog.pg_extension set extconfig=t.config, extcondition=t.condition
  from (
    select array_agg((schemaname || '.' || tablename)::regclass::oid) as config,
           array_agg(''::text) as condition
      from pg_catalog.pg_tables where schemaname='mbus' and tablename in ('qt_model','consumer','dmq','queue','trigger')
    ) as t
where extname='mbus';

do $code$
declare
 r record;
 cons record;
 queue text;
 matches text[];
 begin
     for r in (select substring(table_name from $RE$qt\$(.*)$RE$) as qname from information_schema.tables where table_schema='mbus' and table_name like 'qt$%') loop
      -- raise notice 'queue=%', r.qname;
      if not exists(select * from mbus.queue q where q.qname=r.qname) then
        insert into mbus.queue(qname, consumers_cnt) values(r.qname,128);
      end if;
      for cons in (select routine_definition as def, substring(ro.routine_name from '_by_(.*)') as cname 
                     from information_schema.routines ro 
                    where ro.specific_schema='mbus' and routine_name like 'consume_' || r.qname || '_by_%'
                  ) loop
                  raise notice ' consumer=%', cons.cname;
                  matches = regexp_matches(cons.def, 
                             $RE$lock_not_available .* 
                                 where \s+ (\d+)<>all\(received\) \s+ 
                                 and \s+ t.delayed_until<now\(\) \s+ and \s+ \((.*)\)=true \s+ 
                                 and \s+ added \s+ >\s+ '(\d\d\d\d-\d\d-\d\d \s \d\d:\d\d:\d\d\.\d\d\d) .*
                                 .* limit \s+ 1 \s+ for \s+ update; $RE$,'x');
                  -- raise notice ' id=%', matches[1];
                  -- raise notice ' selector=%', matches[2];
                  -- raise notice ' added=%', matches[3];
                  if not exists(select * from mbus.consumer c where c.qname=r.qname and c.name=cons.cname) then
                    insert into mbus.consumer(id, name, qname, selector,added) values(matches[1]::integer, cons.cname, r.qname, matches[2], matches[3]::timestamp);
                  end if;
      end loop;
     end loop;
     perform setval('mbus.queue_id_seq', (select max(id) from mbus.queue));
     perform setval('mbus.consumer_id_seq', (select max(id) from mbus.consumer));
    end;
$code$;

do $code$
begin
  begin
   alter extension mbus drop sequence mbus.seq;
  exception
   when sqlstate '55000' then null;
  end;

  begin
   alter extension mbus drop sequence mbus.qt_model_id_seq;
  exception
   when sqlstate '55000' then null;
  end;

  begin
   alter extension mbus drop sequence mbus.consumer_id_seq;
  exception
   when sqlstate '55000' then null;
  end;

  begin
   alter extension mbus drop sequence mbus.queue_id_seq;
  exception
   when sqlstate '55000' then null;
  end;

  grant usage on schema mbus to public;
end;
$code$;