
CREATE or replace FUNCTION clear_tempq() 
RETURNS void
LANGUAGE plpgsql
AS $$
begin
	delete from mbus.trigger where dst like 'temp.%' and not exists (select * from pg_stat_activity where dst like 'temp.' || md5(pid::text || backend_start::text) || '%');
	delete from mbus.tempq where not exists (select * from pg_stat_activity where (headers->'tempq') is null and (headers->'tempq') like 'temp.' || md5(pid::text || backend_start::text) || '%');
end;
$$;



CREATE or replace FUNCTION create_temporary_queue() RETURNS text
    LANGUAGE sql
    AS $$
  select 'temp.' || md5(pid::text || backend_start::text) || '.' || txid_current() from pg_stat_activity where pid=pg_backend_pid();
$$;



