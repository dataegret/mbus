-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION mbus" to load this file. \quit

SET lc_messages to 'en_US.UTF-8';

CREATE SEQUENCE seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 100;

CREATE SEQUENCE qt_model_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CYCLE
    CACHE 50;



CREATE TABLE qt_model (
	id bigint NOT NULL,
	added timestamp without time zone NOT NULL,
	iid text NOT NULL,
	delayed_until timestamp without time zone NOT NULL,
	expires timestamp without time zone,
	received integer[],
	headers hstore,
	properties hstore,
	data hstore
);

CREATE TABLE consumer (
    id integer NOT NULL,
    name text,
    qname text,
    selector text,
    added timestamp without time zone
--    constraint consumer_pkey PRIMARY KEY(id)
);



CREATE SEQUENCE consumer_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


CREATE TABLE dmq (
    id integer DEFAULT nextval('qt_model_id_seq'::regclass) NOT NULL,
    added timestamp without time zone NOT NULL,
    iid text NOT NULL,
    delayed_until timestamp without time zone NOT NULL,
    expires timestamp without time zone,
    received integer[],
    headers hstore,
    properties hstore,
    data hstore
);

CREATE TABLE queue (
    id integer NOT NULL,
    qname text NOT NULL,
    consumers_cnt integer,
    is_roles_security_model boolean
);



CREATE SEQUENCE queue_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


CREATE TABLE tempq (
    id integer DEFAULT nextval('qt_model_id_seq'::regclass) NOT NULL,
    added timestamp without time zone NOT NULL,
    iid text NOT NULL,
    delayed_until timestamp without time zone NOT NULL,
    expires timestamp without time zone,
    received integer[],
    headers hstore,
    properties hstore,
    data hstore
);



CREATE TABLE trigger (
    src text NOT NULL,
    dst text NOT NULL,
    selector text
);




ALTER TABLE ONLY consumer ALTER COLUMN id SET DEFAULT nextval('consumer_id_seq'::regclass);



ALTER TABLE ONLY qt_model ALTER COLUMN id SET DEFAULT nextval('qt_model_id_seq'::regclass);



ALTER TABLE ONLY queue ALTER COLUMN id SET DEFAULT nextval('queue_id_seq'::regclass);



ALTER TABLE ONLY consumer
    ADD CONSTRAINT consumer_pkey PRIMARY KEY (id);



ALTER TABLE ONLY dmq
    ADD CONSTRAINT dmq_iid_key UNIQUE (iid);



ALTER TABLE ONLY qt_model
    ADD CONSTRAINT qt_model_iid_key UNIQUE (iid);



ALTER TABLE ONLY queue
    ADD CONSTRAINT queue_pkey PRIMARY KEY (id);



ALTER TABLE ONLY queue
    ADD CONSTRAINT queue_qname_key UNIQUE (qname);



ALTER TABLE ONLY tempq
    ADD CONSTRAINT tempq_iid_key UNIQUE (iid);



ALTER TABLE ONLY trigger
    ADD CONSTRAINT trigger_src_dst UNIQUE (src, dst);



CREATE INDEX tempq_name_added ON tempq USING btree (((headers -> 'tempq'::text)), added) WHERE ((headers -> 'tempq'::text) IS NOT NULL);

create or replace function owner_set(c_uname text default NULL) returns void as
$code$
declare 
	r_role RECORD;
begin
	if c_uname is not null then
		alter function mbus.create_queue(qname text, consumers_cnt integer, is_roles_security_model boolean) SECURITY DEFINER;
		alter function mbus.peek(msgiid text) SECURITY DEFINER;
		alter function mbus.create_consumer(cname text, qname text, p_selector text, noindex boolean) SECURITY DEFINER;
		alter function mbus.drop_queue(qname text) SECURITY DEFINER;
		alter function mbus.drop_consumer(cname text,qname text) SECURITY DEFINER;
		for r_role in select * from pg_catalog.pg_roles where rolsuper = false loop
			execute 'revoke EXECUTE on  function mbus.create_queue(qname text, consumers_cnt integer, is_roles_security_model boolean) from ' || r_role.rolname;
			execute 'revoke EXECUTE on  function mbus.peek(msgiid text) from ' || r_role.rolname;
			execute 'revoke EXECUTE on  function mbus.create_consumer(cname text, qname text, p_selector text, noindex boolean) from ' || r_role.rolname;
			execute 'revoke EXECUTE on  function mbus.drop_queue(qname text) from ' || r_role.rolname;
			execute 'revoke EXECUTE on  function mbus.drop_consumer(cname text,qname text) from ' || r_role.rolname;
		end loop;
		execute 'grant EXECUTE on  function mbus.create_queue(qname text, consumers_cnt integer, is_roles_security_model boolean) to ' || c_uname;
		execute 'grant EXECUTE on  function mbus.peek(msgiid text) to ' || c_uname;
		execute 'grant EXECUTE on  function mbus.create_consumer(cname text, qname text, p_selector text, noindex boolean) to ' || c_uname;
		execute 'grant EXECUTE on  function mbus.drop_queue(qname text) to ' || c_uname;
		execute 'grant EXECUTE on  function mbus.drop_consumer(cname text,qname text) to ' || c_uname;
	else
		alter function mbus.create_queue(qname text, consumers_cnt integer, is_roles_security_model boolean) SECURITY invoker;
		alter function mbus.peek(msgiid text) SECURITY invoker;
		alter function mbus.create_consumer(cname text, qname text, p_selector text, noindex boolean) SECURITY invoker;
		alter function mbus.drop_queue(qname text) SECURITY invoker;
		alter function mbus.drop_consumer(cname text,qname text) SECURITY invoker;
	end if;
end;
$code$
language plpgsql;

create or replace function raise_exception(exc text) returns void as
$code$
 begin
  raise exception '%', exc;
 end;
$code$
language plpgsql;

create or replace function mbus.is_roles_security_model(qname text) returns boolean as
$code$
 select is_roles_security_model from mbus.queue q where q.qname=is_roles_security_model.qname;
$code$
language sql
security definer set search_path = mbus, pg_temp, public;

create or replace function _is_superuser() returns boolean as
$code$
/*
do $testcode$
-->>>Run it as superuser
 declare
  rootconn text:='host=localhost port=5432 dbname=wrk user=postgres password=postgres';
  conn text:='host=localhost port=5432 dbname=wrk user=qqqzzz password=zzzqqq';
 begin
   if not mbus._is_superuser() then
     raise exception 'Not superuser';     
   end if;

   perform dblink_exec(rootconn, $QQ$drop role if exists qqqzzz$QQ$);
   perform dblink_exec(rootconn, $QQ$create role qqqzzz with unencrypted password 'zzzqqq' login$QQ$);

   perform * from dblink(rootconn, $QQ$select mbus.create_queue('zzzxxxxq',12);$QQ$) as (r text);
   perform dblink_exec(rootconn, 'grant mbus_' || current_database() || '_admin_zzzxxxxq to qqqzzz with admin option');

   perform dblink_exec(rootconn, $QQ$drop role if exists qqqzzz2$QQ$);
   perform dblink_exec(rootconn, $QQ$create role qqqzzz2 with unencrypted password 'zzzqqq' login$QQ$);
   perform dblink_exec(rootconn, $QQ$grant mbus_$QQ$ || current_database() || $QQ$_admin to qqqzzz$QQ$);
   perform dblink_exec(conn,
                     $DBLINKEXEC$
      do $remote$
        begin          
           grant mbus_$DBLINKEXEC$ || current_database() || $DBLINKEXEC$_consume_zzzxxxxq_by_default to qqqzzz2;
           raise sqlstate 'RB999';
          exception 
            when sqlstate 'RB999' then null;
        end;
      $remote$;
    $DBLINKEXEC$
  );
   perform dblink_exec(rootconn, $QQ$drop role qqqzzz$QQ$);
   perform dblink_exec(rootconn, $QQ$drop role qqqzzz2$QQ$);
 end;
--<<<
$testcode$;
*/
   select rolsuper from pg_catalog.pg_roles where rolname=session_user;
$code$
language sql;

create or replace function mbus._is_mbus_admin() returns boolean as
$code$
/*
-->>>Run it as mbus_admin
begin
   perform mbus.create_queue('zzqqq1',128);
   if not mbus._is_mbus_admin() then
     raise exception 'Not admin';     
   end if;
end;
--<<<
*/
   select mbus._is_superuser() or pg_has_role(session_user,'mbus_' || current_database() || '_admin','usage');
$code$
language sql;


create or replace function mbus.can_consume(qname text, cname text default 'default') returns boolean as
$code$
/*
-->>>Run it as superuser
begin
    perform mbus.create_queue('q1',128);
    if not mbus.can_consume('q1') then
     raise exception 'Superuser cannot consume';
    end if;
end;
--<<<
*/
   select not mbus.is_roles_security_model(qname)
          or mbus._is_mbus_admin()
          or pg_has_role(session_user,'mbus_' || current_database() || '_consume_' || $1 || '_by_' || $2,'usage')::boolean
          or pg_has_role(session_user,'mbus_' || current_database() || '_admin_' || $1,'usage')::boolean;
$code$
language sql;

create or replace function can_post(qname text) returns boolean as
$code$
/*
-->>>Run it as superuser
begin
    perform mbus.create_queue('q1',128);
    if not mbus.can_post('q1') then
     raise exception 'Superuser cannot post';
    end if;
end;
--<<<
*/
   select not mbus.is_roles_security_model(qname)
          or mbus._is_mbus_admin()
          or pg_has_role(session_user,'mbus_' || current_database() || '_post_' || $1,'usage')::boolean
          or pg_has_role(session_user,'mbus_' || current_database() || '_admin_' || $1,'usage')::boolean;
$code$
language sql;


create or replace function mbus._should_be_able_to_consume(qname text, cname text default 'default') returns void as
$code$
/*
-->>>Create ordinal queue
begin
  perform mbus.create_queue('qxz1',128);
  drop role if exists atestrole;
  create role atestrole login;
  set local session authorization atestrole;
  if has_table_privilege('mbus.qt$qxz1','SELECT') then
    raise exception 'Newly added user already can consume!';
  end if;
  reset session_authorization;
 end;
--<<<

do $code2$
-->>>Create role-based queue
declare
 failed_but_ok boolean;
begin
  perform mbus.create_queue('qzx2',128, true);
  perform mbus._should_be_able_to_consume('qzx2');
  set local session authorization atestrole;

  failed_but_ok:=false;
  begin
    perform mbus._should_be_able_to_consume('qzx2');
  exception 
    when others then 
      if sqlerrm like '%denied%' then
        failed_but_ok:=true;
      end if;
  end;
  if not failed_but_ok then
    raise exception 'Newly added user already can consume!';
  end if;
  reset session_authorization;
  execute 'grant mbus_' || current_database() || '_consume_qzx2_by_default to atestrole';

  set local session authorization atestrole;
  perform mbus._should_be_able_to_consume('qzx2');
end;
--<<<
$code2$
*/
begin
  if not mbus.can_consume(qname, cname) then
    raise exception 'Access denied';
  end if;
end;
$code$
language plpgsql;

create or replace function mbus._should_be_owner_or_root() returns boolean as
$code$
/*
-->>>_should_be_owner_or_root
begin
  perform mbus.create_queue('qxz1',128);
  perform mbus._should_be_owner_or_root();
end;
--<<<
*/
  select mbus._is_mbus_admin()
    or exists(select schema_owner from information_schema.schemata where schema_name='mbus' and schema_owner=session_user) 
    or mbus.raise_exception('Access denied') is not null;
$code$
language sql;

create or replace function mbus._should_be_authorized(qname text) returns boolean as
$code$
/*
-->>>_should_be_authorized
begin
  perform mbus.create_queue('qxz1',128);
  perform mbus._should_be_authorized('qxz1');
end;
--<<<
*/
  select mbus._is_superuser() 
    or exists(select schema_owner from information_schema.schemata where schema_name='mbus' and schema_owner=session_user) or 
    pg_has_role(session_user, 'mbus_' || current_database() || '_admin_' || qname, 'usage')
    or mbus.raise_exception('Access denied') is not null;
$code$
language sql;

  
create or replace function mbus._should_be_able_to_post(qname text) returns boolean as
$code$
/*
-->>>_should_be_able_to_post
begin
  perform mbus.create_queue('qxz1',128);
  create role atestrole2 login;
  set local session authorization atestrole2;
  if has_table_privilege('mbus.qt$qxz1','SELECT') then
    raise exception 'Newly added user already can consume!';
  end if;
  reset session_authorization;
 end;
--<<<

do $code2$
-->>>Create role-based queue
declare
 failed_but_ok boolean;
begin
  perform mbus.create_queue('qzx2',128, true);
  perform mbus._should_be_able_to_post('qzx2');
  set local session authorization atestrole2;

  failed_but_ok:=false;
  begin
    perform mbus._should_be_able_to_post('qzx2');
  exception 
    when others then 
      if sqlerrm like '%denied%' then
        failed_but_ok:=true;
      end if;
  end;
  if not failed_but_ok then
    raise exception 'Newly added user already can post!';
  end if;
  reset session_authorization;
  execute 'grant mbus_' || current_catalog || '_consume_qzx2_by_default to atestrole2';

  set local session authorization atestrole2;
  perform mbus._should_be_able_to_consume('qzx2');
end;
--<<<
$code2$
*/
  select mbus._is_superuser() or mbus.can_post(qname) or mbus.raise_exception('Access denied') is not null;
$code$
language sql;


create or replace function mbus._create_consumer_role(qname text, cname text) returns void as
$code$
/*
-->>>Check for roles after queue creation
 declare
  fail_but_ok boolean;
 begin
  perform mbus.create_queue('qzqn1',32);
  if not exists(select * from information_schema.enabled_roles where role_name='mbus_' || current_catalog || '_post_qzqn1') then
    raise exception 'Expected role % does not found', 'mbus_' || current_catalog || '_post_qzqn1';
  end if;

  if not exists(select * from information_schema.enabled_roles where role_name='mbus_' || current_catalog || '_consume_qzqn1_by_default') then
    raise exception 'Expected role % does not found', 'mbus_' || current_catalog || '_consume_q1_by_default';
  end if;
  fail_but_ok:=false;
  begin
     perform mbus.create_queue('qzqn-1',32);
  exception
   when others then
     if sqlerrm like '%'||'qzqn-1'||'%' then
       fail_but_ok:=true;
     end if;
  end;
  if not fail_but_ok then
    raise exception 'Can create queue with wrong name';
  end if;
 end;
--<<<

do $testcode$
-->>>Run 
 declare
  conn text:='host=localhost port=5432 dbname=wrk user=%<login> password=%<password>';
 begin
   create role qzzq unencrypted password 'lala' login;
   create role qzzq2 unencrypted password 'lala' login;
   perform mbus.create_queue('qqzzxx',12);
   execute 'create role mbus_' || current_database() || '_admin_qqzzxx';
   execute 'grant mbus_' || current_database() || '_admin_qqzzxx to qzzq';
   dblink_exec(conn, mbus.string_format($REMOTE$
     begin
       grant mbus_%<curdb>_qqzzxx to qzzq2;
     end;
   $REMOTE$, hstore(array['login', 'qzzq', 'password', 'lala', 'curdb', current_database()]);
 end;
$testcode$
*/
begin
  if cname ~ $RE$\W$RE$ or length(cname)>32 then
      raise exception 'Wrong consumer name:%', cname;
  end if;

  if not exists(select * from mbus.queue q where q.qname=_create_consumer_role.qname) then
      raise exception 'Queue % does not exist';
  end if;

  begin
     execute 'create role mbus_' || current_database() || '_consume_' || qname || '_by_' || cname;
  exception
   when sqlstate '42710' -- "role already exists
      then 
       raise notice 'Role % already exists', cname;
  end;
  execute 'grant mbus_' || current_database() || '_consume_' || qname || '_by_' || cname || ' to mbus_' || current_database() || '_admin_' || qname || ' with admin option';
  execute 'grant mbus_' || current_database() || '_consume_' || qname || '_by_' || cname || ' to mbus_' || current_database() || '_admin with admin option';
end;
$code$
language plpgsql;

create or replace function mbus._create_queue_role(qname text) returns void as
$code$
/*

*/
begin
  if qname ~ $RE$\W$RE$ or length(qname)>32 then
      raise exception 'Wrong queue name:%', qname;
  end if;

  begin
    execute 'create role mbus_' || current_database() || '_post_' || qname;
  exception
   when sqlstate '42710' -- "role already exists
      then 
       raise notice 'Role mbus_post_% already exists', qname;
  end;

  begin
    execute 'create role mbus_' || current_database() || '_admin_' || qname;
  exception
   when sqlstate '42710' -- "role already exists
      then 
       raise notice 'Role mbus_post_% already exists', qname;
  end;
  execute 'grant mbus_' || current_database() || '_post_' || qname || ' to mbus_' || current_database() || '_admin_' || qname || ' with admin option';
  execute 'grant mbus_' || current_database() || '_post_' || qname || ' to mbus_' || current_database() || '_admin with admin option';
end;
$code$
language plpgsql;


create or replace function mbus._drop_consumer_role(qname text, cname text) returns void as
$code$
/*
do $thetest$
-->>>Check for roles after queue creation
 declare
  fail_but_ok boolean;
  errm text;
 begin
  perform mbus.create_queue('qzqn1',32);
  if not exists(select * from information_schema.enabled_roles where role_name='mbus_' || current_catalog || '_post_qzqn1') then
    raise exception 'Expected role % does not found', 'mbus_' || current_catalog || '_post_qzqn1';
  end if;

  if not exists(select * from information_schema.enabled_roles where role_name='mbus_' || current_catalog || '_consume_qzqn1_by_default') then
    raise exception 'Expected role % does not found', 'mbus_' || current_catalog || '_consume_q1_by_default';
  end if;
  fail_but_ok:=false;
end;
--<<<
$thetest$;

do $thetest$
-->>>test actual role drops
 begin
  begin
    perform mbus._drop_consumer_role('qzqn1','default');
    raise sqlstate 'RB999';
  exception
    when sqlstate 'RB999' then null;
    when others then raise;
  end;
 end;
--<<<
$thetest$;
*/
begin
  if cname ~ $RE$\W$RE$ or length(cname)>32 then
      raise exception 'Wrong consumer name:%', cname;
  end if;
  execute 'drop role if exists mbus_' || current_database() || '_consume_' || qname || '_by_' || cname;
end;
$code$
language plpgsql;

create or replace function mbus._drop_queue_role(qname text) returns void as
$code$
/*
do $thetest$
-->>>Check for roles after queue creation
 declare
  fail_but_ok boolean;
  errm text;
 begin
  perform mbus.create_queue('qzqn1',32);
  if not exists(select * from information_schema.enabled_roles where role_name='mbus_' || current_catalog || '_post_qzqn1') then
    raise exception 'Expected role % does not found', 'mbus_' || current_catalog || '_post_qzqn1';
  end if;

  if not exists(select * from information_schema.enabled_roles where role_name='mbus_' || current_catalog || '_consume_qzqn1_by_default') then
    raise exception 'Expected role % does not found', 'mbus_' || current_catalog || '_consume_q1_by_default';
  end if;

 end;
--<<<

-->>>test actual role drops
 begin
  perform mbus._drop_queue_role('qzqn1');
  if exists(select * from information_schema.enabled_roles where role_name='mbus_' || current_catalog || '_post_qzqn1') then
    raise exception 'Unexpected role % have been found', 'mbus_' || current_catalog || '_post_qzqn1';
  end if;

  if exists(select * from information_schema.enabled_roles where role_name='mbus_' || current_catalog || '_consume_qzqn1_by_default') then
    raise exception 'Unexpected role % have been found', 'mbus_' || current_catalog || '_consume_q1_by_default';
  end if;
 end;
--<<<
$thetest$;
*/

declare
 r record;
begin
  execute 'drop role if exists mbus_' || current_database() || '_post_' || qname;
  execute 'drop role if exists mbus_' || current_database() || '_admin_' || qname;

  for r in select * from mbus.consumer c where c.qname=_drop_queue_role.qname loop
    begin
        execute 'drop role mbus_' || current_database() || '_consume_' || qname || '_by_' || r.name;
    exception
      when sqlstate '42704' then raise notice 'Role % does not exists', r.name; --role doesn't exist
    end;
  end loop;
end;
$code$
language plpgsql;



CREATE OR REPLACE FUNCTION quote_html(s text)
  RETURNS text AS
$BODY$
 select replace(replace(replace(replace($1,'"','&quot;'),'&','&amp;'),'<','&lt;'),'>','&gt;')
$BODY$
  LANGUAGE sql IMMUTABLE
  COST 100;


CREATE OR REPLACE FUNCTION string_format(format text, param hstore)
  RETURNS text AS
$BODY$
/*
Форматирует строку в соответствии с заданным шаблоном
%"name" || || %{name} - quote_ident
%'name' || %[name] - вставляется quote_literal(param->'name')
%<var> - as is
%#html# - quote_html
%[-!@#$^&*=+] - т.е. %-, %!, %@, %#, %$, %^, %&, &* - то as is из param->'-', param->'!' и т.п.
explain analyze
select string_format($$%"name" == %{name} is %[value] == %'value' and n=%<n> and name=%'name' %%<v> and html=%#amp# and %#$$, hstore('name','la la') || hstore('value', 'The Value')||hstore('n',n::text)||hstore('amp','<a href="lakak">')||hstore('#','hash sign'))
  from generate_series(1,100) as gs(n);

 -->>>string_format
 declare
   testtable text[][]:=array[
     array[$STR$test$STR$,$STR$test=>test$STR$,$STR$test$STR$],
     array[$STR$%"username"$STR$,$STR$username=>"user name"$STR$,$STR$"user name"$STR$],
     array[$STR$%{username}$STR$,$STR$username=>"user name"$STR$,$STR$"user name"$STR$],
     array[$STR$Q%{username}$STR$,$STR$username=>"user name"$STR$,$STR$Q"user name"$STR$],
     array[$STR$Q%{username}Q$STR$,$STR$username=>"user name"$STR$,$STR$Q"user name"Q$STR$],
     array[$STR$%{username}Q$STR$,$STR$username=>"user name"$STR$,$STR$"user name"Q$STR$],
     array[$STR$%{username}Q$STR$,$STR$username=>"username"$STR$,$STR$usernameQ$STR$],
     
     array[$STR$%'username'$STR$,$STR$username=>"username"$STR$,$STR$'username'$STR$],
     array[$STR$%[username]Q$STR$,$STR$username=>"username"$STR$,$STR$'username'Q$STR$],
     
     array[$STR$%<username>$STR$,$STR$username=>"user name"$STR$,$STR$user name$STR$],
     array[$STR$--%<username>--Q$STR$,$STR$username=>">user name<"$STR$,$STR$-->user name<--Q$STR$],

     array[$STR$%<username>Q$STR$,$STR$username=>"user name"$STR$,$STR$user nameQ$STR$],
     array[$STR$%#gt#%#lt#%#amp#Q$STR$,$STR$gt=>">",lt=>"<",amp=>"&"$STR$,$STR$&gt;&lt;&amp;Q$STR$]
   ];
   i integer;
   res text;
  begin
   for i in 1..array_upper(testtable,1) loop
     res:=mbus.string_format(testtable[i][1],testtable[i][2]::hstore);
     if res <> testtable[i][3] then
       raise exception '%', 'Format ' || testtable[i][1] || ', params are:' || testtable[i][2] || ', got ' ||coalesce(res,'<NULL>') || ', expected ' || testtable[i][3];
     end if;
   end loop;
  end;
 --<<<
*/
select
  string_agg(
         case when s[1] like '^%"%"' escape '^' or s[1] like '^%{%}' escape '^'
                then coalesce(quote_ident($2->(substr(s[1],3,length(s[1])-3))),quote_ident(''))
              when s[1] like $$^%'%'$$ escape '^' or s[1] like $$^%[%]$$ escape '^'
                then coalesce(quote_literal($2->(substr(s[1],3,length(s[1])-3))),quote_literal(''))
              when s[1] like '^%<%>' escape '^'
                then coalesce($2->(substr(s[1],3,length(s[1])-3)),'')
              when s[1] like '^%#%#' escape '^'
                then coalesce(mbus.quote_html(($2->(substr(s[1],3,length(s[1])-3)))),'')
              when s[1] ~ '%[-!@#$^&*=+]'
                then coalesce($2->(substr(s[1],2,1)),'')
              when s[1]='%%'
                then '%'
              else
               s[1]
         end,'') as s
      from regexp_matches($1,
                         $RE$
                         (%%
                          |
                          %[$@]
                          |
                          % ([['"<{#]) [$@\w]+ ([]"'>}#])
                          |
                          %[-!@#$^&*=+]
                          |
                          (?: [^%]+ | %(?! [<{'"] ) )
                         )
                         $RE$,
                         'gx')
                         as re(s);
$BODY$
  LANGUAGE sql IMMUTABLE
  COST 100;

--' - single quote here is just for correct text highlight of colororer


CREATE FUNCTION clear_tempq() 
RETURNS void
LANGUAGE plpgsql
AS $$
/*
-->>>Test for ability to just run
 declare
  tqid text:=mbus.create_temporary_queue();
  begin
    perform mbus.clear_tempq();
  end;
--<<<
*/
begin
    perform mbus._should_be_owner_or_root();
	delete from mbus.trigger where dst like 'temp.%' and not exists (select * from pg_stat_activity where dst like 'temp.' || md5(pid::text || backend_start::text) || '%');
	delete from mbus.tempq where not exists (select * from pg_stat_activity where (headers->'tempq') like 'temp.' || md5(pid::text || backend_start::text) || '.%');
end;
$$
security definer set search_path = mbus, pg_temp, public;

CREATE FUNCTION consume(qname text, cname text DEFAULT 'default'::text) 
RETURNS SETOF qt_model
LANGUAGE plpgsql
AS $_$
begin
	if qname like 'temp.%' then
        	return query select * from mbus.consume_temp(qname);
            	return;
        end if;
        raise exception 'No queues were defined';
end;
$_$;



CREATE FUNCTION consume_temp(tqname text) 
RETURNS SETOF qt_model
LANGUAGE plpgsql
AS $$
/*
-->>>consume temporary queue
 declare
  tqid text:=mbus.create_temporary_queue();
  rv mbus.qt_model;
  begin
    perform mbus.create_queue('junk',32);
    perform mbus.post(tqid,'key=>val12'::hstore);
    select * into rv from mbus.consume(tqid);
    if not found or (rv.data->'key') is distinct from 'val12' then
      raise exception 'Cannot consume expected message from temp q: got %', rv;
    end if;
  end;
--<<<
*/
declare
	rv mbus.qt_model;
begin
	select * into rv from mbus.tempq where (headers->'tempq')=tqname and coalesce(expires,'2070-01-01'::timestamp)>now()::timestamp and coalesce(delayed_until,'1970-01-01'::timestamp)<now()::timestamp order by added limit 1;
	if rv.id is not null then
    		delete from mbus.tempq where iid=rv.iid;
    		return next rv;
 	end if;   
	return;
end;
$$;


CREATE FUNCTION create_consumer(cname text, qname text, p_selector text DEFAULT NULL::text, noindex boolean DEFAULT false)
RETURNS void
LANGUAGE plpgsql
security definer set search_path = mbus, pg_temp, public
AS $_$
<<code>>
/*
-->>>create_consumer
 declare
  iid text;
  rv mbus.qt_model;
 begin
   perform mbus.create_queue('zzqqx',12);
   perform mbus.create_consumer('zqqzz','zzqqx');
   iid:=mbus.post('zzqqx',hstore('key','value'));
   rv:=mbus.peek(iid);
   rv:=mbus.take(iid);
   if (rv.data->'key')<>'value' then
     raise exception 'Cannot take!';
   end if;
 end;
--<<<
*/
declare
	c_id integer;
	cons_src text;
	consn_src text;
	take_src text;
	ind_src text;
	selector text;
	nowtime text:=(now()::text)::timestamp without time zone;
	is_roles_security_model boolean;
begin
        perform mbus._should_be_owner_or_root();
	selector := case when p_selector is null or p_selector ~ '^ *$' then '1=1' else p_selector end;
        if not exists(select * from mbus.queue q where q.qname=create_consumer.qname) then
	    raise exception 'Wrong queue name:%', create_consumer.qname;
	end if;

	if exists(select * from mbus.consumer q where q.qname=create_consumer.qname and q.name=create_consumer.cname and q.selector is distinct from code.selector) then
	    raise exception 'Consumer with identical name (%) and different selector already exists!', cname;
	end if;
 	
        if not exists(select * from mbus.consumer q where q.qname=create_consumer.qname and q.name=create_consumer.cname) then
    	   insert into mbus.consumer(name, qname, selector, added) values(cname, qname, selector, now()::timestamp without time zone) returning id into c_id;
    	else
    	   select id, added into c_id, nowtime from mbus.consumer c where c.name=cname and c.qname=create_consumer.qname;
    	end if;
 	perform mbus._create_consumer_role(qname, cname);

 	cons_src:=$CONS_SRC$
	----------------------------------------------------------------------------------------
	CREATE OR REPLACE FUNCTION mbus.consume_<!qname!>_by_<!cname!>()
  	RETURNS SETOF mbus.qt_model AS
	$DDD$
	declare
 		rv mbus.qt_model;
 		c_id int;
 		pn int;
 		cnt int;
 		r record;
 		gotrecord boolean:=false;
	begin
 		<!should_be_able_to_consume!>

  		if version() like 'PostgreSQL 9.0%' then
     			for r in 
      				select * 
        				from mbus.qt$<!qname!> t
       					where <!consumer_id!><>all(received) and t.delayed_until<now() and (<!selector!>)=true and added >'<!now!>' and coalesce(expires,'2070-01-01'::timestamp) > now()::timestamp 
                                        and (not exist(t.headers,'consume_after') or (select every(not mbus.is_msg_exists(u.v)) from unnest( ((t.headers)->'consume_after')::text[]) as u(v)))
       					order by added, delayed_until
       				limit <!consumers!>
			loop
				begin
					select * into rv from mbus.qt$<!qname!> t where t.iid=r.iid and <!consumer_id!><>all(received) for update nowait;
					continue when not found;
					gotrecord:=true;
					exit;
				exception
					when lock_not_available then null;
				end;
			end loop;
  		else
      			select * 
        			into rv
        			from mbus.qt$<!qname!> t
       				where <!consumer_id!><>all(received) and t.delayed_until<now() and (<!selector!>)=true and added > '<!now!>' and coalesce(expires,'2070-01-01'::timestamp) > now()::timestamp 
                                and ((t.headers->'consume_after') is null or (select every(not mbus.is_msg_exists(u.v)) from unnest( ((t.headers)->'consume_after')::text[]) as u(v)))
         			and pg_try_advisory_xact_lock( hashtext(t.iid))
       				order by added, delayed_until
       				limit 1
         		for update;
  		end if;


		if rv.iid is not null then 
			if mbus.build_<!qname!>_record_consumer_list(rv) <@ (rv.received || <!c_id!>) then
				delete from mbus.qt$<!qname!> where iid = rv.iid;
			else
				update mbus.qt$<!qname!> t set received=received || <!c_id!> where t.iid=rv.iid;
			end if;
			rv.headers = rv.headers || hstore('destination','<!qname!>');
			return next rv;
			return;
		end if; 
	end;
	$DDD$
	LANGUAGE plpgsql VOLATILE
	<!SECURITY_DEFINER!>
    SET enable_seqscan = off set enable_indexscan=on set enable_indexonlyscan=on set enable_sort = off
	$CONS_SRC$; 

	cons_src:=regexp_replace(cons_src,'<!qname!>', qname, 'g');
	cons_src:=regexp_replace(cons_src,'<!cname!>', cname,'g');
	cons_src:=regexp_replace(cons_src,'<!consumers!>', (select consumers_cnt::text from mbus.queue q where q.qname=create_consumer.qname),'g');
	cons_src:=regexp_replace(cons_src,'<!consumer_id!>',c_id::text,'g');
	cons_src:=regexp_replace(cons_src,'<!selector!>',selector,'g');
	cons_src:=regexp_replace(cons_src,'<!now!>',nowtime,'g');
	cons_src:=regexp_replace(cons_src,'<!c_id!>',c_id::text,'g');

	is_roles_security_model:=(select q.is_roles_security_model from mbus.queue q where q.qname=create_consumer.qname);
	cons_src:=regexp_replace(cons_src,'<!should_be_able_to_consume!>',
	                                   case when is_roles_security_model then $S$perform mbus._should_be_able_to_consume('$S$ || qname || $S$','$S$ || cname || $S$');$S$ else '--' end
	                        );
	cons_src:=regexp_replace(cons_src,'<!SECURITY_DEFINER!>',
	                                   case when is_roles_security_model then 'security definer set search_path = mbus, pg_temp, public ' else '' end
	                        );
	execute cons_src;

	consn_src:=$CONSN_SRC$
	----------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION mbus.consumen_<!qname!>_by_<!cname!>(amt integer)
RETURNS SETOF mbus.qt_model AS
$DDD$
declare
	rv mbus.qt_model;
	rvarr mbus.qt_model[];
	c_id int;
	pn int;
	cnt int;
	r record;
	inloop boolean;
	trycnt integer:=0;
begin
 	<!should_be_able_to_consume!>
	rvarr:=array[]::mbus.qt_model[];
 	if version() like 'PostgreSQL 9.0%' then  
   		while coalesce(array_length(rvarr,1),0)<amt loop
       		inloop:=false;
       		for r in select * 
                	from mbus.qt$<!qname!> t
                 	where <!c_id!><>all(received) 
			   and t.delayed_until<now() 
			   and (<!selector!>)=true 
			   and added > '<!now!>' 
			   and coalesce(expires,'2070-01-01'::timestamp) > now()::timestamp 
			   and t.iid not in (select a.iid from unnest(rvarr) as a)
                           and (not exist(t.headers,'consume_after') or (select every(not mbus.is_msg_exists(u.v)) from unnest( ((t.headers)->'consume_after')::text[]) as u(v)))
                 	order by added, delayed_until
                	limit amt
       		loop
          		inloop:=true;
          	begin
              		select * into rv from mbus.qt$<!qname!> t where t.iid=r.iid and <!c_id!><>all(received) for update nowait;
              		continue when not found;
              		rvarr:=rvarr||rv;
          	exception
              		when lock_not_available then null;
          	end;
       	end loop; 
       		exit when not inloop;
    	end loop;
  	else
    	rvarr:=array(select row(t.* )::mbus.qt_model
                   from mbus.qt$<!qname!> t
                  where <!c_id!><>all(received) 
                    and t.delayed_until<now() 
                    and (<!selector!>)=true 
                    and added > '<!now!>' 
                    and coalesce(expires,'2070-01-01'::timestamp) > now()::timestamp 
                    and (not exist(t.headers,'consume_after') or (select every(not mbus.is_msg_exists(u.v)) from unnest( ((t.headers)->'consume_after')::text[]) as u(v)))
                    and pg_try_advisory_xact_lock(hashtext(t.iid))
                  order by added, delayed_until
                  limit amt
                    for update
                );
  	end if;

	if array_length(rvarr,1)>0 then 
   		for rv in select * from unnest(rvarr) loop
    		if mbus.build_<!qname!>_record_consumer_list(rv) <@ (rv.received || <!c_id!>) then
      			delete from mbus.qt$<!qname!> where iid = rv.iid;
    		else
      		update mbus.qt$<!qname!> t set received=received || <!c_id!> where t.iid=rv.iid;
    		end if;
   		end loop; 
   		return query select id, added, iid, delayed_until, expires, received, headers || hstore('destination','<!qname!>') as headers, properties, data from unnest(rvarr);
   		return;
	end if; 
end;
$DDD$
LANGUAGE plpgsql VOLATILE
<!SECURITY_DEFINER!>
SET enable_seqscan = off set enable_indexscan=on set enable_indexonlyscan=on
$CONSN_SRC$; 

 consn_src:=regexp_replace(consn_src,'<!qname!>', qname, 'g');
 consn_src:=regexp_replace(consn_src,'<!cname!>', cname,'g');
 consn_src:=regexp_replace(consn_src,'<!consumers!>', (select consumers_cnt::text from mbus.queue q where q.qname=create_consumer.qname),'g');
 consn_src:=regexp_replace(consn_src,'<!consumer_id!>',c_id::text,'g');
 consn_src:=regexp_replace(consn_src,'<!selector!>',selector,'g');
 consn_src:=regexp_replace(consn_src,'<!now!>',nowtime,'g');
 consn_src:=regexp_replace(consn_src,'<!c_id!>',c_id::text,'g');

 is_roles_security_model:=(select q.is_roles_security_model from mbus.queue q where q.qname=create_consumer.qname);
 consn_src:=regexp_replace(consn_src,'<!should_be_able_to_consume!>',
                                    case when is_roles_security_model then $S$perform mbus._should_be_able_to_consume('$S$ || qname || $S$','$S$ || cname || $S$');$S$ else '--' end
                         );
 consn_src:=regexp_replace(consn_src,'<!SECURITY_DEFINER!>',
                                    case when is_roles_security_model then 'security definer set search_path = mbus, pg_temp, public ' else '' end
                         );

 execute consn_src;

 take_src:=$TAKE$
 create or replace function mbus.take_from_<!qname!>_by_<!cname!>(msgid text)
  returns mbus.qt_model as
  $PRC$
     <!should_be_able_to_consume!>
     update mbus.qt$<!qname!> t set received=received || <!consumer_id!> where iid=$1 and <!c_id!> <> ALL(received) returning *;
  $PRC$
  <!SECURITY_DEFINER!>
  language sql;
 $TAKE$;
 take_src:=regexp_replace(take_src,'<!qname!>', qname, 'g');
 take_src:=regexp_replace(take_src,'<!cname!>', cname,'g');
 take_src:=regexp_replace(take_src,'<!consumers!>', (select consumers_cnt::text from mbus.queue q where q.qname=create_consumer.qname),'g');
 take_src:=regexp_replace(take_src,'<!consumer_id!>',c_id::text,'g');
 take_src:=regexp_replace(take_src,'<!c_id!>',c_id::text,'g');
 take_src:=regexp_replace(take_src,'<!should_be_able_to_consume!>',
                                    case when is_roles_security_model then $S$select mbus._should_be_able_to_consume('$S$ || qname || $S$','$S$ || cname || $S$');$S$ else '--' end
                         );
 take_src:=regexp_replace(take_src,'<!SECURITY_DEFINER!>',
                                    case when is_roles_security_model then 'security definer set search_path = mbus, pg_temp, public ' else '' end
                         );

 execute take_src;
 

-- ind_src:= $IND$create index qt$<!qname!>_for_<!cname!> on mbus.qt$<!qname!>((id % <!consumers!>), id, delayed_until)  WHERE <!consumer_id!> <> ALL (received) and (<!selector!>)=true and added >'<!now!>'$IND$;
 ind_src:= $IND$create index qt$<!qname!>_for_<!cname!> on mbus.qt$<!qname!>(added, delayed_until)  WHERE <!consumer_id!> <> ALL (received) and (<!selector!>)=true and added >'<!now!>'$IND$;
 ind_src:=regexp_replace(ind_src,'<!qname!>', qname, 'g');
 ind_src:=regexp_replace(ind_src,'<!cname!>', cname,'g');
 ind_src:=regexp_replace(ind_src,'<!consumers!>', (select consumers_cnt::text from mbus.queue q where q.qname=create_consumer.qname),'g');
 ind_src:=regexp_replace(ind_src,'<!consumer_id!>',c_id::text,'g');
 ind_src:=regexp_replace(ind_src,'<!selector!>',selector,'g');
 ind_src:=regexp_replace(ind_src,'<!now!>',nowtime,'g');
 if noindex then
   raise notice '%', 'You must create index ' || ind_src;
 else
   execute ind_src;
 end if;  
 perform mbus.regenerate_functions(false);
end;
$_$;


CREATE FUNCTION mbus.create_queue(qname text, consumers_cnt integer, is_roles_security_model boolean default null) 
RETURNS void
security definer set search_path = mbus, pg_temp, public
LANGUAGE plpgsql
AS $_$
/*
do $testcode$
 -->>>Queue&consumer creation/drop and simple post in it; consume after; ensure roles would have been created
 declare
  h hstore;
  rv mbus.qt_model;
  failed_but_ok boolean;
 begin
  perform mbus.create_queue('qzqn1',128);
  if not exists(select * from information_schema.enabled_roles where role_name='mbus_' || current_catalog || '_post_qzqn1') then
    raise exception 'Expected role % does not found', 'mbus_' || current_catalog || '_post_qzqn1';
  end if;

  if not exists(select * from information_schema.enabled_roles where role_name='mbus_' || current_catalog || '_consume_qzqn1_by_default') then
    raise exception 'Expected role % does not found', 'mbus_' || current_catalog || '_consume_q1_by_default';
  end if;

  failed_but_ok:=false;
  begin
    perform mbus.create_consumer('ananotherconsumer','qzqn11');
    exception 
     when others then
      if sqlerrm like '%Wrong%queue%name%:%qzqn11%' then
        failed_but_ok:=true;
      end if;
  end;

  if not failed_but_ok then
    raise exception 'Can create subscriber to unexist queue';
  end if;

  perform mbus.create_consumer('ananotherconsumer','qzqn1');
  if not exists(select * from information_schema.enabled_roles where role_name='mbus_' || current_catalog || '_consume_qzqn1_by_ananotherconsumer') then
    raise exception 'Expected role % does not found', 'mbus_' || current_catalog || '_consume_qzqn1_by_ananotherconsumer';
  end if;

  perform mbus.post(qname:='qzqn1',data:=('key=>value12')::hstore, headers:=hstore('enqueue_time', (now()+'1s'::interval)::text));
  select * into rv from mbus.consume(qname:='qzqn1',cname:='ananotherconsumer');
  if not found or ((rv.data)->'key') is null or ((rv.data)->'key') <> 'value12' then
    raise exception 'cannot get posted message. Got: %', rv;
  end if;  

  failed_but_ok:=false;
  begin
    perform mbus.post(qname:='qzqn111',data:=('key=>value12')::hstore, headers:=hstore('enqueue_time', (now()+'1s'::interval)::text));
    exception 
     when others then
      if sqlerrm like '%queue%' then
        failed_but_ok:=true;
      end if;
  end;
  if not failed_but_ok then
    raise exception 'Can post to unexist queue';
  end if;

  select * into rv from mbus.consume(qname:='qzqn1');
  if not found or ((rv.data)->'key') is null or ((rv.data)->'key') <> 'value12' then
    raise exception 'cannot get posted message. Got: %', rv;
  end if;  

  failed_but_ok:=false;
  begin
    select * into rv from mbus.consume(qname:='qzqn1qqzz');
    exception 
     when others then
      if sqlerrm like '%Queue%' then
        failed_but_ok:=true;
      end if;
  end;
  if not failed_but_ok then
    raise exception 'Can consume from unexist queue';
  end if;

  select * into rv from mbus.consume(qname:='qzqn1',cname:='ananotherconsumer');
  if found then
    raise exception 'get duplicated message. Got: %', rv;
  end if;  

  failed_but_ok:=false;
  begin
    perform mbus.drop_queue('q1qqqzzxx');
    exception 
     when others then
      if sqlerrm like '%Queue%' then
        failed_but_ok:=true;
      end if;
  end;
  if not failed_but_ok then
    raise exception 'Can consume from unexist queue';
  end if;

  perform mbus.drop_queue('qzqn1');
  if exists(select * from information_schema.routines where routine_schema='mbus' and routine_name like '%_qzqn1_%') then
    raise exception 'Not all routies have been dropped due dropping of queue q1';
  end if;

 end;
 --<<<
 $testcode$;

 -->>>Ordering test
 declare
  iid text;
  rv mbus.qt_model;
 begin
   perform mbus.create_queue('zq_ordr',8);
   iid:=mbus.get_iid('zq_ordr');
   if iid is null then 
     raise exception 'Get null for iid';
   end if;
   perform mbus.post_zq_ordr(data:='k=>1', iid:=iid);
   perform mbus.post('zq_ordr',data:='k=>2', headers:=hstore('consume_after',array[iid]::text));
   select * into rv from mbus.consume_zq_ordr_by_default();
   if rv.data<>'k=>1'::hstore then
     raise notice 'Order has been destroyed';
   end if;

   select * into rv from mbus.consume_zq_ordr_by_default();
   if rv.data<>'k=>2'::hstore then
     raise notice 'Order has been destroyed';
   end if;

 end;
 --<<<

 -->>>Clear queue
 begin
   perform mbus.create_queue('zq_ordr',8);
   perform mbus.clear_queue_zq_ordr();
 end;
 --<<<
*/
declare
	schname text:= 'mbus';
	post_src text;
	clr_src text;
	peek_src text;
	is_roles_security_model boolean;
	is_clean_creation boolean := false;
begin
        perform mbus._should_be_owner_or_root();
	if length(qname)>32 or qname ~ $RE$\W$RE$ then
	   raise exception 'Name % too long (32 chars max) or contains illegal chars', qname;
	end if;
	--qname:=lower(qname);
	if not exists(select * from mbus.queue q where q.qname=create_queue.qname) then
  		execute 'create table ' || schname || '.qt$' || qname || '( like ' || schname || '.qt_model including all)';
		perform mbus._create_queue_role(qname);
		insert into mbus.queue(qname,consumers_cnt, is_roles_security_model) values(qname,consumers_cnt, create_queue.is_roles_security_model);
		is_clean_creation := true;
	end if;
	post_src := $post_src$
        CREATE OR REPLACE FUNCTION mbus.post_<!qname!>(data hstore, headers hstore DEFAULT NULL::hstore, properties hstore DEFAULT NULL::hstore, delayed_until timestamp without time zone DEFAULT NULL::timestamp without time zone, expires timestamp without time zone DEFAULT NULL::timestamp without time zone, iid text default null)
  	RETURNS text AS
	$BDY$	        
	        <!should_be_able_to_post!>
 		select mbus.run_trigger('<!qname!>', $1, $2, $3, $4, $5);
                select mbus.raise_exception('Wrong iid=' || $6 || ' for queue <!qname!>') where $6 is not null and $6 not like $Q$<!qname!>.%$Q$ and $6 not like $Q$dmq.%$Q$;
 		insert into mbus.qt$<!qname!>(data, 
                                headers, 
                                properties, 
                                delayed_until, 
                                expires, 
                                added, 
                                iid, 
                                received
                               )values(
                                $1,
                                hstore('enqueue_time',coalesce((headers->'enqueue_time'),now()::timestamp::text)) ||
                                hstore('source_db', current_database())       ||
                                hstore('destination_queue', $Q$<!qname!>$Q$)       ||
                                coalesce($2,''::hstore)||
                                case when $2 is not null and exist($2,'consume_after')  then hstore('consume_after', ($2->'consume_after')::text[]::text) else ''::hstore end ||
                                case when $2 is null or not exist($2,'seenby') then hstore('seenby', array[ current_database() ]::text) else hstore('seenby', (($2->'seenby')::text[] || array[current_database()::text])::text) end,
                                $3,
                                coalesce($4, now() - '1h'::interval),
                                $5, 
                                coalesce((headers->'enqueue_time')::timestamp,now()), 
                                coalesce($6, $Q$<!qname!>$Q$ || '.' || nextval('mbus.seq')),
                                array[]::int[] 
                               ) returning iid;
	$BDY$
  	LANGUAGE sql VOLATILE
  	<!SECURITY_DEFINER!>
	SET enable_seqscan = off
  	COST 100;
	$post_src$;

 	post_src:=regexp_replace(post_src,'<!qname!>', qname, 'g');

	is_roles_security_model:=(select q.is_roles_security_model from mbus.queue q where q.qname=create_queue.qname);
	post_src:=regexp_replace(post_src,'<!should_be_able_to_post!>',
	                                   case when is_roles_security_model then $S$select mbus._should_be_able_to_post('$S$ || qname || $S$');$S$ else '--' end
	                        );
	post_src:=regexp_replace(post_src,'<!SECURITY_DEFINER!>',
	                                   case when is_roles_security_model then 'security definer set search_path = mbus, pg_temp, public ' else '' end
	                        );
        execute 'drop function if exists mbus.post_' || qname || '(hstore, hstore, hstore,timestamp without time zone,timestamp without time zone)';
        execute 'drop function if exists mbus.post_' || qname || '(hstore, hstore, hstore,timestamp without time zone,timestamp without time zone, text)';
 	execute post_src;

 	clr_src:=$CLR_SRC$
	create or replace function mbus.clear_queue_<!qname!>()
	returns void as
	$ZZ$
	declare
		qry text;
	begin
        <!should_be_able_to_clear!>

	select string_agg( 'select id from mbus.consumer where id=' || id::text ||' and ( (' || r.selector || ')' || (case when r.added is null then ')' else $$ and q.added >= '$$ || (r.added::text) || $$'::timestamp without time zone)$$ end) ||chr(10), ' union all '||chr(10))
  	into qry
  		from mbus.consumer r where qname='<!qname!>';
 	execute 'delete from mbus.qt$<!qname!> q where expires < now() or (received <@ array(' || qry || '))';
	end; 
	$ZZ$
	language plpgsql
        <!SECURITY_DEFINER!>
	$CLR_SRC$;

 	clr_src:=regexp_replace(clr_src,'<!qname!>', qname, 'g');
        clr_src:=regexp_replace(clr_src,'<!should_be_able_to_clear!>',
	                                   case when is_roles_security_model then $S$perform mbus._should_be_owner_or_root();$S$ else '--' end
	                        );
	clr_src:=regexp_replace(clr_src,'<!SECURITY_DEFINER!>',
	                                   case when is_roles_security_model then 'security definer set search_path = mbus, pg_temp, public ' else '' end
	                        );
        execute clr_src;

 	peek_src:=$PEEK$
  	create or replace function mbus.peek_<!qname!>(msgid text default null)
  	returns mbus.qt_model as
  	$PRC$
   	select case 
              when <!should_be_able_to_consume!> is not null and $1 is null then (select row(q.*)::mbus.qt_model from mbus.qt$<!qname!> q)
              else (select row(q.*)::mbus.qt_model from mbus.qt$<!qname!> q where iid=$1)::mbus.qt_model
        end;
  	$PRC$
  	language sql
    <!SECURITY_DEFINER!>
 	$PEEK$;
 	peek_src:=regexp_replace(peek_src,'<!qname!>', qname, 'g');
        peek_src:=regexp_replace(peek_src,'<!should_be_able_to_consume!>',
	                                   case when is_roles_security_model then $S$mbus._should_be_able_to_consume('$S$ || qname || $S$')$S$ else $S$''$S$ end
	                        );
	peek_src:=regexp_replace(peek_src,'<!SECURITY_DEFINER!>',
	                                   case when is_roles_security_model then 'security definer set search_path = mbus, pg_temp, public ' else '' end
	                        );
 	execute peek_src;
 
 	perform mbus.create_consumer(cname:='default',qname:=qname, noindex:=not is_clean_creation);
 	perform mbus.regenerate_functions(false); 
end; 
$_$;



CREATE FUNCTION create_run_function(qname text) 
RETURNS void
LANGUAGE plpgsql
AS $_$
declare
	func_src text:=$STR$
	create or replace function mbus.run_on_<!qname!>(exec text)
	returns integer as
	$CODE$
	declare
  		r mbus.qt_model;
  		cnt integer:=0;
 	begin
  		for r in select * from mbus.consumen_<!qname!>_by_default(100) loop
			begin
				execute exec using r;
				cnt:=cnt+1;
			exception
			when others then
				insert into mbus.dmq(added,  iid,  delayed_until,   expires,received,    headers,  properties,  data)  
					values(r.added,r.iid,r.delayed_until, r.expires,r.received,r.headers||hstore('dmq.added',now()::timestamp::text)||hstore('dmq.error',sqlerrm),r.properties,r.data);
			end;
  		end loop;
  	return cnt;
 	end;
	$CODE$
	language plpgsql
	$STR$;
begin
	if not exists(select * from mbus.queue q where q.qname=create_run_function.qname) then
  		raise exception 'Queue % does not exists!',qname;
 	end if;
 	func_src:=regexp_replace(func_src,'<!qname!>', qname, 'g');
 		execute func_src;
 	raise notice '%', func_src;
end;
$_$;



CREATE FUNCTION create_temporary_consumer(cname text, p_selector text DEFAULT NULL::text) 
RETURNS text
LANGUAGE plpgsql
    AS $_$
declare
	selector text;
	tq text:=mbus.create_temporary_queue();
begin
	if not exists(select * from pg_catalog.pg_tables where schemaname='mbus' and tablename='qt$' || cname) then
        	raise notice 'WARNING: source queue (%) does not exists (yet?)', cname;
   	end if;
  	selector := case when p_selector is null or p_selector ~ '^ *$' then '1=1' else p_selector end;
  		insert into mbus.trigger(src,dst,selector) values(cname,tq,selector);
  	return tq;
end;
$_$;



CREATE FUNCTION create_temporary_queue() RETURNS text
    LANGUAGE sql
    AS $$
  select 'temp.' || md5(pid::text || backend_start::text) || '.' || txid_current() from pg_stat_activity where pid=pg_backend_pid();
$$;


CREATE FUNCTION create_trigger(src text, dst text, selector text DEFAULT NULL::text) RETURNS void
    LANGUAGE plpgsql
    AS $_$
/*

-->>>Trigger test
declare
  h hstore;
  rv mbus.qt_model;
 begin
  perform mbus.create_queue('q1',128);
  perform mbus.create_queue('qcopy',128);
  perform mbus.create_queue('qcopy2',128);
  perform mbus.create_trigger('q1','qcopy');
  perform mbus.create_trigger('q1','qcopy2',$SEL$not exist(t.data,'key2') or (t.data->'key2')<>'N'$SEL$);

  perform mbus.post(qname:='q1',data:=('key=>val')::hstore, headers:=hstore('enqueue_time', (now()+'1s'::interval)::text));

  select * into rv from mbus.consume('qcopy');
  if not found or ((rv.data)->'key') is distinct from 'val' then
    raise notice '%', found;
    raise exception '1.Cannot get copy of message!';
  end if;

  select * into rv from mbus.consume('qcopy2');
  if not found or ((rv.data)->'key') is distinct from 'val' then
    raise notice '%', found;
    raise exception '2.Cannot get copy of message!';
  end if;

  perform mbus.post(qname:='q1',data:=('key=>val,key2=>N')::hstore, headers:=hstore('enqueue_time', (now()+'1s'::interval)::text));
  select * into rv from mbus.consume('qcopy');
  if not found or ((rv.data)->'key') is distinct from 'val' then
    raise notice '%', found;
    raise exception '3.Cannot get copy of message!%',rv;
  end if;

  select * into rv from mbus.consume('qcopy2');
  if found then
    raise exception 'Get copy of message that must be filtered out!';
  end if;
end;
--<<<
*/
declare
 selfunc text;
begin
   if not exists(select * from pg_catalog.pg_tables where schemaname='mbus' and tablename='qt$' || src) then
        raise notice 'WARNING: source queue (%) does not exists (yet?)', src;
   end if;
   if not exists(select * from pg_catalog.pg_tables where schemaname='mbus' and tablename='qt$' || dst) then
        raise notice 'WARNING: destination queue (%) does not exists (yet?)', dst;
   end if;

   if exists(
              with recursive tq as(
                select 1 as rn, src as src, dst as dst
                union all
                select tq.rn+1, t.src, t.dst from mbus.trigger t, tq where t.dst=tq.src
              )
              select * from tq t1, tq t2 where t1.dst=t2.src and t1.rn=1   
             ) then
     raise exception 'Loop detected';
   end if;
   
   if selector is not null then
        begin
        execute $$with t as (
                     select now() as added, 
                           'IID' as iid, 
                           now() as delayed_until, 
                           now() as expires, 
                           array[]::integer[] as received, 
                           hstore('$%$key','NULL') as headers, 
                           hstore('$%$key','NULL') as properties,
                           hstore('$%$key','NULL') as data
                       )
                       select * from t where $$ || selector;
          exception
           when sqlstate '42000' then 
             raise exception 'Syntax error in selector:%', selector;
          end;
          
          selfunc:= $SRC$create or replace function mbus.trigger_<!srcdst!>(t mbus.qt_model) returns boolean as $FUNC$
                         begin
                           return <!selector!>;
                         end;
                        $FUNC$
                        language plpgsql immutable
                     $SRC$;
          selfunc:=regexp_replace(selfunc,'<!srcdst!>', src || '_to_' || dst);
          selfunc:=regexp_replace(selfunc,'<!selector!>', selector);
      
          execute selfunc;
      end if;
      insert into mbus.trigger(src,dst,selector) values(src,dst,selector);         
end;
$_$;


CREATE FUNCTION create_view(qname text, cname text default 'default', sname text default 'public', viewname text default null) RETURNS void
    security definer set search_path = mbus, pg_temp, public
    LANGUAGE plpgsql
    AS $_$
/*
-->>>Views test
 declare
  rv hstore;
 begin
   perform mbus.create_queue('q1',128);
   perform mbus.create_consumer('ananotherconsumer','q1');
   perform mbus.post(qname:='q1',data:=('key=>value12')::hstore, headers:=hstore('enqueue_time', (now()+'1s'::interval)::text));
   perform mbus.create_view(qname:='q1', viewname:='atestviewforq1');
   insert into atestviewforq1(data) values('thekey=>theval'::hstore);
   select data into rv from atestviewforq1;
   if not found or (rv->'key') is distinct from 'value12' then
     raise exception 'cannot consume expected record from created view:%',found::text || ' ' ||rv;
   end if;
 end;   
--<<<
*/

declare
	param hstore:=hstore('qname',qname)||hstore('cname',cname)|| hstore('sname',sname||'.')|| hstore('viewname', coalesce(viewname, 'public.'||qname));
begin
	perform mbus._should_be_owner_or_root();
	execute mbus.string_format($STR$ create view %<sname>%<viewname> as select data from mbus.consume('%<qname>', '%<cname>')$STR$, param);
	execute mbus.string_format($STR$
	create or replace function %<sname>trg_post_%<viewname>() returns trigger as
	$thecode$
	begin
		perform mbus.post('%<qname>',new.data);
 		return null;
	end;
	$thecode$
	security definer
	SET search_path = mbus, pg_temp, public
	language plpgsql;

	create trigger trg_%<qname>  instead of insert on %<sname>%<viewname> for each row execute procedure %<sname>trg_post_%<viewname>();   
	$STR$, param);
end;
$_$;


CREATE FUNCTION create_view_prop(qname text, cname text, sname text, viewname text, with_delay boolean default false, with_expire boolean default false) RETURNS void
    security definer set search_path = mbus, pg_temp, public
    LANGUAGE plpgsql
    AS $_$
declare
	param hstore := hstore('qname',qname)||hstore('cname',cname)|| hstore('sname',sname||'.')|| hstore('viewname', coalesce(viewname, 'public.'||qname));
begin
        perform mbus._should_be_owner_or_root();
	execute mbus.string_format($STR$ create view %<sname>%<viewname> as select data, properties, delayed_until, expires from mbus.consume('%<qname>', '%<cname>')$STR$, param);
	execute mbus.string_format($STR$
	create or replace function %<sname>trg_post_%<viewname>() returns trigger as
	$thecode$
	begin
		perform mbus.post_%<qname>(new.data, null::hstore, new.properties, new.delayed_until, new.expires);
 		return null;
	end;
	$thecode$
	security definer
	SET search_path = mbus, pg_temp, public
	language plpgsql;

	create trigger trg_%<qname>  instead of insert on %<sname>%<viewname> for each row execute procedure %<sname>trg_post_%<viewname>();   
	$STR$, param);
end;
$_$;

create or replace function queue_acl( oper text, usr text, qname text, viewname text, schemaname text)
returns void 
security definer set search_path = mbus, pg_temp, public
language plpgsql as
$_$
declare
	param hstore := hstore('oper', oper) || hstore('qname', qname) || hstore('usr', usr ) || hstore('viewname', viewname) || hstore('schemaname', schemaname);
	l_func text;
begin
/*

*/
        perform mbus._should_be_owner_or_root();
	if lower(oper) = 'grant' then
		param := param || hstore('dir', 'to');
	elsif lower(oper) = 'revoke' then
		param := param || hstore('dir', 'from');
	else
		return;
	end if;

	execute mbus.string_format($SCH$ %<oper> usage on schema mbus %<dir> %<usr>$SCH$, param);
	execute mbus.string_format($VSCH$ %<oper> usage on schema %<schemaname> %<dir> %<usr>$VSCH$, param);
	execute mbus.string_format($VIW$ %<oper> insert,select on %<schemaname>.%<viewname> %<dir> %<usr>$VIW$, param);
	execute mbus.string_format($TBL$ %<oper> all on mbus.qt$%<qname> %<dir> %<usr>$TBL$, param);
	execute mbus.string_format($FNC$   
		with f as ( 
			SELECT n.nspname ||  '.' || p.proname || '(' || pg_catalog.pg_get_function_identity_arguments(p.oid) || ')' as func
				FROM pg_catalog.pg_proc p
				     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
				WHERE n.nspname ~ '^(mbus)$' and p.proname ~ '%<qname>'
			)
			select array_to_string( array_agg(func), ',') from f 
	$FNC$, param) into l_func;
	
	param := param || hstore('l_func', l_func);
	execute mbus.string_format($FNCL$%<oper> execute on function %<l_func> %<dir> %<usr>$FNCL$, param);
	
end;
$_$;


CREATE FUNCTION drop_consumer(cname text, qname text) RETURNS void
    security definer set search_path = mbus, pg_temp, public
    LANGUAGE plpgsql
    AS $_$
 begin
   perform mbus._should_be_owner_or_root();
   delete from mbus.consumer c where c.name=drop_consumer.cname and c.qname=drop_consumer.qname;
   execute 'drop index mbus.qt$' || qname || '_for_' || cname;
   execute 'drop function mbus.consume_' || qname || '_by_' || cname || '()';
   execute 'drop function mbus.consumen_' || qname || '_by_' || cname || '(integer)';
   execute 'drop function mbus.take_from_' || qname || '_by_' || cname || '(text)';
   perform mbus._drop_consumer_role(qname, cname);
   perform mbus.regenerate_functions(false);
 end;
$_$;



CREATE FUNCTION drop_queue(qname text) RETURNS void
    security definer set search_path = mbus, pg_temp, public
    LANGUAGE plpgsql
    AS $_X$
/*
do $thecode$
-->>>Drop queue
 declare
  cnt integer;
 begin
   perform mbus.create_queue('zzqn122',32);
   perform mbus.drop_queue('zzqn122');
   if exists(select * from information_schema.tables where table_schema='mbus' and table_name='qt$zzqn122') then
     raise exception 'Table has been left after queue was dropped';
   end if;
   execute mbus.string_format($SQL$select count(*) from information_schema.applicable_roles where role_name='mbus_%<db>_post_zzqn122'$SQL$, hstore('db', current_catalog))
     into cnt;
   if cnt>0 then
     raise exception 'Role for posters has been left after queue was dropped';
   end if;
   execute mbus.string_format($SQL$select count(*) from information_schema.applicable_roles where role_name like 'mbus_%<db>_consume_zzqn122_by%'$SQL$, hstore('db', current_catalog))
     into cnt;
   if cnt>0 then
     raise exception 'Role for posters has been left after queue was dropped';
   end if;
 end;
 --<<<
 $thecode$;
*/
declare
 r record;
begin
 perform mbus._should_be_owner_or_root();
 begin
   execute 'drop table mbus.qt$' || qname || ' cascade';
 exception
  when sqlstate '42P01' then raise exception 'Queue % does not exists', qname;
 end;

 perform mbus._drop_queue_role(qname);
 for r in (select * from 
                  pg_catalog.pg_proc prc, pg_catalog.pg_namespace nsp 
                  where  prc.pronamespace=nsp.oid and nsp.nspname='mbus'
                  and ( prc.proname ~ ('^post_' || qname || '$')
                        or
                        prc.proname ~ ('^consume_' || qname || $Q$_by_\w+$$Q$)
                        or 
                        prc.proname ~ ('^consumen_' || qname || $Q$_by_\w+$$Q$)
                        or 
                        prc.proname ~ ('^peek_' || qname || '$')
                        or 
                        prc.proname ~ ('^take_from_' || qname)
                        or
                        prc.proname ~('^run_on_' || qname)
                  )
  ) loop
    case 
      when r.proname like 'post_%' then
       execute 'drop function if exists mbus.' || r.proname || '(hstore, hstore, hstore, timestamp without time zone, timestamp without time zone, text)';
       execute 'drop function if exists mbus.' || r.proname || '(hstore, hstore, hstore, timestamp without time zone, timestamp without time zone)';
      when r.proname like 'consumen_%' then
       execute 'drop function mbus.' || r.proname || '(integer)';
      when r.proname like 'peek_%' then
       execute 'drop function mbus.' || r.proname || '(text)';
      when r.proname like 'take_%' then
       execute 'drop function mbus.' || r.proname || '(text)';
      when r.proname like 'run_on_%' then
       execute 'drop function mbus.' || r.proname || '(text)';
      else
       execute 'drop function mbus.' || r.proname || '()';
    end case;   
  end loop;
  delete from mbus.consumer c where c.qname=drop_queue.qname;
  delete from mbus.queue q where q.qname=drop_queue.qname;
  execute 'drop function mbus.clear_queue_' || qname || '()'; 
  
  begin
    execute 'drop function mbus.build_' || qname || '_record_consumer_list(mbus.qt_model)';
  exception when others then null;
  end;
  perform mbus.regenerate_functions(false);  
end;
$_X$;



CREATE FUNCTION drop_trigger(src text, dst text) RETURNS void
    security definer set search_path = mbus, pg_temp, public
    LANGUAGE plpgsql
    AS $$
 begin
  perform mbus._should_be_owner_or_root();
  delete from mbus.trigger where trigger.src=drop_trigger.src and trigger.dst=drop_trigger.dst;
  if found then
    begin
      execute 'drop function mbus.trigger_' || src || '_to_' || dst ||'(mbus.qt_model)';
    exception
     when sqlstate '42000' then null; --syntax error - skip it
    end;
  end if;
 end;
$$;

CREATE FUNCTION post_dmq(data hstore, headers hstore DEFAULT NULL::hstore, properties hstore DEFAULT NULL::hstore, delayed_until timestamp without time zone DEFAULT NULL::timestamp without time zone, expires timestamp without time zone DEFAULT NULL::timestamp without time zone, iid text default null) RETURNS text
    LANGUAGE sql
    AS $_$
insert into mbus.dmq(data,
                      headers,
                      properties,
                      delayed_until,
                      expires,
                      added,
                      iid
                     )values(
                      $1,
                      $2,
                      $3,
                      coalesce($4, now()),
                      $5,
                      now(),
                      coalesce($6,'dmq.' || nextval('mbus.seq') )
                     ) returning iid;

$_$;



CREATE OR REPLACE FUNCTION mbus.dyn_consume(qname text, selector text DEFAULT '(1=1)'::text, cname text DEFAULT 'default'::text)
  RETURNS SETOF mbus.qt_model AS
$BODY$
declare
 rv mbus.qt_model;
 consid integer;
 consadded timestamp;
 hash text:='_'||md5(coalesce(selector,''));
 is_rsm boolean :=false;
begin
 perform mbus._should_be_owner_or_root();
 if selector is null then
   selector:='(1=1)';
 end if;
 
 select id, added into consid, consadded from mbus.consumer c where c.qname=dyn_consume.qname and c.name=dyn_consume.cname;
 if not found then
    raise exception 'Consumer % does not exists!', cname;
 end if;

 if (select is_roles_security_model from mbus.queue q where q.qname=dyn_consume.qname) then
  	perform mbus._should_be_able_to_consume(qname,cname);
 end if;
 
 begin
   execute 'execute /**/ mbus_dyn_consume_'||qname||hash||'('||consid||','''||consadded||''')' into rv;
  exception
    when sqlstate '26000' then
      if not exists(select * from mbus.queue q where q.qname=dyn_consume.qname) then
        raise exception 'Queue % does not exists!', qname;
      end if;      
      execute       
      $QRY$prepare mbus_dyn_consume_$QRY$ || qname||hash || $QRY$(integer, timestamp) as
        select * 
          from mbus.qt$$QRY$ || qname ||$QRY$ t
         where $1<>all(received) and t.delayed_until<now() and (1=1)=true and added > $2 and coalesce(expires,'2070-01-01'::timestamp) > now()::timestamp 
           and ($QRY$ || selector ||$QRY$)
           and pg_try_advisory_xact_lock( hashtext(t.iid))
         order by added, delayed_until
         limit 1
           for update
        $QRY$;
   execute 'execute /**/ mbus_dyn_consume_'||qname||hash||'('||consid||','''||consadded||''')' into rv;
  end;


 if rv.iid is not null then 
    if (select array_agg(id) from mbus.consumer c where c.qname=dyn_consume.qname and c.added<=rv.added) <@ (rv.received || consid::integer)::integer[] then
      begin
       execute 'execute mbus_dyn_delete_'||qname||'('''||rv.iid||''')' using qname;
      exception
       when sqlstate '26000' then
         execute 'prepare mbus_dyn_delete_'||qname||'(text) as delete from mbus.qt$'|| qname ||' where iid = $1';
         execute 'execute mbus_dyn_delete_'||qname||'('''||rv.iid||''')';
      end;  
    else
      begin
       execute 'execute mbus_dyn_update_'||qname||'('''||rv.iid||''','||consid||')';
      exception
       when sqlstate '26000' then
         execute 'prepare mbus_dyn_update_'||qname||'(text,integer) as update mbus.qt$'||qname||' t set received=received || $2 where t.iid=$1';
         execute 'execute mbus_dyn_update_'||qname||'('''||rv.iid||''','||consid||')';
      end;        
    end if;
    rv.headers = rv.headers || hstore('destination',qname);
    return next rv;
    return;
 end if; 
end;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100
  ROWS 1000
  security definer set search_path = mbus, pg_temp, public
  set enable_seqscan=off;



CREATE FUNCTION post_temp(tqname text, data hstore, headers hstore DEFAULT NULL::hstore, properties hstore DEFAULT NULL::hstore, delayed_until timestamp without time zone DEFAULT NULL::timestamp without time zone, expires timestamp without time zone DEFAULT NULL::timestamp without time zone) RETURNS text
    LANGUAGE sql
    AS $_$
 insert into mbus.tempq(data, 
                                headers, 
                                properties, 
                                delayed_until, 
                                expires, 
                                added, 
                                iid, 
                                received
                               )values(
                                $2,
                                hstore('tempq',$1)                        ||
                                hstore('enqueue_time',now()::timestamp::text) ||
                                hstore('source_db', current_database())       ||
                                case when $3 is null then hstore('seenby','{' || current_database() || '}') else hstore('seenby', (($3->'seenby')::text[] || current_database()::text)::text) end,
                                $4,
                                coalesce($5, now() - '1h'::interval),
                                $6, 
                                coalesce((headers->'enqueue_time')::timestamp,now()), 
                                current_database() || '.' || nextval('mbus.seq'), 
                                array[]::int[] 
                               ) returning iid;

$_$;

create function get_iid(qname text) returns text as
$code$
   select $1 || '.' || nextval('mbus.seq') 
     || case when not exists(select * from mbus.queue q where q.qname=get_iid.qname) then mbus.raise_exception('No such queue: ' || qname) else '' end;
$code$
language sql;

CREATE FUNCTION readme() RETURNS text
LANGUAGE sql
AS $_$
select $TEXT$

$TEXT$::text;
$_$;
CREATE FUNCTION readme_rus() RETURNS text
    LANGUAGE sql
    AS $_$
 select $TEXT$
 Управление очередями
 Очереди нормальные, полноценные, умеют
 1. pub/sub
 2. queue
 3. request-response

 Еще умеют message selectorы, expiration и задержки доставки
 Payload'ом очереди является значение hstore (так что тип hstore должен быть установлен в базе)

 Очередь создается функцией
  mbus.create_queue(qname, ncons, is_roles_security_model default false)
  где
  qname - имя очереди. Допустимы a-z (НЕ A-Z!), _, 0-9
  ncons - число одновременно доступных частей. Разумные значения - от 2 до 128-256
  больше ставить можно, но тогда будут слишком большие задержки на перебор всех частей
  Если указанная очередь уже существует, то будут пересозданы функции получения и отправки сообщений.
  Если параметр is_roles_security_model установлен, то право на отправку сообщений в очередь
  получат только те пользователи, у которых есть роль mbus_<dbname>_post_<qname>, а на получение
  обладатели роли mbus_<dbname>_consume_<queue_name>_by_<consumer_name>.
 
  Теперь в очередь можно помещать сообщения:
  select mbus.post_<qname>(data hstore, 
                            headers hstore DEFAULT NULL::hstore, 
                            properties hstore DEFAULT NULL::hstore, 
                            delayed_until timestamp without time zone DEFAULT NULL::timestamp without time zone, 
                            expires timestamp without time zone DEFAULT NULL::timestamp without time zone)
   где
   data - собственно payload
   headers - заголовки сообщения, в общем, не ожидается, что прикладная программа(ПП) будет их
             отправлять
   properties - заголовки сообщения, предназначенные для ПП
   delayed_until - сообщение будет доставлено ПОСЛЕ указанной даты. Зачем это надо?
             например, пытаемся отправить письмо, почтовая система недоступна.
             Тогда пишем куда-нибудь в properties число попыток, в delayed_until - (now()+'1h'::interval)::timestamp
             Через час это сообщение будет снова выбрано и снова предпринята попытка
             что-то сделать с сообщением
   expires - дата, до которой живет сообщение. По умолчанию - всегда. По достижению указанной даты сообщение удаляется
             Полезно, чтобы не забивать очереди всякой фигней типа "получить урл", а сеть полегла,
             сообщение было проигнорировано и так и осталось болтаться в очереди.
             От таких сообщений очередь чистится функцией mbus.clear_queue_<qname>()
   Возвращаемое значение: iid добавленного сообщения.

   и еще:
   mbus.post(qname text, data ...)
   Функция ничего не возвращает

   Получаем сообщения:
   mbus.consume(qname) - получает сообщения из очереди qname. Возвращает result set из одного
             сообщения, колонки как в mbus.qt_model. Кроме описанных выше в post_<qname>,
             существуют колонки:
              id - просто id сообщения. Единственное, что про него можно сказать - оно уникально.
                   используется исключительно для генерирования id сообщения
              iid - глобальное уникальное id сообщения. Предполагается, что оно глобально среди всех
                   сообщений; предполагается, что среди всех баз, обменивающихся сообщениями, каждая
                   имеет уникальное имя.
              added - дата добавления сообщения в очередь

   Если сообщение было получено одним процессом вызовом функции mbus.consume(qname), то другой процесс
   его НЕ ПОЛУЧИТ. Это классическая очередь.

   Реализация publish/subscribe
   В настояшей реализации доступны только постоянные подписчики (durable subscribers). Подписчик создается
   функцией 
    mbus.create_consumer(qname, cname, selector)
    где
     qname - имя очереди
     cname - имя подписчика
     selector - выражение, ограничивающее множество получаемых сообщений
     Имя подписчика должно быть уникальным среди всех подписчиков (т.е. не только подписчиков этой очереди)
     В selector допустимы только статические значения, известные на момент создания подписчика
     Алиас выбираемой записи - t, тип - mbus.qt_model, т.е. селектор может иметь вид
      $$(t.properties->'STATE')='DONE'$$,
      но не
      $$(t.properties>'user_posted')=current_user$$,
      Следует заметить, что в настоящей реализации селекторы весьма эффективны и предпочтительней
      пользоваться ими, чем фильтровать сообщения уже после получения.
     Замечание: при создании очереди создается подписчик default

    Получение сообщений подписчиком:
     mbus.consume(qname, cname) - возвращает набор типа mbus.qt_model из одной записи из очереди qname для подписчика cname
     mbus.consume_<qname>_by_<cname>() - см. выше
     mbus.consumen_<qname>_by_<cname>(amt integer) - получить не одно сообщение, а набор не более чем из amt штук.

     Сообщение msg, помещенное в очередь q, которую выбирают два процесса, получающие сообщения для подписчика
     'cons', будет выбрано только одним из двух процессов. Если эти процессы получают сообщения из очереди q для
     подписчиков 'cons1' и 'cons2' соответственно, то каждый из них получит свою копию сообщения.
     После получения поле headers сообщения содержит следующие сообщения:
     seenby - text[], массив баз, которые получили это сообщение по пути к получаетелю
     source_db - имя базы, в которой было создано данное сообщение
     destination - имя очереди, из которой было получено это сообщение
     enqueue_time - время помещения в очередь исходного сообщения (может отличаться от added,
     которое указывает, в какое время сообщение было помещено в ту очередь, из которой происходит получение)

     Если сообщение не может быть получено, возвращается пустой набор. Почему не может быть получено сообщение?
     Вариантов два:
      1. очередь просто пуста
      2. все выбираемые ветви очереди уже заняты подписчиками, получающими сообщения. Заняты они могут быть 
      как тем же подписчиком, так и другими.

     Всмпомогательные функции:
     mbus.peek_<qname>(msgid text default null) - проверяет, если ли в очереди qname сообщение с iid=msgid
     Если msgid is null, то проверяет наличие хоть какого-то сообщения. Следует учесть, что значение "истина",
     возвращенное функцией peek, НЕ ГАРАНТИРУЕТ, что какие-либо функции из семейства consume вернут какое-либо
     значение.
     mbus.take_from_<qname>_by_<cname>(msgid text) - получить из очереди qname сообщение с iid=msgid
     ВНИМАНИЕ: это блокирующая функция, в случае, если запись с iid=msgid уже заблокирована какой-либо транзакцией,
     эта функция будет ожидать доступности записи.



     Временные очереди.
     Временная очередь создается функцией
      mbus.create_temporary_queue()
     Сообщения отправляются обычным mbus.post(qname, data...)
     Сообщения получаются обычным mbus.consume(qname)      
     Временные очереди должны периодически очищаться от мусора вызовом функции
     mbus.clear_tempq()
     Выборка (consume) из временных очередей может быть блокирующей!

     Удаление очередей.
     Временные очереди удалять не надо: они будут удалены автоматически после окончания сессии.
     Обычные очереди удаляются функцией mbus.drop_queue(qname)

     Следует также обратить внимание на то, что активно используемые очереди должны _весьма_
     агрессивно очищаться (VACUUM)

     Триггеры
       Для каждой очереди можно создать триггер - т.е. при поступлении сообщения в очередь
       оно может быть скопировано в другую очередь, если селектор для триггера истинный.
       Для чего это надо? Например, есть очень большая очередь, на которую потребовалось
       подписаться. Создание еще одного подписчика - достаточно затратная вещь, для каждого
       подписчика создается отдельный индекс; при большой очереди надо при создании подписчика
       указывать параметр noindex - тогда индекс не будет создаваться, но текст запроса для
       создания требуемого индекса будет возвращен как raise notice.
       Триггер создается фукцией mbus.create_trigger(src_queue_name text, dst_queue_name text, selector text);

     Временные подписчики
       Временные подписчики создаются функцией mbus.create_temporary_consumer(qname text, selector text)
       Функция возвращает имя временного подписчика
       Подписчик существует до тех пор, пока активная текущая сессия.
       Для удаления уставших подписчиков необходимо периодически вызывать функцию mbus.clear_tempq

     create_run_function(qname text)
     Генерирует функцию вида:
       for r in select * from mbus.consumen_<!qname!>_by_default(100) loop
         execute exec using r;
       end loop;     
     для указанной очереди. Используется для обработки сообщений внутри базы.
     Сгенерированная фукция возвращает количество обработанных сообщений.
     Если при обработке сообщения в exec возникло исключение, то сообщение помещается в dmq

     Функция mbus.create_view
     Предполагается, что все функции выполняются от имени пользователя pg с соответствующими правами.
     Это не всегда устраивает; данная фунцкция создает view с именем viewname (если не указано - то с именем public.queuename_q)
     и триггер на вставку в него; на это view уже можно раздавать права для обычных пользователей.

     Упорядочивание сообщений
     Для сообщения (назовем его 1) может быть указан id других сообщений(назовем их 2), ранее получения которых сообщение 1 не может быть получено.
     Он находится в заголовках и называется consume_after. Сообщения 1 и 2 не обязаны быть в одной очереди. Зачем это надо?
     Например, мы отправляем сообщение с командой "создать пользователя вася" и затем "для пользователя вася установить лимит в 10 тугриков".
     Так как порядок получения не определен, не исключена ситуация, когда сообщение с лимитом будет получено хронологически раньше,
     чем сообщение о создании пользователя. Таким образом, не очень понятно, что делать с сообщением об установлении лимита:
     либо отправить его обратно в очередь с увеличением счетчика получений и задержкой доставки, либо отбросить; в любом случае
     требуется дополнительный код и т.п. В случае же с упорядочиванием можно потребовать, чтобы сообщение с лимитом было получено
     только и исключительно после сообщения о создании; таким образом проблема устраняется.
     Так как сообщений о получении может быть указано несколько и они могут находиться в любой очереди, то вполне возможен такой
     вариант:
        поместить сообщение "создать пользователя" в очередь команды для сервера №1 и сохранить id сообщения как id1
        поместить сообщение "создать пользователя" в очередь команды для сервера №2 и сохранить id сообщения как id2
        ...
        поместить сообщение "создать пользователя" в очередь команды для сервера №N и сохранить id сообщения как idN

        поместить сообщение "установить лимит" с ограничением "получить после id1" в очередь команды для сервера №1 
        поместить сообщение "установить лимит" с ограничением "получить после id2" в очередь команды для сервера №2
        ... 
        поместить сообщение "установить лимит" с ограничением "получить после idN" в очередь команды для сервера №N
        
        поместить сообщение "установить местоположение профайла пользователя" с ограничением "получить после id1,id2,...idN" в очередь "локальные команды"
         и сохранить id сообщения как id_place_set
        поместить сообщение "удалить пользователя" с ограничением "получить после id_place_set" в очередь "локальные команды"

     Таким образом пользователь будет скопирован на сервера, на каждом из них будет установлен лимит, установлены ссылки на профайлы
     и удален пользователь на локальном сервере.

         !!!!!  При невозможности обработать сообщение оно должно быть помещено обратно в ту же очередь или в dmq со старым iid !!!!!!
     
     Внимание!
       Большое количество сообщений, ожидающих доставки другого сообщения может привести к снижению производительности.

     Функция dyn_consume(qname text, selector text default '(1=1)', cname text default 'default')
     Позволяет динамически указывать очередь, селектор и получателя
     Работает при пустом селекторе и небольшой очереди примерно в два раза медленее обычного mbus4.consume(qname)
     Так как при обработке селектора не используется индекс, на больших очередях может быть заметно снижение производительности.

    Саги
    Сага - это долговременная последовательность транзакций, которая должна либо выполниться целиком, либо быть откачена целиком
    путем применения для каждой ранее успешно выполненной транзакции компенсационной транзакции, откатывающей ее действие. 
    Например, создание тура для юзера - это сага. Надо

     1. Зарезервировать номер (вебсервис куда-то)
     2. Зарезервировать билеты на самолет (туда и обратно) исходя из параметров резервирования номера (какие-то http-запросы)
     3. Исходя из параметров билетов - зарезервировать трансфер от/до аэропорта (еще какие-то http-запросы)
     4. Юзер утверждает предложенный вариант.

    Пункты 2 и 3 могут выполняться параллельно. В случае сбоя все ранее проведенные резервирования должны быть отменены.
    Как это должно быть выполнено :

    Создаются ДВЕ последовательности действий, две упорядоченных  группы сообщений : первая ("прямая") - резервирование 
    и вторая, с компенсационными сообщениями("обратная") - откат резервирования, причем вторая  упорядочена относительно 
    сообщения в специальной очереди ("затычки"), из которой выборка производится только путем вызова функции take. 
    Это служебное сообщение, от которого зависят  компенсационные сообщения; кроме того, в нем находятся iid всех 
    сообщений прямой последовательности; в случае инициирования отката это сообщение забирается функцией take и 
    функцией же take забираются все сообщения прямой последовательности, полученные из этого служебного сообщения. 
    Все сообщения прямой последовательности содержат ссылку на головное сообщение ("затычку") последовательности отката.
    Последнее сообщение прямой последовательности - также служебное, в нем находятся сообщения последовательности отката и 
    при окончании прямой последовательности они также забираются функцией take. Последнее сообщение обратной 
    последовательности, как и прямой, служебное - для фиксации успешности проведения отката.


                                Прямая последовательность                                  |   Откат
                     ----------------------------------------------------------------------+-----------------------------------------------------------------------
                     id     выбрать после id    команда                 Сообщение отката   | id     выбрать после id    команда
                     ----------------------------------------------------------------------+------------------------------------------------------------------------
                     1        -/-               Зарезервировать номер   1b                 | 1b                         забрать сообщения с #1,2,3,4,5,6
 могут выполняться / 2      1                   Заказ билета туда       1b                 | 2b     1b                  отмена резервирования отеля                  \
 параллельно       \ 3      1                   Заказ билета обратно    1b                 | 3b     1b                  отмена резервирования билета туда             |
 могут выполняться / 4      2                   Трансфер до отеля       1b                 | 4b     1b                  отмена резервирования билета обратно          + могут выполняться параллельно
 параллельно       \ 5      3                   Трансфер от отеля       1b                 | 5b     1b                  отмена резервирования трансфера до отеля      |
                     6      4,5                 забрать сообщения с     1b                 | 6b     1b                  отмена резервирования транфера от отеля      /
                                                #1b,2b,3b,4b,5b,6b,7b                      | 7b     2b,3b,4b,5b,6b      откат завершен

     Всякий консумер прямой очереди в случае обнаружения необходимости отката выбирает сообщение путем вызова функции take, откуда получает полный список
     сообщений прямой последотвальности, которые также забирает командой take, после чего фиксирует транзакцию и завершается.
     Для передачи данных нам потребуется иметь таблицу "проведение тура" (тем более что все равно надо иметь возможность просмотреть формируемые
     туры, залипшие туры, отчетность по сформированным и т.п.)
     Каждый тип сообщения - как прямого, так и сообщения отката - фактически требует своего отдельного консумера, что логично - 
     консумер, который резервирует номера, другой консумер, который разбирается с авиабилетами, третий - с трансфером.

     Псевдокод:
     -- forward transaction
     bung_iid := generate_new_msgiid('bungs'); -- нам потребуется iid сообщения-затычки
     final_iid := generate_new_msgiid('final'); -- и финального сообщения в прямой последовательности

     insert into tour.... returning tour_id into tour_id; --в tour у нас будут жить промежуточные данные - номера рейсов, мест, номеров в отеле и и.п.
                                                          --а также id сообщений, которые обрабатывают данный заказ на тур
     room_resvr := post('rooms_reservations', ....);
     book_tiket_to := post('book_tikets', consume_after := array[room_resvr], tour_id := tour_id, bung :=bung_iid ...);
     book_tiket_from := post('book_tikets', consume_after := array[room_resvr],tour_id := tour_id, bung :=bung_iid ...);
     transf_to := post('transfer_to', consume_after := array[book_tiket_to], tour_id := tour_id, bung :=bung_iid ...);
     transf_from := post('transfer_from', consume_after := array[book_tiket_from], tour_id := tour_id, bung :=bung_iid ...);

     -- compensation transactions
     post('bungs', id_to_take := array[ room_revrv, book_tiket_to, book_tiket_from, transf_to, transf_from, final_iid], iid:=bung_iid);
     cancel_room_resvr:= post('cancel_room_reservations', consume_after:=bung_iid, tour_id := tour_id...);
     cancel_tiket_booking_to:=post('cancel_tiket_booking_to', consume_after:=bung_iid, tour_id := tour_id...);
     cancel_tiket_booking_from:=post('cancel_tiket_booking_from', consume_after:=bung_iid, tour_id := tour_id...);
     cancel_transfer_to:=post('cancel_transfer_to', consume_after:=bung_iid, tour_id := tour_id...);
     cancel_transfer_from:=post('cancel_transfer_from', consume_after:=bung_iid, tour_id := tour_id...);
     post('cancel_completed', consume_after:=array[cancel_room_resvr,cancel_tiket_booking_to,canel_tiket_booking_from,canel_transfer_to,canel_transfer_from]);

     update tour set room_resvr_iid=room_resvr,
                     .... 
                     cancel_room_resvr_iid:=cancel_room_resvr,
                     ....
            where tour_id=tour_id; --чтобы можно было видеть в процессе выполнения статус обработки/отката резервирования тура

     --закрываем прямой ход
     post('end_of_tour_reservation', iid:=final_iid, id_to_take:=array[cancel_room_reservation,cancel_tiket_booking_to,canel_tiket_booking_from,canel_transfer_to,canel_transfer_from, bung_iid]);

     --наконец
     commit;

     Кроме того, нам потребуются 12 обработчиков сообщений - пять резверирующих номера, рейсы, трансфер и пять откатывающих резервирование, плюс два
     для окончания прямого хода или отката. Каждый из пяти резервантов должен в случае обнаружения необходимости отката выбрать сообщение с
     данным bung_id и уже из него выбрать iid сообщений из id_to_take и забрать их тоже. Два оставшихся обработчика - успешного выполнения и отката -
     должны соответствующим образом поменять запись в таблице tour - дескать, заказ завершен и отправлять сообщение с уведомлением 
     юзера о том, что надо утвердить собранное (и менеджеру, что этот тур собран - ну или не собран, хехе)

   Что должны делать обработчики прямой и обратной последовательностей? Все очень прямолинейно:
    1. Начать транзакцию
    2. Получить сообщение
    3. Попытаться выполнить требуемую операцию
    3.1 При удачном выполнении - изменить запись о состоянии резервировании тура (всякие подробности в таблице tour)
    3.2 При неудачном - либо отправить сообщение обратно с отложенной доставкой и увеличением счетчика, либо, если счетчик уже зашкаливает, перейти к следующему пункту
    3.3 При невозможности - сервис посылает или слишком много попыток - начать откат, как описано выше -
        взять iid затычки, выбрать по нему затычку, выбрать все сообщения, которые в ней указаны, поменять tour на состояние "откатывается потому что...."
    4. Зафиксировать транзакцию

 Безопасность и права доступа
    Если пользователь (user) имеет роль вида mbus_<dbname>_post_<queue_name>, где <dbname> - имя базы, <queue_name> - имя очереди, то он 
    может отправлять сообщения в эту очередь.
    Если пользоваетль имеет роль mbus_<dbname>_consume_<queue_name>_by_<consumer_name>, где <queue_name> - имя очереди, <consumer_name> - имя подписчика,
    то он может получать сообщения из этой очереди от имени указанного подписчика.
$TEXT$::text;
$_$;


--' - single quote here is just for colorer

CREATE FUNCTION regenerate_functions(is_full_regeneration boolean default true) RETURNS void
    LANGUAGE plpgsql
    AS $_X$
declare
 r record; 
 r2 record;
 post_qry text:='';
 consume_qry text:='';
 oldqname text:='';
 visibilty_qry text:='';
 msg_exists_qry text:='';
 peek_qry text:='';
 take_qry text:='';
begin
 --set data type for column id to bigint for all queues where datatype is integer
 --it's required just for migration from older version
  for r in (select table_name from information_schema.columns where table_schema='mbus' and table_name like 'qt$%' and data_type='integer') loop
    execute mbus.string_format('alter table mbus.%"table" alter column id set data type bigint', hstore('table', r.table_name));
  end loop;

 for r in select * from mbus.queue loop
   post_qry:=post_qry || $$ when '$$ || lower(r.qname) || $$' then return mbus.post_$$ || r.qname || '(data, headers, properties, delayed_until, expires);'||chr(10);
   msg_exists_qry := msg_exists_qry || 'when $1 like $LIKE$' || lower(r.qname) || '.%$LIKE$ then exists(select * from mbus.qt$' || r.qname || ' q where q.iid=$1 and not mbus.build_' || r.qname ||'_record_consumer_list(row(q.*)::mbus.qt_model) <@ q.received)'||chr(10);
   peek_qry:= peek_qry || 'when $1 like $LIKE$' || lower(r.qname) || '.%$LIKE$ then (select row(q.*)::mbus.qt_model from mbus.qt$' || r.qname || ' q where q.iid=$1 )'||chr(10);
   take_qry:= take_qry || '        when msgiid like $LIKE$' || lower(r.qname) || '.%$LIKE$ then delete from mbus.qt$' || r.qname || ' q where q.iid=$1 returning (q.*) into rv;'||chr(10);
   --create roles
   begin
     execute 'create role mbus_' || current_database() || '_post_' || r.qname;
   exception
    when sqlstate '42710' then null; --already exists
   end;
   if is_full_regeneration then
      perform mbus.create_queue(r.qname, r.consumers_cnt, r.is_roles_security_model);
   end if;
 end loop;

  --create functions for tests for visibility
  for r in 
    select string_agg( 
          'select '|| id::text ||' from t where 
           ( (' 
          || cons.selector 
          || ')' 
          || (case when cons.added is null then ')' else $$ and t.added > '$$ 
          || (cons.added::text) 
          || $$'::timestamp without time zone)$$ end),
          chr(10) || ' union all ' ||chr(10)) as src,
          qname
        from mbus.consumer cons
        group by cons.qname
    loop
      execute $RCL$
      create or replace function mbus.build_$RCL$ || lower(r.qname) || $RCL$_record_consumer_list(qr mbus.qt_model) returns int[] as
      $FUNC$
       begin
         return array( 
         with t as (select qr.*)
          $RCL$ || r.src || $RCL$ );
       end;
      $FUNC$
      language plpgsql;
      $RCL$;
    end loop;

  execute 'drop function if exists mbus.post(text, hstore, hstore, hstore, timestamp, timestamp)';
  execute 'drop function if exists mbus.post(text, hstore, hstore, hstore, timestamp, timestamp, text)';
 
 if post_qry='' then
        begin
          execute 'create or replace function mbus.is_msg_exists(msgiid text) returns boolean as $code$ select false; $code$ language sql';
        end;
 else
        execute $FUNC$
        ---------------------------------------------------------------------------------------------------------------------------------------------
        create or replace function mbus.post(qname text, data hstore, headers hstore default null, properties hstore default null, delayed_until timestamp default null, expires timestamp default null, iid text default null)
        returns text as
        $QQ$
         begin
          if qname like 'temp.%' then
            return mbus.post_temp(qname, data, headers, properties, delayed_until, expires);
          end if;
          case lower(qname) $FUNC$ || post_qry || $FUNC$
          else
           raise exception 'Unknown queue:%', qname;
         end case;
         end;
        $QQ$
        language plpgsql;
        $FUNC$;

        execute $FUNC$
                create or replace function mbus.is_msg_exists(msgiid text) returns boolean as
                $code$
                 select
                    case $FUNC$
                     || msg_exists_qry ||
                    $FUNC$
                    else
                     false 
                    end or exists(select * from mbus.dmq q where q.iid=$1);
                $code$
                language sql
                security definer
        $FUNC$;

        execute $FUNC$
                create or replace function mbus.peek(msgiid text) returns mbus.qt_model as
                $code$
                 select mbus._should_be_able_to_consume(substring(msgiid from '^[^.]+'));
                 select coalesce(
                    case $FUNC$
                     || peek_qry ||
                    $FUNC$
                    else
                     null
                    end, (select row(q.*)::mbus.qt_model from mbus.dmq q where q.iid=$1));
                $code$
                language sql
                security definer set search_path = mbus, pg_temp, public
        $FUNC$;

        execute $FUNC$
                create or replace function mbus.take(msgiid text) returns mbus.qt_model as
                $code$
                declare
                  rv mbus.qt_model;
                begin
                 perform mbus._should_be_able_to_consume(substring(msgiid from '^[^.]+'));
                 perform pg_advisory_xact_lock( hashtext(msgiid));
                    case $FUNC$
                     || take_qry ||
                    $FUNC$
                    end case;
                    if rv is null then
                     delete from mbus.dmq q where iid=msgiid returning (q.*) into rv;
                    end if;
                    return rv;
                end;
                $code$
                language plpgsql
                security definer set search_path = mbus, pg_temp, public
        $FUNC$;
end if;

 for r2 in select * from mbus.consumer order by qname loop
   if oldqname<>r2.qname then
     if consume_qry<>'' then
       consume_qry:=consume_qry || ' else raise exception $$unknown consumer:%$$, cname; end case;' || chr(10);
     end if;
     consume_qry:=consume_qry || $$ when '$$ || lower(r2.qname) ||$$' then case lower(cname) $$;
   end if;
   consume_qry:=consume_qry || $$ when '$$ || lower(r2.name) || $$' then return query select * from mbus.consume_$$ || r2.qname || '_by_' || r2.name ||'(); return;';
   oldqname=r2.qname;
   --create roles
   begin
     execute 'create role mbus_' || current_database() || '_consume_' || r2.qname || '_by_' || r2.name;
   exception
    when sqlstate '42710' then null; --already exists
   end;
   if is_full_regeneration then
      perform mbus.create_consumer(r2.name, r2.qname, r2.selector, true);
   end if;
 end loop;

 if consume_qry<>'' then
    consume_qry:=consume_qry || ' else raise exception $$unknown consumer:%$$, consumer; end case;' || chr(10);
 end if;

 if consume_qry='' then 
        execute $STR$
          create or replace function mbus.consume(qname text, cname text default 'default') returns setof mbus.qt_model as $code$ select mbus.raise_exception('No queues were defined'); select * from mbus.qt_model;$code$ language sql $STR$;
 else 
        execute $FUNC$
        create or replace function mbus.consume(qname text, cname text default 'default') returns setof mbus.qt_model as
        $QQ$
          /*
          -->>>Will try to consume unexisted queue
          declare
            failed_but_ok boolean;
          begin
            begin
              perform mbus.consume('zzzqqqq');
            exception
              when others then
                if sqlerrm like '%queue%were%defined%' then
                  failed_but_ok:=true;
                end if;
            end;
            if not failed_but_ok then
              raise exception 'Can consumer from unexisted queue';
            end if;
          end;
          --<<<
          */
        begin
          if qname like 'temp.%' then
            return query select * from mbus.consume_temp(qname);
            return;
          end if;
         case lower(qname)
        $FUNC$ || consume_qry || $FUNC$
         else raise exception 'Queue % does not exists', qname;
         end case;
        end;
        $QQ$
        language plpgsql;
        $FUNC$;
 end if;
 
end;
$_X$
set search_path = mbus, pg_temp, public;



CREATE FUNCTION run_trigger(qname text, data hstore, headers hstore DEFAULT NULL::hstore, properties hstore DEFAULT NULL::hstore, delayed_until timestamp without time zone DEFAULT NULL::timestamp without time zone, expires timestamp without time zone DEFAULT NULL::timestamp without time zone) RETURNS void
    LANGUAGE plpgsql
    AS $_$
declare
 r record;
 res boolean;
 qtm mbus.qt_model;
begin
 if headers->'Redeploy' then
  return;
 end if;
 <<mainloop>>
  for r in select * from mbus.trigger t where src=qname loop
   res:=false;
   if r.selector is not null then
       qtm.data:=data;
       qtm.headers:=headers;
       qtm.properties:=properties;
       qtm.delayed_until:=delayed_until;
       qtm.expires:=expires;
       if r.dst like 'temp.%' then
         perform mbus.post_temp(r.dst, data, headers, properties,delayed_until, expires);
       else
         begin
           execute 'select mbus.trigger_'||r.src||'_to_'||r.dst||'($1)' into res using qtm;
        exception
          when others then
            continue mainloop;
        end;  
       end if;
    continue mainloop when not res or res is null;
   end if;
   perform mbus.post(r.dst, data:=run_trigger.data, properties:=run_trigger.properties, headers:=run_trigger.headers);
  end loop;
end;
$_$;



CREATE FUNCTION trigger_work_to_jms_trigger_queue_testing(t qt_model) RETURNS boolean
    LANGUAGE plpgsql IMMUTABLE
    AS $$
                         begin
                           return ((t.properties->'JMS_mbus-JMSType')='MAPMESSAGE');
                         end;
                        $$;

create or replace function test_a_routine(fname text) returns 
 table(routine text, testname text, result text, passed boolean, failed boolean, error_message text) as

 $code$
/*
 Тестируем функции.
 Параметр: название функции в виде "название" или "схема.название". "Название" может быть шаблоном для like.
 Тесты берутся из комменариев в тексте функций. Тест начинается со строки
 -->>>Название теста
 -- Тут собственно тест
 -- тест всего лишь не должен выбросить исключений
 -- если не выбросил - то все хорошо, если выбросил - тест не пройден
 begin
   perform 1;
 end;
 --<<<

 Тестов можен быть несколько
 -->>>Проверяем присванивание
  declare
   v text;
   begin
    v:=100;
    if v::integer<>100 then
      raise exception 'Strange behavior of assignment';
    end if;
  end;
  --<<<

  Все тесты для одной функции выполняются в одной транзакции; после выполнения тестов для этой функции транзакция для
  нее откатывается; таким образом, по завершению всех тестов откатываются все транзакции.
*/
declare
 src text;
 matches text[];
 rs record;
 rname text:=coalesce(substring(fname from $RE$\.(.+)$RE$), fname);
 sname text:=coalesce(substring(fname from $RE$^([^.]*)\.$RE$),'public');
 r record;
 result text;
 begin
  if rname is null then
    raise exception 'Wrong function name:%', fname;
  end if;
  
  for rs in (select routine_definition as def, ro.specific_schema, ro.routine_name from information_schema.routines ro where ro.specific_schema=sname and routine_name like rname) loop
    begin
       passed:=null;
       failed:=null;
       error_message:='No tests available';
       routine:=rs.specific_schema || '.' || rs.routine_name;
       testname:=null;
       for r in (select * from regexp_matches(rs.def,'[^\n]*\n?\s*-->>>([^\n]*)\n((.(?!--<<<))+)\s*--<<<[^\n]*\n','g') as ms(s)) loop
        passed:=true;
        failed:=false;
        error_message:=null;
        testname:=r.s[1];
        begin
          execute 'do $theroutenecodegoeshere$ ' || r.s[2] || ' $theroutenecodegoeshere$;';
        exception 
          when others then 
            passed:=false;
            failed:=true;
            error_message:=sqlerrm || ' sqlstate:' || sqlstate;
        end;
        return next;
       end loop;
       raise exception sqlstate 'RB999';
     exception
       when sqlstate 'RB999' then null;
    end;   
   end loop;     
 end;
$code$
language plpgsql;

CREATE OR REPLACE FUNCTION mbus.escapechars(s text)
  RETURNS text AS
$BODY$
select
replace(
  replace(
    replace(
      replace(
        replace($1,chr(10),$S$\n$S$),
              chr(9),$S$\t$S$),
            chr(13),$S$\r$S$),
          chr(8),$S$\b$S$),
        chr(12),$S$\f$S$)
$BODY$
  LANGUAGE sql IMMUTABLE STRICT
  COST 100;


CREATE OR REPLACE FUNCTION mbus.hstore2json(hs hstore)
  RETURNS text AS
$BODY$
/*
do $CODE$
-->>>hstore2json
begin
  if mbus.hstore2json(hstore('lala','dodo'))<>'{"lala":"dodo"}' or
     mbus.hstore2json(hstore('lal"a','do"do')) <> $${"lal\"a":"do\"do"}$$ then
       raise exception 'hstore2json is not passed:%', sqlerrm;
  end if;
end;
--<<<
$CODE$;
*/
declare
rv text;
r record;
count integer;
begin
count:=0;
rv:='';
for r in (select key, val from each(hs) as h(key, val)) loop
  if rv<>'' then
   rv:=rv||',';
  end if;
  rv:=rv || '"'  || regexp_replace(regexp_replace(mbus.escapechars(coalesce(r.key,'')),$$\\$$,$$\\\\$$,'g'),'"', $$\"$$,'g') || '":';
  --' just for colorer
  rv:=rv || '"' || regexp_replace(regexp_replace(mbus.escapechars(coalesce(r.val,'')),$$\\$$,$$\\\\$$,'g'),'"', $$\"$$,'g') || '"';
  --' just for colorer
  count:=count+1;
end loop;
return '{'||rv||'}';
end;
$BODY$
  LANGUAGE plpgsql IMMUTABLE
  COST 100;


CREATE OR REPLACE FUNCTION mbus.arrayofhstore2json(hs hstore[])
  RETURNS text AS
 $BODY$
/*
do $CODE$
-->>>arrayofhstore2json
begin
  if mbus.arrayofhstore2json(array[hstore('lala','dodo'), hstore('dodo','lala')])<>'[{"lala":"dodo"},{"dodo":"lala"}]' then
       raise exception 'hstore2json is not passed:%', sqlerrm;
  end if;
end;
--<<<
$CODE$
*/
   declare
      i integer;
      rv text;
   begin
     if hs is null or array_lower(hs,1) is null then
        return 'null';
     end if;
     rv:='';
     for i in array_lower(hs,1) .. array_upper(hs,1) loop
        if rv<>'' then
           rv:=rv||',';
        end if;

        rv:=rv || mbus.hstore2json(hs[i]);
     end loop;
     return '[' || rv || ']';
   end;
$BODY$
  LANGUAGE plpgsql IMMUTABLE
  COST 100;

create or replace function mbus.estimated_messages(qname text) returns bigint as
$code$
/*
do $CODE$
-->>>estimated_messages
declare
 emc bigint;
 qname text:='qqtestq';
begin
 perform mbus.create_queue(qname,8);
 perform mbus.post_qqtestq(hstore('id', n::text)) from generate_series(1,100) as gs(n);
 analyze mbus.qt$qqtestq;
 emc:=mbus.estimated_messages(qname);
 if emc is null then
   raise exception 'Cannot get estimated messages count';
 end if;  
 emc:=mbus.estimated_messages(qname||'qqq');
 if emc is not null then
   raise exception 'Can get unexpected estimated messages count';
 end if;  
end;
--<<<
$CODE$
*/

declare
 rv bigint;
 begin
   select reltuples::bigint into rv from pg_catalog.pg_class where oid=('mbus.qt$' || qname)::regclass;
   return rv;
 exception
   when sqlstate '42P01' then 
     raise notice 'Requested queue ''%'' does not exists', qname;
     return null;  
 end; 
$code$
language plpgsql;
SELECT pg_catalog.pg_extension_config_dump('qt_model', '');
SELECT pg_catalog.pg_extension_config_dump('consumer', '');
SELECT pg_catalog.pg_extension_config_dump('dmq', '');
SELECT pg_catalog.pg_extension_config_dump('queue', '');
SELECT pg_catalog.pg_extension_config_dump('trigger', '');

alter extension mbus drop sequence seq;
alter extension mbus drop sequence qt_model_id_seq;
alter extension mbus drop sequence consumer_id_seq;
alter extension mbus drop sequence queue_id_seq;

do $role$
begin
  execute 'drop role if exists mbus_' || current_database() || '_admin';
  execute 'create role mbus_' || current_database() || '_admin';
end;
$role$;
grant usage on schema mbus to public;
