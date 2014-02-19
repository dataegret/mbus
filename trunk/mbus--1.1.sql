-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION mbus" to load this file. \quit

SET lc_messages to 'en_US.UTF-8';

CREATE SEQUENCE seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE qt_model_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;



CREATE TABLE qt_model (
	id integer NOT NULL,
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
    consumers_cnt integer
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

create or replace function _is_superuser() returns boolean as
$code$
   select rolsuper from pg_catalog.pg_roles where rolname=session_user;
$code$
language sql;

create or replace function mbus.can_consume(qname text, cname text default 'default') returns boolean as
$code$
   select mbus._is_superuser() or pg_has_role(session_user,'mbus_' || current_database() || '_consume_' || $1 || '_by_' || $2,'usage')::boolean;
$code$
language sql;

create or replace function can_post(qname text) returns boolean as
$code$
   select mbus._is_superuser() or pg_has_role(session_user,'mbus_' || current_database() || '_post_' || $1,'usage')::boolean;
$code$
language sql;


create or replace function mbus._should_be_able_to_consume(qname text, cname text default 'default') returns void as
$code$
begin
  if not mbus._is_superuser() and not mbus.can_consume(qname, cname) then
    raise exception 'Access denied';
  end if;
end;
$code$
language plpgsql;

create or replace function mbus._should_be_able_to_post(qname text) returns void as
$code$
begin
  if not mbus._is_superuser() and not mbus.can_post(qname) then
    raise exception 'Access denied';
  end if;
end;
$code$
language plpgsql;


create or replace function mbus._create_consumer_role(qname text, cname text) returns void as
$code$
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
end;
$code$
language plpgsql;

create or replace function mbus._create_queue_role(qname text) returns void as
$code$
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
end;
$code$
language plpgsql;


create or replace function mbus._drop_consumer_role(qname text, cname text) returns void as
$code$
begin
  if cname ~ $RE$\W$RE$ or length(cname)>32 then
      raise exception 'Wrong consumer name:%', cname;
  end if;
  execute 'drop role mbus_' || current_database() || 'consume_' || qname || '_by_' || cname;
end;
$code$
language plpgsql;

create or replace function mbus._drop_queue_role(qname text) returns void as
$code$
declare
 r record;
begin
  execute 'drop role mbus_' || current_database() || '_post_' || qname;

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
                then coalesce(quote_html(($2->(substr(s[1],3,length(s[1])-3)))),'')
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
--'
create or replace function raise_exception(exc text) returns void as
$code$
 begin
  raise exception '%', exc;
 end;
$code$
language plpgsql;


CREATE FUNCTION clear_tempq() 
RETURNS void
LANGUAGE plpgsql
AS $$
begin
	delete from mbus.trigger where dst like 'temp.%' and not exists (select * from pg_stat_activity where dst like 'temp.' || md5(pid::text || backend_start::text) || '%');
	delete from mbus.tempq where not exists (select * from pg_stat_activity where (headers->'tempq') like 'temp.' || md5(pid::text || backend_start::text) || '.%');
end;
$$;





CREATE FUNCTION consume(qname text, cname text DEFAULT 'default'::text) 
RETURNS SETOF qt_model
LANGUAGE plpgsql
AS $_$
begin
	if qname like 'temp.%' then
        	return query select * from mbus.consume_temp(qname);
            	return;
        end if;
        case lower(qname)
        	when 'work' then case lower(cname)  when 'default' then return query select * from mbus.consume_work_by_default(); return; when 'def2' then return query select * from mbus.consume_work_by_def2(); return; else raise exception $$unknown consumer:%$$, consumer; end case;

        end case;
end;
$_$;



CREATE FUNCTION consume_temp(tqname text) 
RETURNS SETOF qt_model
LANGUAGE plpgsql
AS $$
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
AS $_$
declare
	c_id integer;
	cons_src text;
	consn_src text;
	take_src text;
	ind_src text;
	selector text;
	nowtime text:=(now()::text)::timestamp without time zone;
begin
	selector := case when p_selector is null or p_selector ~ '^ *$' then '1=1' else p_selector end;
        if not exists(select * from mbus.queue q where q.qname=create_consumer.qname) then
	    raise exception 'Wrong queue name:%', create_consumer.qname;
	end if;
 	insert into mbus.consumer(name, qname, selector, added) values(cname, qname, selector, now()::timestamp without time zone) returning id into c_id;
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
 		perform mbus._should_be_able_to_consume('<!qname!>', '<!cname!>');

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
	SECURITY DEFINER
	SET search_path = mbus, pg_temp
	SET enable_seqscan = off
	$CONS_SRC$; 

	cons_src:=regexp_replace(cons_src,'<!qname!>', qname, 'g');
	cons_src:=regexp_replace(cons_src,'<!cname!>', cname,'g');
	cons_src:=regexp_replace(cons_src,'<!consumers!>', (select consumers_cnt::text from mbus.queue q where q.qname=create_consumer.qname),'g');
	cons_src:=regexp_replace(cons_src,'<!consumer_id!>',c_id::text,'g');
	cons_src:=regexp_replace(cons_src,'<!selector!>',selector,'g');
	cons_src:=regexp_replace(cons_src,'<!now!>',nowtime,'g');
	cons_src:=regexp_replace(cons_src,'<!c_id!>',c_id::text,'g');
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
	set local enable_seqscan=off;

 	perform mbus._should_be_able_to_consume('<!qname!>', '<!cname!>');
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
                    and t.iid not in (select a.iid from unnest(rvarr) as a)
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
SECURITY DEFINER
SET search_path = mbus, pg_temp
SET enable_seqscan = off
$CONSN_SRC$; 

 consn_src:=regexp_replace(consn_src,'<!qname!>', qname, 'g');
 consn_src:=regexp_replace(consn_src,'<!cname!>', cname,'g');
 consn_src:=regexp_replace(consn_src,'<!consumers!>', (select consumers_cnt::text from mbus.queue q where q.qname=create_consumer.qname),'g');
 consn_src:=regexp_replace(consn_src,'<!consumer_id!>',c_id::text,'g');
 consn_src:=regexp_replace(consn_src,'<!selector!>',selector,'g');
 consn_src:=regexp_replace(consn_src,'<!now!>',nowtime,'g');
 consn_src:=regexp_replace(consn_src,'<!c_id!>',c_id::text,'g');
 execute consn_src;

 take_src:=$TAKE$
 create or replace function mbus.take_from_<!qname!>_by_<!cname!>(msgid text)
  returns mbus.qt_model as
  $PRC$
     update mbus.qt$<!qname!> t set received=received || <!consumer_id!> where iid=$1 and <!c_id!> <> ALL(received) returning *;
  $PRC$
  language sql;
 $TAKE$;
 take_src:=regexp_replace(take_src,'<!qname!>', qname, 'g');
 take_src:=regexp_replace(take_src,'<!cname!>', cname,'g');
 take_src:=regexp_replace(take_src,'<!consumers!>', (select consumers_cnt::text from mbus.queue q where q.qname=create_consumer.qname),'g');
 take_src:=regexp_replace(take_src,'<!consumer_id!>',c_id::text,'g');
 take_src:=regexp_replace(take_src,'<!c_id!>',c_id::text,'g');

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
 perform mbus.regenerate_functions();
end;
$_$;


CREATE FUNCTION create_queue(qname text, consumers_cnt integer) 
RETURNS void
LANGUAGE plpgsql
AS $_$
declare
	schname text:= 'mbus';
	post_src text;
	clr_src text;
	peek_src text;
begin
	if length(qname)>32 or qname ~ $RE$\W$RE$ then
	   raise exception 'Name % too long (32 chars max)', qname;
	end if;
	--qname:=lower(qname);
	execute 'create table ' || schname || '.qt$' || qname || '( like ' || schname || '.qt_model including all)';
	perform mbus._create_queue_role(qname);
	insert into mbus.queue(qname,consumers_cnt) values(qname,consumers_cnt);
	post_src := $post_src$
        CREATE OR REPLACE FUNCTION mbus.post_<!qname!>(data hstore, headers hstore DEFAULT NULL::hstore, properties hstore DEFAULT NULL::hstore, delayed_until timestamp without time zone DEFAULT NULL::timestamp without time zone, expires timestamp without time zone DEFAULT NULL::timestamp without time zone, iid text default null)
  	RETURNS text AS
	$BDY$
	        select mbus._should_be_able_to_post('<!qname!>');
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
                                hstore('enqueue_time',now()::timestamp::text) ||
                                hstore('source_db', current_database())       ||
                                hstore('destination_queue', $Q$<!qname!>$Q$)       ||
                                coalesce($2,''::hstore)||
                                case when $2 is not null and exist($2,'consume_after')  then hstore('consume_after', ($2->'consume_after')::text[]::text) else ''::hstore end ||
                                case when $2 is null or not exist($2,'seenby') then hstore('seenby', array[ current_database() ]::text) else hstore('seenby', (($2->'seenby')::text[] || array[current_database()::text])::text) end,
                                $3,
                                coalesce($4, now() - '1h'::interval),
                                $5, 
                                now(), 
                                coalesce($6, $Q$<!qname!>$Q$ || '.' || nextval('mbus.seq')),
                                array[]::int[] 
                               ) returning iid;
	$BDY$
  	LANGUAGE sql VOLATILE
  	SECURITY DEFINER
	SET search_path = mbus, pg_temp
	SET enable_seqscan = off
  	COST 100;
	$post_src$;
 	post_src:=regexp_replace(post_src,'<!qname!>', qname, 'g');
 	execute post_src;

 	clr_src:=$CLR_SRC$
	create or replace function mbus.clear_queue_<!qname!>()
	returns void as
	$ZZ$
	declare
		qry text;
	begin
 		select string_agg( 'select id from mbus.consumer where id=' || id::text ||' and ( (' || r.selector || ')' || (case when r.added is null then ')' else $$ and q.added > '$$ || (r.added::text) || $$'::timestamp without time zone)$$ end) ||chr(10), ' union all '||chr(10))
  	into qry
  		from mbus.consumer r where qname='<!qname!>';
 	execute 'delete from mbus.qt$<!qname!> q where expires < now() or (received <@ array(' || qry || '))';
	end; 
	$ZZ$
	language plpgsql
	$CLR_SRC$;

 	clr_src:=regexp_replace(clr_src,'<!qname!>', qname, 'g');
 	execute clr_src;

 	peek_src:=$PEEK$
  	create or replace function mbus.peek_<!qname!>(msgid text default null)
  	returns boolean as
  	$PRC$
   	select case 
              when $1 is null then exists(select * from mbus.qt$<!qname!>)
              else exists(select * from mbus.qt$<!qname!> where iid=$1)
        end;
  	$PRC$
  	language sql
 	$PEEK$;
 	peek_src:=regexp_replace(peek_src,'<!qname!>', qname, 'g');
 	execute peek_src;
 
 	perform mbus.create_consumer('default',qname);
 	perform mbus.regenerate_functions(); 
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



CREATE FUNCTION create_view(qname text, cname text DEFAULT 'default'::text) RETURNS void
    LANGUAGE plpgsql
    AS $_$
declare
	param hstore:=hstore('qname',qname)||hstore('cname',cname);
begin
	execute string_format($STR$ create view %<qname>_q as select data from mbus.consume('%<qname>', '%<cname>')$STR$, param);
	execute string_format($STR$
	create or replace function trg_post_%<qname>() returns trigger as
	$thecode$
	begin
 		perform mbus.post('%<qname>',new.data);
 		return null;
	end;
	$thecode$
	security definer
	SET search_path = mbus, pg_temp
	language plpgsql;

	create trigger trg_%<qname>  instead of insert on %<qname>_q for each row execute procedure trg_post_%<qname>();   
  	$STR$, param);
end;
$_$;



CREATE FUNCTION create_view(qname text, cname text, sname text, viewname text) RETURNS void
    LANGUAGE plpgsql
    AS $_$
declare
	param hstore:=hstore('qname',qname)||hstore('cname',cname)|| hstore('sname',sname||'.')|| hstore('viewname', coalesce(viewname, 'public.'||qname));
begin
	execute string_format($STR$ create view %<sname>%<viewname> as select data from mbus.consume('%<qname>', '%<cname>')$STR$, param);
	execute string_format($STR$
	create or replace function %<sname>trg_post_%<viewname>() returns trigger as
	$thecode$
	begin
		perform mbus.post('%<qname>',new.data);
 		return null;
	end;
	$thecode$
	security definer
	SET search_path = mbus, pg_temp
	language plpgsql;

	create trigger trg_%<qname>  instead of insert on %<sname>%<viewname> for each row execute procedure %<sname>trg_post_%<viewname>();   
	$STR$, param);
end;
$_$;

CREATE FUNCTION create_view_prop(qname text, cname text, sname text, viewname text) RETURNS void
    LANGUAGE plpgsql
    AS $_$
declare
	param hstore := hstore('qname',qname)||hstore('cname',cname)|| hstore('sname',sname||'.')|| hstore('viewname', coalesce(viewname, 'public.'||qname));
begin
	execute mbus.string_format($STR$ create view %<sname>%<viewname> as select data, properties from mbus.consume('%<qname>', '%<cname>')$STR$, param);
	execute mbus.string_format($STR$
	create or replace function %<sname>trg_post_%<viewname>() returns trigger as
	$thecode$
	begin
		perform mbus.post_%<qname>(new.data, null::hstore, new.properties, null::timestamp, null::timestamp);
 		return null;
	end;
	$thecode$
	security definer
	SET search_path = mbus, pg_temp
	language plpgsql;

	create trigger trg_%<qname>  instead of insert on %<sname>%<viewname> for each row execute procedure %<sname>trg_post_%<viewname>();   
	$STR$, param);
end;
$_$;


CREATE FUNCTION create_view_prop(qname text, cname text, sname text, viewname text, with_delay boolean default false, with_expire boolean default false) RETURNS void
    LANGUAGE plpgsql
    AS $_$
declare
	param hstore := hstore('qname',qname)||hstore('cname',cname)|| hstore('sname',sname||'.')|| hstore('viewname', coalesce(viewname, 'public.'||qname));
begin
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
	SET search_path = mbus, pg_temp
	language plpgsql;

	create trigger trg_%<qname>  instead of insert on %<sname>%<viewname> for each row execute procedure %<sname>trg_post_%<viewname>();   
	$STR$, param);
end;
$_$;

create or replace function queue_acl( oper text, usr text, qname text, viewname text, schemaname text)
returns void 
language plpgsql as
$_$
declare
	param hstore := hstore('oper', oper) || hstore('qname', qname) || hstore('usr', usr ) || hstore('viewname', viewname) || hstore('schemaname', schemaname);
	l_func text;
begin
/*
	execute string_format($STR$

	$STR$, param);
*/
	if lower(oper) = 'grant' then
		param := param || hstore('dir', 'to');
	elsif lower(oper) = 'revoke' then
		param := param || hstore('dir', 'from');
	else
		return;
	end if;

	execute string_format($SCH$ %<oper> usage on schema mbus %<dir> %<usr>$SCH$, param);
	execute string_format($VSCH$ %<oper> usage on schema %<schemaname> %<dir> %<usr>$VSCH$, param);
	execute string_format($VIW$ %<oper> insert,select on %<schemaname>.%<viewname> %<dir> %<usr>$VIW$, param);
	execute string_format($TBL$ %<oper> all on mbus.qt$%<qname> %<dir> %<usr>$TBL$, param);
	execute string_format($FNC$   
		with f as ( 
			SELECT n.nspname ||  '.' || p.proname || '(' || pg_catalog.pg_get_function_identity_arguments(p.oid) || ')' as func
				FROM pg_catalog.pg_proc p
				     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
				WHERE n.nspname ~ '^(mbus)$' and p.proname ~ '%<qname>'
			)
			select array_to_string( array_agg(func), ',') from f 
	$FNC$, param) into l_func;
	
	param := param || hstore('l_func', l_func);
	execute string_format($FNCL$%<oper> execute on function %<l_func> %<dir> %<usr>$FNCL$, param);
	
end;
$_$;


CREATE FUNCTION drop_consumer(cname text, qname text) RETURNS void
    LANGUAGE plpgsql
    AS $_$
 begin
   delete from mbus.consumer c where c.name=drop_consumer.cname and c.qname=drop_consumer.qname;
   execute 'drop index mbus.qt$' || qname || '_for_' || cname;
   execute 'drop function mbus.consume_' || qname || '_by_' || cname || '()';
   execute 'drop function mbus.consumen_' || qname || '_by_' || cname || '(integer)';
   execute 'drop function mbus.take_from_' || qname || '_by_' || cname || '(text)';
   perform mbus._drop_consumer_role(qname, cname);
   perform mbus.regenerate_functions();
 end;
$_$;



CREATE FUNCTION drop_queue(qname text) RETURNS void
    LANGUAGE plpgsql
    AS $_X$
declare
 r record;
begin
 execute 'drop table mbus.qt$' || qname || ' cascade';

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
       execute 'drop function mbus.' || r.proname || '(hstore, hstore, hstore, timestamp without time zone, timestamp without time zone, text)';
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
  perform mbus.regenerate_functions();  
end;
$_X$;



CREATE FUNCTION drop_trigger(src text, dst text) RETURNS void
    LANGUAGE plpgsql
    AS $$
 begin
  delete from mbus.trigger where trigger.src=drop_trigger.src and trigger.dst=drop_trigger.dst;
  if found then
    begin
      execute 'drop function mbus.trigger_' || src || '_to_' || dst ||'(mbus.qt_model)';
    exception
     when sqlstate '42000' then null;
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



CREATE FUNCTION dyn_consume(qname text, selector text DEFAULT '(1=1)'::text, cname text DEFAULT 'default'::text) RETURNS SETOF qt_model
    LANGUAGE plpgsql
    AS $_$
declare
 rv mbus.qt_model;
 consid integer;
 consadded timestamp;
 hash text:='_'||md5(coalesce(selector,''));
begin
 set local enable_seqscan=off;
 if selector is null then
   selector:='(1=1)';
 end if;
 select id, added into consid, consadded from mbus.consumer c where c.qname=dyn_consume.qname and c.name=dyn_consume.cname;
  begin
   execute 'execute /**/ mbus_dyn_consume_'||qname||hash||'('||consid||','''||consadded||''')' into rv;
  exception
    when sqlstate '26000' then
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
$_$;



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
                                now(), 
                                current_database() || '.' || nextval('mbus.seq'), 
                                array[]::int[] 
                               ) returning iid;

$_$;

create function get_iid(qname text) returns text as
$code$
   select $1 || nextval('mbus.seq');
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
  mbus.create_queue(qname, ncons)
  где
  qname - имя очереди. Допустимы a-z (НЕ A-Z!), _, 0-9
  ncons - число одновременно доступных частей. Разумные значения - от 2 до 128-256
  больше ставить можно, но тогда будут слишком большие задержки на перебор всех частей
 
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



CREATE FUNCTION regenerate_functions() RETURNS void
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

 
 if post_qry='' then
        begin
          execute 'drop function mbus.post(text, hstore, hstore, hstore, timestamp, timestamp)';
          execute 'create or replace function mbus.is_msg_exists(msgiid text) returns boolean $code$ select false; $code$ language sql';
        end;
 else
        execute $FUNC$
        ---------------------------------------------------------------------------------------------------------------------------------------------
        create or replace function mbus.post(qname text, data hstore, headers hstore default null, properties hstore default null, delayed_until timestamp default null, expires timestamp default null)
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
        $FUNC$;

        execute $FUNC$
                create or replace function mbus.peek(msgiid text) returns mbus.qt_model as
                $code$
                 select coalesce(
                    case $FUNC$
                     || peek_qry ||
                    $FUNC$
                    else
                     null
                    end, (select row(q.*)::mbus.qt_model from mbus.dmq q where q.iid=$1));
                $code$
                language sql
        $FUNC$;

        execute $FUNC$
                create or replace function mbus.take(msgiid text) returns mbus.qt_model as
                $code$
                declare
                  rv mbus.qt_model;
                begin
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
 end loop;

 if consume_qry<>'' then
    consume_qry:=consume_qry || ' else raise exception $$unknown consumer:%$$, consumer; end case;' || chr(10);
 end if;

 if consume_qry='' then 
        execute 'drop function mbus.consume(text, text)';
 else 
        execute $FUNC$
        create or replace function mbus.consume(qname text, cname text default 'default') returns setof mbus.qt_model as
        $QQ$
        begin
          if qname like 'temp.%' then
            return query select * from mbus.consume_temp(qname);
            return;
          end if;
         case lower(qname)
        $FUNC$ || consume_qry || $FUNC$
         end case;
        end;
        $QQ$
        language plpgsql;
        $FUNC$;
 end if;
 
end;
$_X$;



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