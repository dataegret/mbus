
-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION mbus" to load this file. \quit


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



CREATE SEQUENCE qt_model_id_seq
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



CREATE SEQUENCE seq
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


CREATE OR REPLACE FUNCTION string_format(format text, param hstore)
RETURNS text 
LANGUAGE sql IMMUTABLE
AS
$BODY$
	/*
	formats string using template
	%[name] - inserting quote_literal(param->'<NAME>')
	%{name} - quote_ident

	select string_format('%[name] is %{value} and n=%[n] and name=%[name} %%[v]', hstore('name','lala') || hstore('value', 'The Value')||hstore('n',n::text))  from generate_series(1,1000) as gs(n); 
	*/
select 
	array_to_string(
   		array(
     			select
				case when s[1] like '^%{%}' escape '^'
				      then quote_ident($2->(substr(s[1],3,length(s[1])-3)))
				      when s[1] like '^%[%]' escape '^'
				      then quote_literal($2->(substr(s[1],3,length(s[1])-3)))
				      when s[1] like '^%<%>' escape '^'
				      then ($2->(substr(s[1],3,length(s[1])-3)))
				else
					s[1]
				end as s
      		 	from regexp_matches($1, 
                        	$RE$
                         	(
                          	% [[{<] \w+ []}>]
                          	|
                          	%%
                          	|
                          	(?: [^%]+ | %(?! [[{] ) )
                         	)
                         	$RE$,
                         	'gx')
                        as re(s)
      
		),
  	'');

$BODY$
COST 100;

CREATE FUNCTION clear_tempq() 
RETURNS void
LANGUAGE plpgsql
AS $$
begin
	delete from mbus.trigger where dst like 'temp.%' and not exists (select * from pg_stat_activity where dst like 'temp.' || md5(pid::text || backend_start::text) || '%');
	delete from mbus.tempq where not exists (select * from pg_stat_activity where (headers->'tempq') is null and (headers->'tempq') like 'temp.' || md5(pid::text || backend_start::text) || '%');
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
 	insert into mbus.consumer(name, qname, selector, added) values(cname, qname, selector, now()::timestamp without time zone) returning id into c_id;
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
 		set local enable_seqscan=off;

  		if version() like 'PostgreSQL 9.0%' then
     			for r in 
      				select * 
        				from mbus.qt$<!qname!> t
       					where <!consumer_id!><>all(received) and t.delayed_until<now() and (<!selector!>)=true and added >'<!now!>' and coalesce(expires,'2070-01-01'::timestamp) > now()::timestamp 
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
         			and pg_try_advisory_xact_lock( ('X' || md5('mbus.qt$<!qname!>.' || t.iid))::bit(64)::bigint )
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
                    and pg_try_advisory_xact_lock( ('X' || md5('mbus.qt$<!qname!>.' || t.iid))::bit(64)::bigint )
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
	execute 'create table ' || schname || '.qt$' || qname || '( like ' || schname || '.qt_model including all)';
	insert into mbus.queue(qname,consumers_cnt) values(qname,consumers_cnt);
	post_src := $post_src$
	CREATE OR REPLACE FUNCTION mbus.post_<!qname!>(data hstore, headers hstore DEFAULT NULL::hstore, properties hstore DEFAULT NULL::hstore, delayed_until timestamp without time zone DEFAULT NULL::timestamp without time zone, expires timestamp without time zone DEFAULT NULL::timestamp without time zone)
  	RETURNS text AS
	$BDY$
 		notify QN_<!qname!>;
 		select mbus.run_trigger('<!qname!>', $1, $2, $3, $4, $5);
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
                                case when $2 is null then hstore('seenby','{' || current_database() || '}') else hstore('seenby', (($2->'seenby')::text[] || current_database()::text)::text) end,
                                $3,
                                coalesce($4, now() - '1h'::interval),
                                $5, 
                                now(), 
                                current_database() || '.' || nextval('mbus.seq') || '.' || txid_current() || '.' || md5($1::text), 
                                array[]::int[] 
                               ) returning iid;
	$BDY$
  	LANGUAGE sql VOLATILE
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
        	raise notice 'WARNING: source queue (%) does not exists (yet?)', src;
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
       execute 'drop function mbus.' || r.proname || '(hstore, hstore, hstore, timestamp without time zone, timestamp without time zone)';
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
           and pg_try_advisory_xact_lock( ('X' || md5('mbus.qt$$QRY$ || qname ||$QRY$.' || t.iid))::bit(64)::bigint )
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
                                current_database() || '.' || nextval('mbus.seq') || '.' || txid_current() || '.' || md5($1::text), 
                                array[]::int[] 
                               ) returning iid;

$_$;



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
begin
 for r in select * from mbus.queue loop
   post_qry:=post_qry || $$ when '$$ || lower(r.qname) || $$' then return mbus.post_$$ || r.qname || '(data, headers, properties, delayed_until, expires);'||chr(10);
 end loop;
 
 if post_qry='' then
        begin
          execute 'drop function mbus.post(text, hstore, hstore, hstore, timestamp, timestamp)';
        exception when others then null;
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
 end loop;

 if consume_qry<>'' then
    consume_qry:=consume_qry || ' else raise exception $$unknown consumer:%$$, consumer; end case;' || chr(10);
 end if;

 if consume_qry='' then 
        begin
          execute 'drop function mbus.consume(text, text)';
        exception when others then null;
        end;
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






