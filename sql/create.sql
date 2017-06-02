/*
This file is part of Guru Cue Search & Recommendation Engine.
Copyright (C) 2017 Guru Cue Ltd.

Guru Cue Search & Recommendation Engine is free software: you can
redistribute it and/or modify it under the terms of the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

Guru Cue Search & Recommendation Engine is distributed in the hope
that it will be useful, but WITHOUT ANY WARRANTY; without even the
implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with Guru Cue Search & Recommendation Engine. If not, see
<http://www.gnu.org/licenses/>.
*/

/*
To create a database run the following two lines in pgsql, and then input this file.
create database demo with encoding 'UTF8' template template0;
\c demo
*/

create extension btree_gin;

\set AUTOCOMMIT off

create domain r_id as bigint;
create domain r_varchar255 as varchar(255);
create domain r_varchar200 as varchar(200);
create domain r_varchar100 as varchar(100);
create domain r_varchar40 as varchar(40);
create domain r_varchar30 as varchar(30);
create domain r_varchar10 as varchar(10);
create domain r_int as integer;
create domain r_bool as smallint check (value is null or value in (0, 1));
create domain r_text as text;
create domain r_timestamp as timestamp;
create domain r_vote as decimal(2,1);
create domain r_recommendation as decimal(3,2);
create domain r_short as smallint;
create domain r_varchar31 as varchar(31);
create domain r_varchar1024 as varchar(1024);
create domain r_char1 as char(1);
create domain r_varchar50 as varchar(50);
create domain r_text256 as text;
create domain r_milliseconds as BigInt;
create domain r_double as Double precision;
create domain r_varchar400 as text;

create sequence attribute_id_seq;
create sequence consumer_id_seq;
create sequence partner_id_seq;
create sequence product_id_seq;
create sequence property_id_seq;
create sequence language_id_seq;
create sequence product_type_id_seq;
create sequence consumer_event_id_seq;
create sequence consumer_event_type_id_seq;
create sequence data_type_id_seq;
create sequence recommender_id_seq;
create sequence log_svc_rec_id_seq;
create sequence partner_recommender_id_seq;
create sequence rec_consumer_override_id_seq;
create sequence relation_type_id_seq;
create sequence relation_consumer_product_id_seq;
create sequence log_product_id_seq;
create sequence log_svc_search_id_seq;
create sequence mkt_def_id_seq;

/* main entities */

create table language (
    id r_id not null default nextval('language_id_seq'),
    iso639_2t r_varchar10,
    iso639_1 r_varchar10,
    constraint language_pk primary key (id),
    constraint language_uq1 unique (iso639_2t),
    constraint language_uq2 unique (iso639_1)
);

create table product_type (
    id r_id not null default nextval('product_type_id_seq'),
    identifier r_varchar30 not null,
    constraint product_type_pk primary key (id),
    constraint product_type_uq1 unique (identifier)
);

create table partner (
    id r_id not null default nextval('partner_id_seq'),
    name r_varchar100 not null,
    username r_varchar100 not null,
    login_suffix r_varchar100,
    language_id r_id not null,
    constraint partner_pk primary key (id),
    constraint partner_fk1 foreign key (language_id) references language (id) on delete no action on update cascade,
    constraint partner_uq1 unique (name),
    constraint partner_uq2 unique (username)
);

create table product (
    id r_id not null default nextval('product_id_seq'),
    partner_id r_id not null,
    product_type_id r_id not null,
    partner_product_code r_varchar100,
    last_log_id r_id,
    added timestamp not null default now(),
    modified timestamp not null default now(),
    deleted timestamp,
    related jsonb not null default '{}'::jsonb,
    attributes jsonb not null default '{}'::jsonb,
    constraint product_pk primary key (id),
    constraint product_fk1 foreign key (partner_id) references partner (id) on delete no action on update cascade,
    constraint product_fk2 foreign key (product_type_id) references product_type (id) on delete no action on update cascade
);

create table consumer (
    id r_id not null default nextval('consumer_id_seq'),
    username r_varchar100,
    partner_id r_id,
    activated r_timestamp default (current_timestamp at time zone 'utc'),
    deleted r_timestamp,
    status r_short not null default 1,
    type_id r_id not null,
    parent_id r_id,
    info jsonb,
    constraint consumer_pk primary key (id),
    constraint consumer_fk1 foreign key (partner_id) references partner (id) on delete set null on update cascade,
    constraint consumer_fk2 foreign key (parent_id) references consumer (id) on update cascade on delete cascade
);

create table property (
    id r_id not null default nextval('property_id_seq'),
    identifier r_varchar30 not null,
    constraint property_pk primary key (id),
    constraint property_uq1 unique (identifier)
);

create table relation_type (
    id r_id not null default nextval('relation_type_id_seq'),
    identifier r_varchar40 not null,
    constraint relation_type_pk primary key (id),
    constraint relation_type_uq1 unique (identifier)
);

create table attribute (
    id r_id not null default nextval('attribute_id_seq'),
    identifier r_varchar30 not null,
    value_type r_int not null, /* TODO: put a value range restriction on it */
    is_translatable r_bool not null,
    is_multivalue r_bool not null,
    is_private r_bool not null,
    constraint attribute_pk primary key (id),
    constraint attribute_uq1 unique (identifier)
);

create table consumer_event_type (
    id r_id not null default nextval('consumer_event_type_id_seq'),
    identifier r_varchar30 not null,
    constraint consumer_event_type_pk primary key (id),
    constraint consumer_event_type_uq1 unique (identifier)
);

create table data_type (
    id r_id not null default nextval('data_type_id_seq'),
    identifier r_varchar30 not null,
    constraint data_type_pk primary key (id),
    constraint data_type_uq1 unique (identifier)
);

create table recommender (
    id r_id not null default nextval('recommender_id_seq'),
    name r_varchar50 not null,
    hostname r_varchar100,
    constraint recommender_pk primary key (id),
    constraint recommender_uq1 unique (name)
);

create table mkt_def (
  id r_id not null default nextval('mkt_def_id_seq'),
  partner_id r_id not null,
  src_type r_short not null,
  name r_text not null,
  data jsonb not null,
  constraint mkt_def_pk primary key (id),
  constraint mkt_def_fk1 foreign key (partner_id) references partner (id) on delete cascade on update cascade
);

/* connecting entities */

create table consumer_property (
    consumer_id r_id not null,
    property_id r_id not null,
    property_value r_varchar200,
    constraint consumer_property_pk primary key (consumer_id, property_id),
    constraint consumer_property_fk1 foreign key (consumer_id) references consumer (id) on delete cascade on update cascade,
    constraint consumer_property_fk2 foreign key (property_id) references property (id) on delete cascade on update cascade
);

create table consumer_event (
    id r_id not null default nextval('consumer_event_id_seq'),
    event_timestamp r_timestamp,
    partner_id r_id,
    consumer_id r_id,
    product_id r_id,
    event_type_id r_id,
    user_profile_id r_id,
    response_code r_id,
    failure_condition r_text256,
    failed_request r_text256,
    request_timestamp r_timestamp,
    request_duration_ms r_milliseconds,
    info jsonb,
/* foreign keys are disabled for performance reasons; if you enable them, then also enable corresponding indices below */
--    constraint consumer_event_fk1 foreign key (partner_id) references partner (id) on delete cascade on update cascade,
--    constraint consumer_event_fk2 foreign key (consumer_id) references consumer (id) on delete cascade on update cascade,
--    constraint consumer_event_fk3 foreign key (product_id) references product (id) on delete cascade on update cascade,
--    constraint consumer_event_fk4 foreign key (event_type_id) references consumer_event_type (id) on delete no action on update cascade,
--    constraint consumer_event_fk5 foreign key (user_profile_id) references consumer (id) on delete cascade on update cascade,
    constraint consumer_event_pk primary key (id)
);

create table consumer_event_type_data (
    event_type_id r_id not null,
    data_type_id r_id not null,
    is_required r_bool not null,
    constraint consumer_event_type_data_pk primary key (event_type_id, data_type_id),
    constraint consumer_event_type_data_fk1 foreign key (event_type_id) references consumer_event_type (id) on delete no action on update cascade,
    constraint consumer_event_type_data_fk2 foreign key (data_type_id) references data_type (id) on delete no action on update cascade
);

create table recommender_setting (
    recommender_id r_id not null,
    setting_name r_varchar50 not null,
    setting_value r_text not null,
    constraint recommender_setting_pk primary key (recommender_id, setting_name),
    constraint recommender_setting_fk1 foreign key (recommender_id) references recommender (id) on delete cascade on update cascade
);

create table partner_recommender (
    id r_id not null default nextval('partner_recommender_id_seq'),
    partner_id r_id not null,
    recommender_id r_id not null,
    name r_varchar50 not null,
    constraint partner_recommender_pk primary key (id),
    constraint partner_recommender_fk1 foreign key (partner_id) references partner (id) on update cascade on delete cascade,
    constraint partner_recommender_fk2 foreign key (recommender_id) references recommender (id) on update cascade on delete cascade,
    constraint partner_recommender_uq1 unique (partner_id, name)
);

create table recommender_consumer_override (
  id r_id not null default nextval('rec_consumer_override_id_seq'),
  consumer_id r_id not null,
  original_partner_recommender_id r_id not null,
  override_partner_recommender_id r_id not null,
  constraint rec_consumer_override_pk primary key (id),
  constraint rec_consumer_override_fk1 foreign key (consumer_id) references consumer (id) on update cascade on delete cascade,
  constraint rec_consumer_override_fk2 foreign key (original_partner_recommender_id) references partner_recommender (id) on update cascade on delete cascade,
  constraint rec_consumer_override_fk3 foreign key (override_partner_recommender_id) references partner_recommender (id) on update cascade on delete cascade
);

create table relation_consumer_product (
    id r_id not null default nextval('relation_consumer_product_id_seq'),
    consumer_id r_id not null,
    product_id r_id not null,
    relation_type_id r_id not null,
    relation_start r_timestamp,
    relation_end r_timestamp,
    added r_timestamp not null default (current_timestamp at time zone 'utc'),
    deleted r_timestamp,
    constraint relation_consumer_product_pk primary key (id),
    constraint relation_consumer_product_fk1 foreign key (consumer_id) references consumer (id) on delete cascade on update cascade,
    constraint relation_consumer_product_fk2 foreign key (product_id) references product (id) on delete cascade on update cascade,
    constraint relation_consumer_product_fk3 foreign key (relation_type_id) references relation_type (id) on delete cascade on update cascade
);

create table product_type_attribute (
    product_type_id r_id not null,
    attribute_id r_id not null,
    ident_level r_short default 0 not null,
    constraint product_type_attribute_pk primary key (product_type_id, attribute_id),
    constraint product_type_attribute_fk1 foreign key (product_type_id) references product_type (id) on delete cascade on update cascade,
    constraint product_type_attribute_fk2 foreign key (attribute_id) references attribute (id) on delete cascade on update cascade
);


/* service usage logging tables */

create table log_svc_rec (
    id r_id not null default nextval('log_svc_rec_id_seq'),
    partner_id r_id not null,
    consumer_id r_id,
    max_recommendations r_int,
    resp_code r_int not null,
    failure_condition r_text256,
    failed_request r_text256,
    request_timestamp r_timestamp not null,
    request_duration_ms r_milliseconds,
    partner_recommender_name r_varchar50,
    blender_name r_varchar100,
    ret_products jsonb,
    data jsonb,
    candidates jsonb,
    ref_products jsonb,
    attributes jsonb,
    blender_feedback jsonb,
    constraint log_svc_rec_pk primary key (id),
    constraint log_svc_rec_fk1 foreign key (partner_id) references partner (id) on delete no action on update cascade,
    constraint log_svc_rec_fk2 foreign key (consumer_id) references consumer (id) on delete no action on update cascade
);

create table log_svc_search (
    id r_id not null default nextval('log_svc_search_id_seq'),
    partner_id r_id not null,
    consumer_id r_id,
    max_items r_int,
    resp_code r_int not null,
    failure_condition r_text256,
    failed_request r_text256,
    request_timestamp r_timestamp not null,
    request_duration_ms r_milliseconds,
    requested_blender_name r_varchar100,
    blender_name r_varchar100,
    ret_products jsonb,
    data jsonb,
    candidates jsonb,
    ref_products jsonb,
    attributes jsonb,
    blender_feedback jsonb,
    constraint log_svc_search_pk primary key (id),
    constraint log_svc_search_fk1 foreign key (partner_id) references partner (id) on delete no action on update cascade,
    constraint log_svc_search_fk2 foreign key (consumer_id) references consumer (id) on delete no action on update cascade
);

/* product change logging tables */

create table log_product (
    log_id r_id not null default nextval('log_product_id_seq'),
    valid_from timestamp default (current_timestamp at time zone 'utc') not null,
    valid_till timestamp,
    id r_id not null,
    partner_id r_id not null,
    product_type_id r_id not null,
    partner_product_code r_varchar100,
    related jsonb,
    attributes jsonb,
    constraint log_product_pk primary key (log_id)
);

commit;

/* foreign key indices */

create index partner_fkidx1 on partner (language_id);

create index product_fkidx1 on product (partner_id);
create index product_fkidx2 on product (product_type_id);

create index consumer_fkidx1 on consumer (partner_id);
create index consumer_fkidx2 on consumer (parent_id);

create index consumer_property_fkidx1 on consumer_property (consumer_id);
create index consumer_property_fkidx2 on consumer_property (property_id);

/* indices for consumer_event - enable only if you also enabled the corresponding foreign keys */
--create index consumer_event_fkidx1 on consumer_event (partner_id);
--create index consumer_event_fkidx2 on consumer_event (consumer_id);
create index consumer_event_fkidx3 on consumer_event (product_id);
--create index consumer_event_fkidx4 on consumer_event (event_type_id);
--create index consumer_event_fkidx5 on consumer_event (user_profile_id);

create index consumer_event_type_data_fkidx1 on consumer_event_type_data (event_type_id);
create index consumer_event_type_data_fkidx2 on consumer_event_type_data (data_type_id);

create index recommender_setting_fkidx1 on recommender_setting (recommender_id);

create index partner_recommender_fkidx1 on partner_recommender (partner_id);
create index partner_recommender_fkidx2 on partner_recommender (recommender_id);

create index recommender_consumer_override_fkidx1 on recommender_consumer_override (consumer_id);
create index recommender_consumer_override_fkidx2 on recommender_consumer_override (original_partner_recommender_id);
create index recommender_consumer_override_fkidx3 on recommender_consumer_override (override_partner_recommender_id);

create index relation_consumer_product_fkidx1 on relation_consumer_product (consumer_id);
create index relation_consumer_product_fkidx2 on relation_consumer_product (product_id);
create index relation_consumer_product_fkidx3 on relation_consumer_product (relation_type_id);

create index log_svc_rec_fkidx1 on log_svc_rec (partner_id);
create index log_svc_rec_fkidx2 on log_svc_rec (consumer_id);

create index product_type_attribute_fkidx1 on product_type_attribute (product_type_id);
create index product_type_attribute_fkidx2 on product_type_attribute (attribute_id);

create index log_svc_search_fkidx1 on log_svc_search (partner_id);
create index log_svc_search_fkidx2 on log_svc_search (consumer_id);

/* indices */

create index consumer_event_idx1 on consumer_event (event_timestamp);
create index consumer_event_idx2 on consumer_event (consumer_id, event_timestamp);
create index consumer_event_idx3 on consumer_event (request_timestamp);

create index log_svc_rec_idx1 on log_svc_rec (request_timestamp, resp_code);
create index log_svc_rec_idx2 on log_svc_rec (consumer_id, request_timestamp, resp_code);

create index consumer_idx1 on consumer (activated);
create index consumer_idx2 on consumer (partner_id, username, status);
create unique index consumer_uq1 on consumer (partner_id, username) where status = 1 and type_id = 1;

create index product_idx1 on product (partner_id, product_type_id, partner_product_code);
create index product_idx2 on product (partner_id, partner_product_code) where product_type_id = 4;
create index tv_programme_begintime_channel_idx1 on product using gin ((attributes->'tv-channel'), partner_id, cast(attributes->>'begin-time' as bigint)) where product_type_id = 3;

commit;

/* logging triggers */

create or replace function public.product_bip() returns trigger as $$
begin
  insert into log_product (id, partner_id, product_type_id, partner_product_code, related, attributes, valid_from)
  values (new.id, new.partner_id, new.product_type_id, new.partner_product_code, new.related, new.attributes, current_timestamp at time zone 'utc')
  returning log_id into new.last_log_id;
  return new;
end;
$$ language 'plpgsql';

create trigger product_bi before insert on product for each row execute procedure product_bip();


create or replace function public.product_bup() returns trigger as $$
begin
  update log_product set valid_till = current_timestamp at time zone 'utc'
  where log_id = old.last_log_id;
  insert into log_product (id, partner_id, product_type_id, partner_product_code, related, attributes, valid_from)
  values (new.id, new.partner_id, new.product_type_id, new.partner_product_code, new.related, new.attributes, current_timestamp at time zone 'utc')
  returning log_id into new.last_log_id;
  if new.deleted is null then new.modified = current_timestamp at time zone 'utc'; end if;
  return new;
end;
$$ language 'plpgsql';

create trigger product_bu before update on product for each row execute procedure product_bup();


create or replace function public.product_bdp() returns trigger as $$
begin
  update log_product set valid_till = current_timestamp at time zone 'utc'
  where log_id = old.last_log_id;
  return old;
end;
$$ language 'plpgsql';

create trigger product_bd before delete on product for each row execute procedure product_bdp();

commit;

create or replace function select_or_create_consumer(partner_id_in r_id, username_in r_varchar100, consumer_type_id_in r_id, parent_consumer_id_in r_id)
 returns table(id r_id, activated r_timestamp, is_new smallint)
 language 'plpgsql'
as $$
declare
  id_out consumer.id%TYPE;
  activated_out consumer.activated%TYPE;
  is_new_out smallint;
  cnt integer;
begin
  cnt = 0;
  is_new_out = 0;
  loop
    select c.id, c.activated into id_out, activated_out from consumer c where c.partner_id = partner_id_in and c.username = username_in and c.status = 1 for update;
    exit when found;
    begin
      insert into consumer (partner_id, username, status, type_id, parent_id) values (partner_id_in, username_in, 1, consumer_type_id_in, parent_consumer_id_in) returning consumer.id, consumer.activated into id_out, activated_out;
      is_new_out = 1;
      exit;
    exception
      when others then -- do nothing and retry the loop
        cnt := cnt + 1;
        if cnt > 20 then raise; end if;
    end;
  end loop;
  return query select id_out, activated_out, is_new_out;
end;
$$;

create or replace function create_product_view(in_product_type_id bigint)
  returns void
language 'plpgsql'
as $$
declare
  fields text;
  rec_fields text;
  s text;
  fld_name text;
  attr record;
  ptype text;
begin
  select replace(identifier, '-', '_') from product_type where id = in_product_type_id into ptype;
  fields = 'p.id, p.partner_id, p.product_type_id, p.partner_product_code, p.added, p.modified, p.deleted';
  if (in_product_type_id = 1) or (in_product_type_id = 3) then
    fields = fields || ', (p.related->>''video-id'')::bigint as video_id, (p.related->>''series-id'')::bigint as series_id';
  end if;
  rec_fields = '';
  for attr in select a.identifier, a.value_type, a.is_translatable, a.is_multivalue from product_type_attribute t inner join attribute a on a.id = t.attribute_id where t.product_type_id = in_product_type_id loop
    fld_name = replace(attr.identifier, '-', '_');
    if attr.is_multivalue != 0 then -- multivalue
      rec_fields = rec_fields || '"' || attr.identifier || '" jsonb, ';
      if attr.is_translatable != 0 then
        fields = fields || ', (select array_agg(value->>''value'') from jsonb_array_elements(a."' || attr.identifier || '")) as "' || fld_name || '"';
      else
        case attr.value_type
          when 0 then -- integer
          fields = fields || ', (select array_agg(value::bigint) from jsonb_array_elements_text(a."' || attr.identifier || '")) as "' || fld_name || '"';
          when 1 then -- string
          fields = fields || ', (select array_agg(value) from jsonb_array_elements_text(a."' || attr.identifier || '")) as "' || fld_name || '"';
          when 2 then -- boolean
          fields = fields || ', (select array_agg(value::bool) from jsonb_array_elements_text(a."' || attr.identifier || '")) as "' || fld_name || '"';
          when 3 then -- date
          fields = fields || ', (select array_agg(to_timestamp(value::bigint)::date) from jsonb_array_elements_text(a."' || attr.identifier || '")) as "' || fld_name || '"';
          when 5 then -- float
          fields = fields || ', (select array_agg(value::float8) from jsonb_array_elements_text(a."' || attr.identifier || '")) as "' || fld_name || '"';
          when 6 then -- timestamp
          fields = fields || ', (select array_agg(to_timestamp(value::bigint) at time zone ''utc'') from jsonb_array_elements_text(a."' || attr.identifier || '")) as "' || fld_name || '"';
          when 7 then -- timestamp interval
          fields = fields || ', (select array_agg(tsrange(to_timestamp((value->>''begin'')::bigint) at time zone ''utc'', to_timestamp((value->>''end'')::bigint) at time zone ''utc'', ''[]'')) from jsonb_array_elements(a."' || attr.identifier || '")) as "' || fld_name || '"';
        end case;
      end if;
    else -- single value
      if attr.is_translatable != 0 then
        rec_fields = rec_fields || '"' || attr.identifier || '" jsonb, ';
        fields = fields || ', a."' || attr.identifier || '"->>''value'' as "' || fld_name || '"';
      else
        case attr.value_type
          when 0 then -- integer
          rec_fields = rec_fields || '"' || attr.identifier || '" bigint, ';
          fields = fields || ', a."' || attr.identifier || '" as "' || fld_name || '"';
          when 1 then -- string
          rec_fields = rec_fields || '"' || attr.identifier || '" text, ';
          fields = fields || ', a."' || attr.identifier || '" as "' || fld_name || '"';
          when 2 then -- boolean
          rec_fields = rec_fields || '"' || attr.identifier || '" bool, ';
          fields = fields || ', a."' || attr.identifier || '" as "' || fld_name || '"';
          when 3 then -- date
          rec_fields = rec_fields || '"' || attr.identifier || '" bigint, ';
          fields = fields || ', to_timestamp(a."' || attr.identifier || '")::date as "' || fld_name || '"';
          when 5 then -- float
          rec_fields = rec_fields || '"' || attr.identifier || '" float8, ';
          fields = fields || ', a."' || attr.identifier || '" as "' || fld_name || '"';
          when 6 then -- timestamp
          rec_fields = rec_fields || '"' || attr.identifier || '" bigint, ';
          fields = fields || ', to_timestamp(a."' || attr.identifier || '") at time zone ''utc'' as "' || fld_name || '"';
          when 7 then -- timestamp interval
          rec_fields = rec_fields || '"' || attr.identifier || '" jsonb, ';
          fields = fields || ', tsrange(to_timestamp((a."' || attr.identifier || '"->>''begin'')::bigint) at time zone ''utc'', to_timestamp((a."' || attr.identifier || '"->>''end'')::bigint) at time zone ''utc'', ''[]'') as "' || fld_name || '"';
        end case;
      end if;
    end if;
  end loop;
  s = 'create view prod_' || ptype || ' as select ' || fields || ' from product p cross join lateral jsonb_to_record(p.attributes) as a(' || substring(rec_fields from 1 for (char_length(rec_fields) - 2)) || ') where p.product_type_id = ' || in_product_type_id;
  execute 'drop view if exists prod_' || ptype;
  execute s;
end;
$$;

commit;

/* initial data */

insert into language (iso639_2t, iso639_1) values ('eng', 'en');
insert into language (iso639_2t, iso639_1) values ('slv', 'sl');
insert into language (iso639_2t, iso639_1) values ('deu', 'de');
insert into language (iso639_2t, iso639_1) values ('fra', 'fr');
insert into language (iso639_2t, iso639_1) values ('ita', 'it');
insert into language (iso639_2t, iso639_1) values ('unk', 'un');
insert into language (iso639_2t, iso639_1) values ('kor', 'ko');
insert into language (iso639_2t, iso639_1) values ('nor', 'no');
insert into language (iso639_2t, iso639_1) values ('hrv', 'hr');
insert into language (iso639_2t, iso639_1) values ('swe', 'sv');
insert into language (iso639_2t, iso639_1) values ('spa', 'es');
insert into language (iso639_2t, iso639_1) values ('zho', 'zh');
insert into language (iso639_2t, iso639_1) values ('nld', 'nl');
insert into language (iso639_2t, iso639_1) values ('dan', 'da');
insert into language (iso639_2t, iso639_1) values ('jpn', 'ja');
insert into language (iso639_2t, iso639_1) values ('hun', 'hu');
insert into language (iso639_2t, iso639_1) values ('por', 'pt');
insert into language (iso639_2t, iso639_1) values ('fin', 'fi');
insert into language (iso639_2t, iso639_1) values ('ces', 'cs');
insert into language (iso639_2t, iso639_1) values ('ara', 'ar');
insert into language (iso639_2t, iso639_1) values ('pol', 'pl');
insert into language (iso639_2t, iso639_1) values ('bul', 'bg');
insert into language (iso639_2t, iso639_1) values ('srp', 'sr');
insert into language (iso639_2t, iso639_1) values ('rus', 'ru');
insert into language (iso639_2t, iso639_1) values ('bos', 'bs');
insert into language (iso639_2t, iso639_1) values ('tha', 'th');
insert into language (iso639_2t, iso639_1) values ('ell', 'el');
insert into language (iso639_2t, iso639_1) values ('nob', 'nb');
insert into language (iso639_2t, iso639_1) values ('hin', 'hi');
insert into language (iso639_2t, iso639_1) values ('ron', 'ro');
insert into language (iso639_2t, iso639_1) values ('mkd', 'mk');
insert into language (iso639_2t, iso639_1) values ('aar', 'aa');
insert into language (iso639_2t, iso639_1) values ('abk', 'ab');
insert into language (iso639_2t, iso639_1) values ('ace', null);
insert into language (iso639_2t, iso639_1) values ('ach', null);
insert into language (iso639_2t, iso639_1) values ('ada', null);
insert into language (iso639_2t, iso639_1) values ('ady', null);
insert into language (iso639_2t, iso639_1) values ('afa', null);
insert into language (iso639_2t, iso639_1) values ('afh', null);
insert into language (iso639_2t, iso639_1) values ('afr', 'af');
insert into language (iso639_2t, iso639_1) values ('ain', null);
insert into language (iso639_2t, iso639_1) values ('aka', 'ak');
insert into language (iso639_2t, iso639_1) values ('akk', null);
insert into language (iso639_2t, iso639_1) values ('sqi', 'sq');
insert into language (iso639_2t, iso639_1) values ('ale', null);
insert into language (iso639_2t, iso639_1) values ('alg', null);
insert into language (iso639_2t, iso639_1) values ('alt', null);
insert into language (iso639_2t, iso639_1) values ('amh', 'am');
insert into language (iso639_2t, iso639_1) values ('ang', null);
insert into language (iso639_2t, iso639_1) values ('anp', null);
insert into language (iso639_2t, iso639_1) values ('apa', null);
insert into language (iso639_2t, iso639_1) values ('arc', null);
insert into language (iso639_2t, iso639_1) values ('arg', 'an');
insert into language (iso639_2t, iso639_1) values ('hye', 'hy');
insert into language (iso639_2t, iso639_1) values ('arn', null);
insert into language (iso639_2t, iso639_1) values ('arp', null);
insert into language (iso639_2t, iso639_1) values ('art', null);
insert into language (iso639_2t, iso639_1) values ('arw', null);
insert into language (iso639_2t, iso639_1) values ('asm', 'as');
insert into language (iso639_2t, iso639_1) values ('ast', null);
insert into language (iso639_2t, iso639_1) values ('ath', null);
insert into language (iso639_2t, iso639_1) values ('aus', null);
insert into language (iso639_2t, iso639_1) values ('ava', 'av');
insert into language (iso639_2t, iso639_1) values ('ave', 'ae');
insert into language (iso639_2t, iso639_1) values ('awa', null);
insert into language (iso639_2t, iso639_1) values ('aym', 'ay');
insert into language (iso639_2t, iso639_1) values ('aze', 'az');
insert into language (iso639_2t, iso639_1) values ('bad', null);
insert into language (iso639_2t, iso639_1) values ('bai', null);
insert into language (iso639_2t, iso639_1) values ('bak', 'ba');
insert into language (iso639_2t, iso639_1) values ('bal', null);
insert into language (iso639_2t, iso639_1) values ('bam', 'bm');
insert into language (iso639_2t, iso639_1) values ('ban', null);
insert into language (iso639_2t, iso639_1) values ('eus', 'eu');
insert into language (iso639_2t, iso639_1) values ('bas', null);
insert into language (iso639_2t, iso639_1) values ('bat', null);
insert into language (iso639_2t, iso639_1) values ('bej', null);
insert into language (iso639_2t, iso639_1) values ('bel', 'be');
insert into language (iso639_2t, iso639_1) values ('bem', null);
insert into language (iso639_2t, iso639_1) values ('ben', 'bn');
insert into language (iso639_2t, iso639_1) values ('ber', null);
insert into language (iso639_2t, iso639_1) values ('bho', null);
insert into language (iso639_2t, iso639_1) values ('bih', 'bh');
insert into language (iso639_2t, iso639_1) values ('bik', null);
insert into language (iso639_2t, iso639_1) values ('bin', null);
insert into language (iso639_2t, iso639_1) values ('bis', 'bi');
insert into language (iso639_2t, iso639_1) values ('bla', null);
insert into language (iso639_2t, iso639_1) values ('bnt', null);
insert into language (iso639_2t, iso639_1) values ('bra', null);
insert into language (iso639_2t, iso639_1) values ('bre', 'br');
insert into language (iso639_2t, iso639_1) values ('btk', null);
insert into language (iso639_2t, iso639_1) values ('bua', null);
insert into language (iso639_2t, iso639_1) values ('bug', null);
insert into language (iso639_2t, iso639_1) values ('mya', 'my');
insert into language (iso639_2t, iso639_1) values ('byn', null);
insert into language (iso639_2t, iso639_1) values ('cad', null);
insert into language (iso639_2t, iso639_1) values ('cai', null);
insert into language (iso639_2t, iso639_1) values ('car', null);
insert into language (iso639_2t, iso639_1) values ('cat', 'ca');
insert into language (iso639_2t, iso639_1) values ('cau', null);
insert into language (iso639_2t, iso639_1) values ('ceb', null);
insert into language (iso639_2t, iso639_1) values ('cel', null);
insert into language (iso639_2t, iso639_1) values ('cha', 'ch');
insert into language (iso639_2t, iso639_1) values ('chb', null);
insert into language (iso639_2t, iso639_1) values ('che', 'ce');
insert into language (iso639_2t, iso639_1) values ('chg', null);
insert into language (iso639_2t, iso639_1) values ('chk', null);
insert into language (iso639_2t, iso639_1) values ('chm', null);
insert into language (iso639_2t, iso639_1) values ('chn', null);
insert into language (iso639_2t, iso639_1) values ('cho', null);
insert into language (iso639_2t, iso639_1) values ('chp', null);
insert into language (iso639_2t, iso639_1) values ('chr', null);
insert into language (iso639_2t, iso639_1) values ('chu', 'cu');
insert into language (iso639_2t, iso639_1) values ('chv', 'cv');
insert into language (iso639_2t, iso639_1) values ('chy', null);
insert into language (iso639_2t, iso639_1) values ('cmc', null);
insert into language (iso639_2t, iso639_1) values ('cop', null);
insert into language (iso639_2t, iso639_1) values ('cor', 'kw');
insert into language (iso639_2t, iso639_1) values ('cos', 'co');
insert into language (iso639_2t, iso639_1) values ('cpe', null);
insert into language (iso639_2t, iso639_1) values ('cpf', null);
insert into language (iso639_2t, iso639_1) values ('cpp', null);
insert into language (iso639_2t, iso639_1) values ('cre', 'cr');
insert into language (iso639_2t, iso639_1) values ('crh', null);
insert into language (iso639_2t, iso639_1) values ('crp', null);
insert into language (iso639_2t, iso639_1) values ('csb', null);
insert into language (iso639_2t, iso639_1) values ('cus', null);
insert into language (iso639_2t, iso639_1) values ('dak', null);
insert into language (iso639_2t, iso639_1) values ('dar', null);
insert into language (iso639_2t, iso639_1) values ('day', null);
insert into language (iso639_2t, iso639_1) values ('del', null);
insert into language (iso639_2t, iso639_1) values ('den', null);
insert into language (iso639_2t, iso639_1) values ('dgr', null);
insert into language (iso639_2t, iso639_1) values ('din', null);
insert into language (iso639_2t, iso639_1) values ('div', 'dv');
insert into language (iso639_2t, iso639_1) values ('doi', null);
insert into language (iso639_2t, iso639_1) values ('dra', null);
insert into language (iso639_2t, iso639_1) values ('dsb', null);
insert into language (iso639_2t, iso639_1) values ('dua', null);
insert into language (iso639_2t, iso639_1) values ('dum', null);
insert into language (iso639_2t, iso639_1) values ('dyu', null);
insert into language (iso639_2t, iso639_1) values ('dzo', 'dz');
insert into language (iso639_2t, iso639_1) values ('efi', null);
insert into language (iso639_2t, iso639_1) values ('egy', null);
insert into language (iso639_2t, iso639_1) values ('eka', null);
insert into language (iso639_2t, iso639_1) values ('elx', null);
insert into language (iso639_2t, iso639_1) values ('enm', null);
insert into language (iso639_2t, iso639_1) values ('epo', 'eo');
insert into language (iso639_2t, iso639_1) values ('est', 'et');
insert into language (iso639_2t, iso639_1) values ('ewe', 'ee');
insert into language (iso639_2t, iso639_1) values ('ewo', null);
insert into language (iso639_2t, iso639_1) values ('fan', null);
insert into language (iso639_2t, iso639_1) values ('fao', 'fo');
insert into language (iso639_2t, iso639_1) values ('fat', null);
insert into language (iso639_2t, iso639_1) values ('fij', 'fj');
insert into language (iso639_2t, iso639_1) values ('fil', null);
insert into language (iso639_2t, iso639_1) values ('fiu', null);
insert into language (iso639_2t, iso639_1) values ('fon', null);
insert into language (iso639_2t, iso639_1) values ('frm', null);
insert into language (iso639_2t, iso639_1) values ('fro', null);
insert into language (iso639_2t, iso639_1) values ('frr', null);
insert into language (iso639_2t, iso639_1) values ('frs', null);
insert into language (iso639_2t, iso639_1) values ('fry', 'fy');
insert into language (iso639_2t, iso639_1) values ('ful', 'ff');
insert into language (iso639_2t, iso639_1) values ('fur', null);
insert into language (iso639_2t, iso639_1) values ('gaa', null);
insert into language (iso639_2t, iso639_1) values ('gay', null);
insert into language (iso639_2t, iso639_1) values ('gba', null);
insert into language (iso639_2t, iso639_1) values ('gem', null);
insert into language (iso639_2t, iso639_1) values ('kat', 'ka');
insert into language (iso639_2t, iso639_1) values ('gez', null);
insert into language (iso639_2t, iso639_1) values ('gil', null);
insert into language (iso639_2t, iso639_1) values ('gla', 'gd');
insert into language (iso639_2t, iso639_1) values ('gle', 'ga');
insert into language (iso639_2t, iso639_1) values ('glg', 'gl');
insert into language (iso639_2t, iso639_1) values ('glv', 'gv');
insert into language (iso639_2t, iso639_1) values ('gmh', null);
insert into language (iso639_2t, iso639_1) values ('goh', null);
insert into language (iso639_2t, iso639_1) values ('gon', null);
insert into language (iso639_2t, iso639_1) values ('gor', null);
insert into language (iso639_2t, iso639_1) values ('got', null);
insert into language (iso639_2t, iso639_1) values ('grb', null);
insert into language (iso639_2t, iso639_1) values ('grc', null);
insert into language (iso639_2t, iso639_1) values ('grn', 'gn');
insert into language (iso639_2t, iso639_1) values ('gsw', null);
insert into language (iso639_2t, iso639_1) values ('guj', 'gu');
insert into language (iso639_2t, iso639_1) values ('gwi', null);
insert into language (iso639_2t, iso639_1) values ('hai', null);
insert into language (iso639_2t, iso639_1) values ('hat', 'ht');
insert into language (iso639_2t, iso639_1) values ('hau', 'ha');
insert into language (iso639_2t, iso639_1) values ('haw', null);
insert into language (iso639_2t, iso639_1) values ('heb', 'he');
insert into language (iso639_2t, iso639_1) values ('her', 'hz');
insert into language (iso639_2t, iso639_1) values ('hil', null);
insert into language (iso639_2t, iso639_1) values ('him', null);
insert into language (iso639_2t, iso639_1) values ('hit', null);
insert into language (iso639_2t, iso639_1) values ('hmn', null);
insert into language (iso639_2t, iso639_1) values ('hmo', 'ho');
insert into language (iso639_2t, iso639_1) values ('hsb', null);
insert into language (iso639_2t, iso639_1) values ('hup', null);
insert into language (iso639_2t, iso639_1) values ('iba', null);
insert into language (iso639_2t, iso639_1) values ('ibo', 'ig');
insert into language (iso639_2t, iso639_1) values ('isl', 'is');
insert into language (iso639_2t, iso639_1) values ('ido', 'io');
insert into language (iso639_2t, iso639_1) values ('iii', 'ii');
insert into language (iso639_2t, iso639_1) values ('ijo', null);
insert into language (iso639_2t, iso639_1) values ('iku', 'iu');
insert into language (iso639_2t, iso639_1) values ('ile', 'ie');
insert into language (iso639_2t, iso639_1) values ('ilo', null);
insert into language (iso639_2t, iso639_1) values ('ina', 'ia');
insert into language (iso639_2t, iso639_1) values ('inc', null);
insert into language (iso639_2t, iso639_1) values ('ind', 'id');
insert into language (iso639_2t, iso639_1) values ('ine', null);
insert into language (iso639_2t, iso639_1) values ('inh', null);
insert into language (iso639_2t, iso639_1) values ('ipk', 'ik');
insert into language (iso639_2t, iso639_1) values ('ira', null);
insert into language (iso639_2t, iso639_1) values ('iro', null);
insert into language (iso639_2t, iso639_1) values ('jav', 'jv');
insert into language (iso639_2t, iso639_1) values ('jbo', null);
insert into language (iso639_2t, iso639_1) values ('jpr', null);
insert into language (iso639_2t, iso639_1) values ('jrb', null);
insert into language (iso639_2t, iso639_1) values ('kaa', null);
insert into language (iso639_2t, iso639_1) values ('kab', null);
insert into language (iso639_2t, iso639_1) values ('kac', null);
insert into language (iso639_2t, iso639_1) values ('kal', 'kl');
insert into language (iso639_2t, iso639_1) values ('kam', null);
insert into language (iso639_2t, iso639_1) values ('kan', 'kn');
insert into language (iso639_2t, iso639_1) values ('kar', null);
insert into language (iso639_2t, iso639_1) values ('kas', 'ks');
insert into language (iso639_2t, iso639_1) values ('kau', 'kr');
insert into language (iso639_2t, iso639_1) values ('kaw', null);
insert into language (iso639_2t, iso639_1) values ('kaz', 'kk');
insert into language (iso639_2t, iso639_1) values ('kbd', null);
insert into language (iso639_2t, iso639_1) values ('kha', null);
insert into language (iso639_2t, iso639_1) values ('khi', null);
insert into language (iso639_2t, iso639_1) values ('khm', 'km');
insert into language (iso639_2t, iso639_1) values ('kho', null);
insert into language (iso639_2t, iso639_1) values ('kik', 'ki');
insert into language (iso639_2t, iso639_1) values ('kin', 'rw');
insert into language (iso639_2t, iso639_1) values ('kir', 'ky');
insert into language (iso639_2t, iso639_1) values ('kmb', null);
insert into language (iso639_2t, iso639_1) values ('kok', null);
insert into language (iso639_2t, iso639_1) values ('kom', 'kv');
insert into language (iso639_2t, iso639_1) values ('kon', 'kg');
insert into language (iso639_2t, iso639_1) values ('kos', null);
insert into language (iso639_2t, iso639_1) values ('kpe', null);
insert into language (iso639_2t, iso639_1) values ('krc', null);
insert into language (iso639_2t, iso639_1) values ('krl', null);
insert into language (iso639_2t, iso639_1) values ('kro', null);
insert into language (iso639_2t, iso639_1) values ('kru', null);
insert into language (iso639_2t, iso639_1) values ('kua', 'kj');
insert into language (iso639_2t, iso639_1) values ('kum', null);
insert into language (iso639_2t, iso639_1) values ('kur', 'ku');
insert into language (iso639_2t, iso639_1) values ('kut', null);
insert into language (iso639_2t, iso639_1) values ('lad', null);
insert into language (iso639_2t, iso639_1) values ('lah', null);
insert into language (iso639_2t, iso639_1) values ('lam', null);
insert into language (iso639_2t, iso639_1) values ('lao', 'lo');
insert into language (iso639_2t, iso639_1) values ('lat', 'la');
insert into language (iso639_2t, iso639_1) values ('lav', 'lv');
insert into language (iso639_2t, iso639_1) values ('lez', null);
insert into language (iso639_2t, iso639_1) values ('lim', 'li');
insert into language (iso639_2t, iso639_1) values ('lin', 'ln');
insert into language (iso639_2t, iso639_1) values ('lit', 'lt');
insert into language (iso639_2t, iso639_1) values ('lol', null);
insert into language (iso639_2t, iso639_1) values ('loz', null);
insert into language (iso639_2t, iso639_1) values ('ltz', 'lb');
insert into language (iso639_2t, iso639_1) values ('lua', null);
insert into language (iso639_2t, iso639_1) values ('lub', 'lu');
insert into language (iso639_2t, iso639_1) values ('lug', 'lg');
insert into language (iso639_2t, iso639_1) values ('lui', null);
insert into language (iso639_2t, iso639_1) values ('lun', null);
insert into language (iso639_2t, iso639_1) values ('luo', null);
insert into language (iso639_2t, iso639_1) values ('lus', null);
insert into language (iso639_2t, iso639_1) values ('mad', null);
insert into language (iso639_2t, iso639_1) values ('mag', null);
insert into language (iso639_2t, iso639_1) values ('mah', 'mh');
insert into language (iso639_2t, iso639_1) values ('mai', null);
insert into language (iso639_2t, iso639_1) values ('mak', null);
insert into language (iso639_2t, iso639_1) values ('mal', 'ml');
insert into language (iso639_2t, iso639_1) values ('man', null);
insert into language (iso639_2t, iso639_1) values ('mri', 'mi');
insert into language (iso639_2t, iso639_1) values ('map', null);
insert into language (iso639_2t, iso639_1) values ('mar', 'mr');
insert into language (iso639_2t, iso639_1) values ('mas', null);
insert into language (iso639_2t, iso639_1) values ('msa', 'ms');
insert into language (iso639_2t, iso639_1) values ('mdf', null);
insert into language (iso639_2t, iso639_1) values ('mdr', null);
insert into language (iso639_2t, iso639_1) values ('men', null);
insert into language (iso639_2t, iso639_1) values ('mga', null);
insert into language (iso639_2t, iso639_1) values ('mic', null);
insert into language (iso639_2t, iso639_1) values ('min', null);
insert into language (iso639_2t, iso639_1) values ('mis', null);
insert into language (iso639_2t, iso639_1) values ('mkh', null);
insert into language (iso639_2t, iso639_1) values ('mlg', 'mg');
insert into language (iso639_2t, iso639_1) values ('mlt', 'mt');
insert into language (iso639_2t, iso639_1) values ('mnc', null);
insert into language (iso639_2t, iso639_1) values ('mni', null);
insert into language (iso639_2t, iso639_1) values ('mno', null);
insert into language (iso639_2t, iso639_1) values ('moh', null);
insert into language (iso639_2t, iso639_1) values ('mon', 'mn');
insert into language (iso639_2t, iso639_1) values ('mos', null);
insert into language (iso639_2t, iso639_1) values ('mul', null);
insert into language (iso639_2t, iso639_1) values ('mun', null);
insert into language (iso639_2t, iso639_1) values ('mus', null);
insert into language (iso639_2t, iso639_1) values ('mwl', null);
insert into language (iso639_2t, iso639_1) values ('mwr', null);
insert into language (iso639_2t, iso639_1) values ('myn', null);
insert into language (iso639_2t, iso639_1) values ('myv', null);
insert into language (iso639_2t, iso639_1) values ('nah', null);
insert into language (iso639_2t, iso639_1) values ('nai', null);
insert into language (iso639_2t, iso639_1) values ('nap', null);
insert into language (iso639_2t, iso639_1) values ('nau', 'na');
insert into language (iso639_2t, iso639_1) values ('nav', 'nv');
insert into language (iso639_2t, iso639_1) values ('nbl', 'nr');
insert into language (iso639_2t, iso639_1) values ('nde', 'nd');
insert into language (iso639_2t, iso639_1) values ('ndo', 'ng');
insert into language (iso639_2t, iso639_1) values ('nds', null);
insert into language (iso639_2t, iso639_1) values ('nep', 'ne');
insert into language (iso639_2t, iso639_1) values ('new', null);
insert into language (iso639_2t, iso639_1) values ('nia', null);
insert into language (iso639_2t, iso639_1) values ('nic', null);
insert into language (iso639_2t, iso639_1) values ('niu', null);
insert into language (iso639_2t, iso639_1) values ('nno', 'nn');
insert into language (iso639_2t, iso639_1) values ('nog', null);
insert into language (iso639_2t, iso639_1) values ('non', null);
insert into language (iso639_2t, iso639_1) values ('nqo', null);
insert into language (iso639_2t, iso639_1) values ('nso', null);
insert into language (iso639_2t, iso639_1) values ('nub', null);
insert into language (iso639_2t, iso639_1) values ('nwc', null);
insert into language (iso639_2t, iso639_1) values ('nya', 'ny');
insert into language (iso639_2t, iso639_1) values ('nym', null);
insert into language (iso639_2t, iso639_1) values ('nyn', null);
insert into language (iso639_2t, iso639_1) values ('nyo', null);
insert into language (iso639_2t, iso639_1) values ('nzi', null);
insert into language (iso639_2t, iso639_1) values ('oci', 'oc');
insert into language (iso639_2t, iso639_1) values ('oji', 'oj');
insert into language (iso639_2t, iso639_1) values ('ori', 'or');
insert into language (iso639_2t, iso639_1) values ('orm', 'om');
insert into language (iso639_2t, iso639_1) values ('osa', null);
insert into language (iso639_2t, iso639_1) values ('oss', 'os');
insert into language (iso639_2t, iso639_1) values ('ota', null);
insert into language (iso639_2t, iso639_1) values ('oto', null);
insert into language (iso639_2t, iso639_1) values ('paa', null);
insert into language (iso639_2t, iso639_1) values ('pag', null);
insert into language (iso639_2t, iso639_1) values ('pal', null);
insert into language (iso639_2t, iso639_1) values ('pam', null);
insert into language (iso639_2t, iso639_1) values ('pan', 'pa');
insert into language (iso639_2t, iso639_1) values ('pap', null);
insert into language (iso639_2t, iso639_1) values ('pau', null);
insert into language (iso639_2t, iso639_1) values ('peo', null);
insert into language (iso639_2t, iso639_1) values ('fas', 'fa');
insert into language (iso639_2t, iso639_1) values ('phi', null);
insert into language (iso639_2t, iso639_1) values ('phn', null);
insert into language (iso639_2t, iso639_1) values ('pli', 'pi');
insert into language (iso639_2t, iso639_1) values ('pon', null);
insert into language (iso639_2t, iso639_1) values ('pra', null);
insert into language (iso639_2t, iso639_1) values ('pro', null);
insert into language (iso639_2t, iso639_1) values ('pus', 'ps');
insert into language (iso639_2t, iso639_1) values ('que', 'qu');
insert into language (iso639_2t, iso639_1) values ('raj', null);
insert into language (iso639_2t, iso639_1) values ('rap', null);
insert into language (iso639_2t, iso639_1) values ('rar', null);
insert into language (iso639_2t, iso639_1) values ('roa', null);
insert into language (iso639_2t, iso639_1) values ('roh', 'rm');
insert into language (iso639_2t, iso639_1) values ('rom', null);
insert into language (iso639_2t, iso639_1) values ('run', 'rn');
insert into language (iso639_2t, iso639_1) values ('rup', null);
insert into language (iso639_2t, iso639_1) values ('sad', null);
insert into language (iso639_2t, iso639_1) values ('sag', 'sg');
insert into language (iso639_2t, iso639_1) values ('sah', null);
insert into language (iso639_2t, iso639_1) values ('sai', null);
insert into language (iso639_2t, iso639_1) values ('sal', null);
insert into language (iso639_2t, iso639_1) values ('sam', null);
insert into language (iso639_2t, iso639_1) values ('san', 'sa');
insert into language (iso639_2t, iso639_1) values ('sas', null);
insert into language (iso639_2t, iso639_1) values ('sat', null);
insert into language (iso639_2t, iso639_1) values ('scn', null);
insert into language (iso639_2t, iso639_1) values ('sco', null);
insert into language (iso639_2t, iso639_1) values ('sel', null);
insert into language (iso639_2t, iso639_1) values ('sem', null);
insert into language (iso639_2t, iso639_1) values ('sga', null);
insert into language (iso639_2t, iso639_1) values ('sgn', null);
insert into language (iso639_2t, iso639_1) values ('shn', null);
insert into language (iso639_2t, iso639_1) values ('sid', null);
insert into language (iso639_2t, iso639_1) values ('sin', 'si');
insert into language (iso639_2t, iso639_1) values ('sio', null);
insert into language (iso639_2t, iso639_1) values ('sit', null);
insert into language (iso639_2t, iso639_1) values ('sla', null);
insert into language (iso639_2t, iso639_1) values ('slk', 'sk');
insert into language (iso639_2t, iso639_1) values ('sma', null);
insert into language (iso639_2t, iso639_1) values ('sme', 'se');
insert into language (iso639_2t, iso639_1) values ('smi', null);
insert into language (iso639_2t, iso639_1) values ('smj', null);
insert into language (iso639_2t, iso639_1) values ('smn', null);
insert into language (iso639_2t, iso639_1) values ('smo', 'sm');
insert into language (iso639_2t, iso639_1) values ('sms', null);
insert into language (iso639_2t, iso639_1) values ('sna', 'sn');
insert into language (iso639_2t, iso639_1) values ('snd', 'sd');
insert into language (iso639_2t, iso639_1) values ('snk', null);
insert into language (iso639_2t, iso639_1) values ('sog', null);
insert into language (iso639_2t, iso639_1) values ('som', 'so');
insert into language (iso639_2t, iso639_1) values ('son', null);
insert into language (iso639_2t, iso639_1) values ('sot', 'st');
insert into language (iso639_2t, iso639_1) values ('srd', 'sc');
insert into language (iso639_2t, iso639_1) values ('srn', null);
insert into language (iso639_2t, iso639_1) values ('srr', null);
insert into language (iso639_2t, iso639_1) values ('ssa', null);
insert into language (iso639_2t, iso639_1) values ('ssw', 'ss');
insert into language (iso639_2t, iso639_1) values ('suk', null);
insert into language (iso639_2t, iso639_1) values ('sun', 'su');
insert into language (iso639_2t, iso639_1) values ('sus', null);
insert into language (iso639_2t, iso639_1) values ('sux', null);
insert into language (iso639_2t, iso639_1) values ('swa', 'sw');
insert into language (iso639_2t, iso639_1) values ('syc', null);
insert into language (iso639_2t, iso639_1) values ('syr', null);
insert into language (iso639_2t, iso639_1) values ('tah', 'ty');
insert into language (iso639_2t, iso639_1) values ('tai', null);
insert into language (iso639_2t, iso639_1) values ('tam', 'ta');
insert into language (iso639_2t, iso639_1) values ('tat', 'tt');
insert into language (iso639_2t, iso639_1) values ('tel', 'te');
insert into language (iso639_2t, iso639_1) values ('tem', null);
insert into language (iso639_2t, iso639_1) values ('ter', null);
insert into language (iso639_2t, iso639_1) values ('tet', null);
insert into language (iso639_2t, iso639_1) values ('tgk', 'tg');
insert into language (iso639_2t, iso639_1) values ('tgl', 'tl');
insert into language (iso639_2t, iso639_1) values ('bod', 'bo');
insert into language (iso639_2t, iso639_1) values ('tig', null);
insert into language (iso639_2t, iso639_1) values ('tir', 'ti');
insert into language (iso639_2t, iso639_1) values ('tiv', null);
insert into language (iso639_2t, iso639_1) values ('tkl', null);
insert into language (iso639_2t, iso639_1) values ('tlh', null);
insert into language (iso639_2t, iso639_1) values ('tli', null);
insert into language (iso639_2t, iso639_1) values ('tmh', null);
insert into language (iso639_2t, iso639_1) values ('tog', null);
insert into language (iso639_2t, iso639_1) values ('ton', 'to');
insert into language (iso639_2t, iso639_1) values ('tpi', null);
insert into language (iso639_2t, iso639_1) values ('tsi', null);
insert into language (iso639_2t, iso639_1) values ('tsn', 'tn');
insert into language (iso639_2t, iso639_1) values ('tso', 'ts');
insert into language (iso639_2t, iso639_1) values ('tuk', 'tk');
insert into language (iso639_2t, iso639_1) values ('tum', null);
insert into language (iso639_2t, iso639_1) values ('tup', null);
insert into language (iso639_2t, iso639_1) values ('tur', 'tr');
insert into language (iso639_2t, iso639_1) values ('tut', null);
insert into language (iso639_2t, iso639_1) values ('tvl', null);
insert into language (iso639_2t, iso639_1) values ('twi', 'tw');
insert into language (iso639_2t, iso639_1) values ('tyv', null);
insert into language (iso639_2t, iso639_1) values ('udm', null);
insert into language (iso639_2t, iso639_1) values ('uga', null);
insert into language (iso639_2t, iso639_1) values ('uig', 'ug');
insert into language (iso639_2t, iso639_1) values ('ukr', 'uk');
insert into language (iso639_2t, iso639_1) values ('umb', null);
insert into language (iso639_2t, iso639_1) values ('und', null);
insert into language (iso639_2t, iso639_1) values ('urd', 'ur');
insert into language (iso639_2t, iso639_1) values ('uzb', 'uz');
insert into language (iso639_2t, iso639_1) values ('vai', null);
insert into language (iso639_2t, iso639_1) values ('ven', 've');
insert into language (iso639_2t, iso639_1) values ('vie', 'vi');
insert into language (iso639_2t, iso639_1) values ('vol', 'vo');
insert into language (iso639_2t, iso639_1) values ('vot', null);
insert into language (iso639_2t, iso639_1) values ('wak', null);
insert into language (iso639_2t, iso639_1) values ('wal', null);
insert into language (iso639_2t, iso639_1) values ('war', null);
insert into language (iso639_2t, iso639_1) values ('was', null);
insert into language (iso639_2t, iso639_1) values ('cym', 'cy');
insert into language (iso639_2t, iso639_1) values ('wen', null);
insert into language (iso639_2t, iso639_1) values ('wln', 'wa');
insert into language (iso639_2t, iso639_1) values ('wol', 'wo');
insert into language (iso639_2t, iso639_1) values ('xal', null);
insert into language (iso639_2t, iso639_1) values ('xho', 'xh');
insert into language (iso639_2t, iso639_1) values ('yao', null);
insert into language (iso639_2t, iso639_1) values ('yap', null);
insert into language (iso639_2t, iso639_1) values ('yid', 'yi');
insert into language (iso639_2t, iso639_1) values ('yor', 'yo');
insert into language (iso639_2t, iso639_1) values ('ypk', null);
insert into language (iso639_2t, iso639_1) values ('zap', null);
insert into language (iso639_2t, iso639_1) values ('zbl', null);
insert into language (iso639_2t, iso639_1) values ('zen', null);
insert into language (iso639_2t, iso639_1) values ('zgh', null);
insert into language (iso639_2t, iso639_1) values ('zha', 'za');
insert into language (iso639_2t, iso639_1) values ('znd', null);
insert into language (iso639_2t, iso639_1) values ('zul', 'zu');
insert into language (iso639_2t, iso639_1) values ('zun', null);
insert into language (iso639_2t, iso639_1) values ('zxx', null);
insert into language (iso639_2t, iso639_1) values ('zza', null);

insert into relation_type (identifier) values ('subscription');

insert into product_type (identifier) values ('video');
insert into product_type (identifier) values ('package');
insert into product_type (identifier) values ('tv-programme');
insert into product_type (identifier) values ('tv-channel');
insert into product_type (identifier) values ('tvod');
insert into product_type (identifier) values ('svod');
insert into product_type (identifier) values ('interactive');
insert into product_type (identifier) values ('series');

insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('genre',                     1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('production-year',           0, 0, 0, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('run-time',                  0, 0, 0, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('actor',                     1, 1, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('director',                  1, 1, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('voice',                     1, 1, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('title',                     1, 1, 0, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('cover-picture',             1, 0, 0, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('imdb-link',                 1, 0, 0, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('imdb-rating',               5, 0, 0, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('is-adult',                  2, 0, 0, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('freebase-id',               1, 0, 0, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('film-collection',           1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('featured-film-location',    1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('subject',                   1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('film-format',               1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('production-company',        1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('film-festival',             1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('produced-by',               1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('written-by',                1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('cinematography',            1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('edited-by',                 1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('music',                     1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('story-by',                  1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('costume-design-by',         1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('executive-produced-by',     1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('film-casting-director',     1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('film-production-design-by', 1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('film-art-direction-by',     1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('film-set-decoration-by',    1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('spoken-language',           1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('country',                   1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('rating-mpaa',               1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('rating-chvrs',              1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('rating-arg',                1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('rating-bmukk',              1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('rating-cbfc',               1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('rating-fsk',                1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('rating-tela',               1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('rating-acb',                1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('rating-ifco',               1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('rating-bbfc',               1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('rating-grc',                1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('rating-isl',                1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('rating-mtrcb',              1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('catalogue-id',              1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('expiry',                    6, 0, 0, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('category-id',               1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('tv-channel-id',             1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('catchup-hours',             0, 0, 0, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('title2',                    1, 1, 0, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('tv-channel',                1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('video-category',            1, 0, 0, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('episode-identifier',        1, 0, 0, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('episode-number',            0, 0, 0, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('begin-time',                6, 0, 0, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('end-time',                  6, 0, 0, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('is-catchup',                2, 0, 0, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('video-format',              1, 0, 0, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('catalogue-expiry',          1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('price',                     1, 0, 0, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('catalogue-price',           1, 0, 0, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('subtitle-language',         1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('package-type',              1, 0, 0, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('video-id',                  0, 0, 0, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('member',                    1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('tvod-id',                   1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('svod-id',                   1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('interactive-id',            1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('season-number',             0, 0, 0, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('part-number',               1, 0, 0, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('air-date',                  6, 0, 0, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('series-id',                 0, 0, 0, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('parental-rating',           0, 0, 0, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('is-subscribed',             2, 0, 0, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('package-id',                1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('description',               1, 1, 0, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('image-url',                 1, 0, 0, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('vod-category',              1, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('validity',                  7, 0, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('is-series',                 2, 0, 0, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('screenplay-writer',         1, 1, 1, 0);
insert into attribute (identifier, value_type, is_translatable, is_multivalue, is_private) values ('matched-attribute',         1, 0, 1, 0);

insert into consumer_event_type (identifier) values ('rating');
insert into consumer_event_type (identifier) values ('consumption');
insert into consumer_event_type (identifier) values ('product-pair-vote');
insert into consumer_event_type (identifier) values ('purchase');
insert into consumer_event_type (identifier) values ('zap');
insert into consumer_event_type (identifier) values ('interaction');
insert into consumer_event_type (identifier) values ('live-tv-consumption');
insert into consumer_event_type (identifier) values ('feedback');
insert into consumer_event_type (identifier) values ('offline-consumption');
insert into consumer_event_type (identifier) values ('offline-live-tv-consumption');
insert into consumer_event_type (identifier) values ('viewership');

insert into data_type (identifier) values ('rating');
insert into data_type (identifier) values ('catalogue-id');
insert into data_type (identifier) values ('device-id');
insert into data_type (identifier) values ('was-purchased');
insert into data_type (identifier) values ('product1');
insert into data_type (identifier) values ('product2');
insert into data_type (identifier) values ('device-type');
insert into data_type (identifier) values ('watch-offset');
insert into data_type (identifier) values ('watch-duration');
insert into data_type (identifier) values ('watch-delay');
insert into data_type (identifier) values ('content-duration');
insert into data_type (identifier) values ('price');
insert into data_type (identifier) values ('origin');
insert into data_type (identifier) values ('interaction-type');
insert into data_type (identifier) values ('tv-programme-id');
insert into data_type (identifier) values ('tv-channel-id');
insert into data_type (identifier) values ('feedback');
insert into data_type (identifier) values ('zap-count');
insert into data_type (identifier) values ('zap-offset');
insert into data_type (identifier) values ('watch-percentage');
insert into data_type (identifier) values ('action');
insert into data_type (identifier) values ('speed');
insert into data_type (identifier) values ('status');
insert into data_type (identifier) values ('_err-product-type');
insert into data_type (identifier) values ('_err-product-id');
insert into data_type (identifier) values ('origin-id');
insert into data_type (identifier) values ('origin-code');
insert into data_type (identifier) values ('origin-wait');
insert into data_type (identifier) values ('content-watched');
insert into data_type (identifier) values ('fast-forward-duration');
insert into data_type (identifier) values ('content-fast-forwarded');
insert into data_type (identifier) values ('rewind-duration');
insert into data_type (identifier) values ('content-rewound');
insert into data_type (identifier) values ('play-count');
insert into data_type (identifier) values ('fast-forward-count');
insert into data_type (identifier) values ('rewind-count');
insert into data_type (identifier) values ('stop-count');
insert into data_type (identifier) values ('pause-count');
insert into data_type (identifier) values ('consumption-duration');
insert into data_type (identifier) values ('consumption-percentage');
insert into data_type (identifier) values ('user-profile-id');
insert into data_type (identifier) values ('viewer-count');
insert into data_type (identifier) values ('consumer-id-list');
insert into data_type (identifier) values ('zap-id-list');

insert into product_type_attribute (product_type_id, attribute_id)
    select 1, id from attribute where identifier in ('actor', 'air-date', 'catalogue-id', 'country', 'description', 'director', 'episode-number', 'genre', 'image-url', 'imdb-link', 'imdb-rating', 'is-adult', 'parental-rating', 'price', 'production-year', 'run-time', 'season-number', 'spoken-language', 'subtitle-language', 'title', 'title2', 'validity', 'video-category', 'video-format', 'vod-category', 'screenplay-writer');
insert into product_type_attribute (product_type_id, attribute_id)
    select 2, id from attribute where identifier in ('interactive-id', 'package-type', 'svod-id', 'title', 'tv-channel-id');
insert into product_type_attribute (product_type_id, attribute_id)
    select 3, id from attribute where identifier in ('actor', 'air-date', 'begin-time', 'country', 'description', 'director', 'end-time', 'episode-number', 'genre', 'image-url', 'imdb-link', 'imdb-rating', 'is-adult', 'parental-rating', 'production-year', 'run-time', 'season-number', 'spoken-language', 'subtitle-language', 'title', 'title2', 'tv-channel', 'video-category', 'video-format', 'screenplay-writer');
insert into product_type_attribute (product_type_id, attribute_id)
    select 4, id from attribute where identifier in ('catchup-hours', 'is-adult', 'spoken-language', 'subtitle-language', 'video-format', 'title');
insert into product_type_attribute (product_type_id, attribute_id)
    select 5, id from attribute where identifier in ('catalogue-id', 'is-adult', 'title');
insert into product_type_attribute (product_type_id, attribute_id)
    select 6, id from attribute where identifier in ('catalogue-id', 'is-adult', 'title');
insert into product_type_attribute (product_type_id, attribute_id)
    select 7, id from attribute where identifier in ('title');
insert into product_type_attribute (product_type_id, attribute_id)
    select 8, id from attribute where identifier in ('title');

insert into partner (id, name, username, login_suffix, language_id) values (0, 'No partner', 'none', 'none', 1); -- This is a special internal partner
insert into partner (id, name, username, login_suffix, language_id) values (1, 'Demo', 'demo', 'demo', 1);

insert into recommender (id, name, hostname) values (1, 'general');

insert into partner_recommender (partner_id, recommender_id, name) values (1, 1, 'demo-movies');
insert into partner_recommender (partner_id, recommender_id, name) values (1, 1, 'demo-all');
insert into partner_recommender (partner_id, recommender_id, name) values (1, 1, 'demo-similar');
insert into partner_recommender (partner_id, recommender_id, name) values (1, 1, 'demo-tv-series');
insert into partner_recommender (partner_id, recommender_id, name) values (1, 1, 'demo-episodes');

commit;

select create_product_view(1);
select create_product_view(2);
select create_product_view(3);
select create_product_view(4);
select create_product_view(5);
select create_product_view(6);
select create_product_view(7);
select create_product_view(8);
commit;

/* Functions used by the service. These are for a database without partitions. */

create function log_svc_rec_partition_insert_function(
  in_id r_id,
  in_partner_id r_id,
  in_consumer_id r_id,
  in_max_recommendations r_int,
  in_resp_code r_int,
  in_failure_condition r_text256,
  in_failed_request r_text256,
  in_request_timestamp r_timestamp,
  in_request_duration_ms r_milliseconds,
  in_partner_recommender_name r_varchar50,
  in_blender_name r_varchar100,
  in_candidates jsonb,
  in_ret_products jsonb,
  in_ref_products jsonb,
  in_data jsonb,
  in_attributes jsonb,
  in_feedback jsonb
) returns bigint as $$
declare
  _id bigint;
begin
  if in_id is null then
    insert into log_svc_rec (partner_id, consumer_id, max_recommendations, resp_code, failure_condition, failed_request, request_timestamp, request_duration_ms, partner_recommender_name, blender_name, candidates, ret_products, ref_products, data, attributes, blender_feedback)
      values (in_partner_id, in_consumer_id, in_max_recommendations, in_resp_code, in_failure_condition, in_failed_request, in_request_timestamp, in_request_duration_ms, in_partner_recommender_name, in_blender_name, in_candidates, in_ret_products, in_ref_products, in_data, in_attributes, in_feedback)
      returning id
      into _id;
  else
    insert into log_svc_rec (id, partner_id, consumer_id, max_recommendations, resp_code, failure_condition, failed_request, request_timestamp, request_duration_ms, partner_recommender_name, blender_name, candidates, ret_products, ref_products, data, attributes, blender_feedback)
      values (in_id, in_partner_id, in_consumer_id, in_max_recommendations, in_resp_code, in_failure_condition, in_failed_request, in_request_timestamp, in_request_duration_ms, in_partner_recommender_name, in_blender_name, in_candidates, in_ret_products, in_ref_products, in_data, in_attributes, in_feedback)
      returning id
      into _id;
  end if;
  return _id;
end;
$$ language plpgsql;

create function consumer_event_partition_insert_function(
  in_id r_id,
  in_event_timestamp r_timestamp,
  in_partner_id r_id,
  in_consumer_id r_id,
  in_product_id r_id,
  in_event_type_id r_id,
  in_request_timestamp r_timestamp,
  in_user_profile_id r_id,
  in_response_code r_int,
  in_failure_condition r_text256,
  in_failed_request r_text256,
  in_request_duration r_milliseconds,
  in_info jsonb
) returns bigint as $$
declare
  _id bigint;
begin
  if in_id is null then
    insert into consumer_event (event_timestamp, partner_id, consumer_id, product_id, event_type_id, user_profile_id, response_code, failure_condition, failed_request,  request_duration_ms, request_timestamp, info)
      values (in_event_timestamp, in_partner_id, in_consumer_id, in_product_id, in_event_type_id, in_user_profile_id, in_response_code, in_failure_condition, in_failed_request, in_request_duration, in_request_timestamp, in_info)
      returning id
      into _id;
  else
    insert into consumer_event (id, event_timestamp, partner_id, consumer_id, product_id, event_type_id, user_profile_id, response_code, failure_condition, failed_request,  request_duration_ms, request_timestamp, info)
      values (in_id, in_event_timestamp, in_partner_id, in_consumer_id, in_product_id, in_event_type_id, in_user_profile_id, in_response_code, in_failure_condition, in_failed_request, in_request_duration, in_request_timestamp, in_info)
      returning id
      into _id;
  end if;
  return _id;
end;
$$ language plpgsql;

-- drop function log_svc_search_partition_insert_function(r_id, r_id, r_id, r_int, r_int, r_text256, r_text256, r_timestamp, r_milliseconds, r_varchar50, r_varchar100, jsonb, jsonb, jsonb, jsonb, jsonb, jsonb);
create function log_svc_search_partition_insert_function(
  in_id r_id,
  in_partner_id r_id,
  in_consumer_id r_id,
  in_max_items r_int,
  in_resp_code r_int,
  in_failure_condition r_text256,
  in_failed_request r_text256,
  in_request_timestamp r_timestamp,
  in_request_duration_ms r_milliseconds,
  in_requested_blender_name r_varchar50,
  in_blender_name r_varchar100,
  in_ret_products jsonb,
  in_data jsonb,
  in_candidates jsonb,
  in_ref_products jsonb,
  in_attributes jsonb,
  in_feedback jsonb
) returns bigint as $$
declare
  _id bigint;
begin
  if in_id is null then
    insert into log_svc_search (partner_id, consumer_id, max_items, resp_code, failure_condition, failed_request, request_timestamp, request_duration_ms, requested_blender_name, blender_name, ret_products, data, candidates, ref_products, attributes, blender_feedback)
      values (in_partner_id, in_consumer_id, in_max_items, in_resp_code, in_failure_condition, in_failed_request, in_request_timestamp, in_request_duration_ms, in_requested_blender_name, in_blender_name, in_ret_products, in_data, in_candidates, in_ref_products, in_attributes, in_feedback)
      returning id
      into _id;
  else
    insert into log_svc_search (id, partner_id, consumer_id, max_items, resp_code, failure_condition, failed_request, request_timestamp, request_duration_ms, requested_blender_name, blender_name, ret_products, data, candidates, ref_products, attributes, blender_feedback)
      values (in_id, in_partner_id, in_consumer_id, in_max_items, in_resp_code, in_failure_condition, in_failed_request, in_request_timestamp, in_request_duration_ms, in_requested_blender_name, in_blender_name, in_ret_products, in_data, in_candidates, in_ref_products, in_attributes, in_feedback)
      returning id
      into _id;
  end if;
  return _id;
end;
$$ language plpgsql;

commit;
