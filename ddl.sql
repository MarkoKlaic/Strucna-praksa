-- === SCHEMA ===
drop schema if exists fleet cascade;
create schema fleet;

-- === ŠIFRARNICI ===
create table fleet.dc_vehicle_make(
  make_id bigserial primary key,
  name text not null unique
);

create table fleet.dc_vehicle_model(
  model_id bigserial primary key,
  make_id bigint not null references fleet.dc_vehicle_make(make_id) on delete restrict,
  name text not null,
  unique(make_id, name)
);

create table fleet.dc_vehicle_category(
  category_id bigserial primary key,
  name text not null unique
);

create table fleet.dc_fuel_type(
  fuel_type_id bigserial primary key,
  code text not null unique,
  label text
);

-- === JEZGRA ===
create table fleet.employee(
  employee_id bigserial primary key,
  first_name text not null,
  last_name  text not null,
  email text,
  phone text
);

create table fleet.driver(
  driver_id bigint primary key references fleet.employee(employee_id) on delete cascade,
  license_no text not null,
  license_issue_at date,
  license_expire_at date
);

create table fleet.vehicle(
  vehicle_id bigserial primary key,
  registration_number text not null unique,
  vin text not null unique,
  model_id bigint not null references fleet.dc_vehicle_model(model_id) on delete restrict,
  category_id bigint references fleet.dc_vehicle_category(category_id),
  fuel_type_id bigint references fleet.dc_fuel_type(fuel_type_id),
  year int,
  status text not null default 'active' check (status in ('active','service','retired','sold')),
  current_odometer_km numeric(10,1) default 0 check (current_odometer_km >= 0),
  created_at timestamptz not null default now()
);

-- === OPERATIVA ===
create table fleet.vehicle_assignment(
  assignment_id bigserial primary key,
  vehicle_id bigint not null references fleet.vehicle(vehicle_id) on delete cascade,
  driver_id  bigint not null references fleet.driver(driver_id) on delete restrict,
  start_at timestamptz not null,
  end_at   timestamptz,
  notes text
);

create table fleet.odometer_reading(
  reading_id bigserial primary key,
  vehicle_id bigint not null references fleet.vehicle(vehicle_id) on delete cascade,
  reading_at timestamptz not null,
  odometer_km numeric(10,1) not null check (odometer_km >= 0),
  is_anomalous boolean not null default false
);

create table fleet.fuel_card(
  fuel_card_id bigserial primary key,
  card_no text not null unique,
  vendor_name text,
  active boolean not null default true
);

create table fleet.fuel_transaction(
  fuel_tx_id bigserial primary key,
  vehicle_id bigint not null references fleet.vehicle(vehicle_id) on delete cascade,
  driver_id  bigint references fleet.driver(driver_id) on delete set null,
  fuel_card_id bigint references fleet.fuel_card(fuel_card_id) on delete set null,
  posted_at timestamptz not null,
  liters numeric(10,2) not null check (liters >= 0),
  price_per_liter numeric(10,3) not null check (price_per_liter >= 0),
  odometer_km numeric(10,1) not null check (odometer_km >= 0),
  station_name text,
  receipt_number text
);

-- === ODRŽAVANJE ===
create table fleet.vendor(
  vendor_id bigserial primary key,
  name text not null,
  address text,
  phone text,
  active boolean not null default true
);

create table fleet.maintenance_order(
  mo_id bigserial primary key,
  vehicle_id bigint not null references fleet.vehicle(vehicle_id) on delete cascade,
  vendor_id  bigint references fleet.vendor(vendor_id) on delete set null,
  opened_at timestamptz not null,
  started_at    timestamptz,
  closed_at timestamptz,
  status text not null default 'open' check (status in ('open','in_progress','closed', 'cancelled')),
  total_cost numeric(12,2) not null default 0 check (total_cost >= 0),
  description text not null
);

create table fleet.maintenance_item(
  item_id bigserial primary key,
  mo_id bigint not null references fleet.maintenance_order(mo_id) on delete cascade,
  work_type text not null,
  parts_cost numeric(12,2) not null default 0 check (parts_cost >= 0),
  labor_cost numeric(12,2) not null default 0 check (labor_cost >= 0),
  description text not null
);

-- === USKLAĐENOST / DOKUMENTI ===
create table fleet.insurance_policy(
  policy_id bigserial primary key,
  vehicle_id bigint not null references fleet.vehicle(vehicle_id) on delete cascade,
  provider text not null,
  policy_no text not null,
  valid_from timestamptz not null,
  valid_to   timestamptz not null check (valid_to > valid_from),
  premium_amount numeric(12,2) not null,
  coverage_notes text,
  unique(vehicle_id, provider, policy_no)
);

create table fleet.registration_record(
  reg_id bigserial primary key,
  vehicle_id bigint not null references fleet.vehicle(vehicle_id) on delete cascade,
  valid_from timestamptz not null,
  valid_to   timestamptz not null check (valid_to > valid_from),
  fee_amount numeric(12,2) not null
);

create table fleet.dc_document_type (
  code        text primary key,          -- INSURANCE, REGISTRATION, SERVICE, OTHER
  description text not null
);

create table fleet.document(
  document_id bigserial primary key,
  doc_type_code text not null references fleet.dc_document_type(code) on update cascade,
  title text not null,
  url_or_path text not null,
  uploaded_by  bigint not null references fleet.employee(employee_id) on delete restrict, 
  uploaded_at timestamptz not null default now(),
  is_deleted boolean not null default false,
  constraint chk_document_url_valid
    check (url_or_path ~ '^(https?://|/).+')
);

create table fleet.vehicle_document(
  vehicle_document_id bigserial primary key,
  vehicle_id bigint not null references fleet.vehicle(vehicle_id) on delete cascade,
  document_id bigint not null references fleet.document(document_id) on delete restrict,
  doc_type_code text not null references fleet.dc_document_type(code) on update cascade,
  unique(vehicle_id, document_id)
);

-- === DOGAĐAJI ===
-- 20. Kazna
create table fleet.fine(
  fine_id bigserial primary key,
  vehicle_id bigint not null references fleet.vehicle(vehicle_id) on delete cascade,
  driver_id  bigint references fleet.driver(driver_id) on delete set null,
  issued_at timestamptz not null,
  amount numeric(12,2) not null check (amount > 0),
  reason text not null,
  paid_at timestamptz,
  payment_method text,
  constraint chk_fine
    check (paid_at is null or nullif(trim(payment_method), '') is not null)
);

create table fleet.accident(
  accident_id bigserial primary key,
  vehicle_id bigint not null references fleet.vehicle(vehicle_id) on delete cascade,
  driver_id  bigint references fleet.driver(driver_id) on delete set null,
  happened_at timestamptz not null,
  severity text not null check (severity in ('minor','major','total')),
  description text,
  cost_estimate numeric(12,2) check (cost_estimate >= 0)
);


-- === VIEW ===

create or replace view fleet.v_active_assignments as
select a.* from fleet.vehicle_assignment a where a.end_at is null;


create or replace view fleet.v_monthly_fuel_stats as
with fuel_by_month as (
  select
    vehicle_id,
    date_trunc('month', posted_at) as month,
    sum(liters)                     as liters_total,
    sum(liters * price_per_liter)   as fuel_cost_total
  from fleet.fuel_transaction
  group by vehicle_id, date_trunc('month', posted_at)
),
odo_by_month as (
  -- udaljenost = (max(odo) - min(odo)) unutar mjeseca
  select
    vehicle_id,
    date_trunc('month', reading_at) as month,
    (max(odometer_km) - min(odometer_km))::numeric(12,1) as distance_km
  from fleet.odometer_reading
  group by vehicle_id, date_trunc('month', reading_at)
)
select
  coalesce(f.vehicle_id, o.vehicle_id) as vehicle_id,
  coalesce(f.month, o.month)            as month,
  coalesce(f.liters_total, 0)::numeric(12,2)     as liters_total,
  coalesce(f.fuel_cost_total, 0)::numeric(12,2)  as fuel_cost_total,
  coalesce(o.distance_km, 0)::numeric(12,1)      as distance_km,
  case
    when coalesce(o.distance_km,0) > 0
      then round( (coalesce(f.liters_total,0) * 100.0) / o.distance_km, 2)
    else null
  end as l_per_100km
from fuel_by_month f
full join odo_by_month o
  on f.vehicle_id = o.vehicle_id and f.month = o.month;


create or replace view fleet.v_cost_per_km_overall as
with dist as (
  select
    vehicle_id,
    (max(odometer_km) - min(odometer_km))::numeric(12,1) as distance_km
  from fleet.odometer_reading
  group by vehicle_id
),
fuel_cost as (
  select
    vehicle_id,
    sum(liters * price_per_liter)::numeric(14,2) as total_fuel_cost
  from fleet.fuel_transaction
  group by vehicle_id
),
maint_cost as (
  select
    vehicle_id,
    sum(total_cost)::numeric(14,2) as total_maint_cost
  from fleet.maintenance_order
  group by vehicle_id
)
select
  v.vehicle_id,
  coalesce(d.distance_km, 0)                         as distance_km,
  coalesce(fc.total_fuel_cost, 0)                    as total_fuel_cost,
  coalesce(mc.total_maint_cost, 0)                   as total_maint_cost,
  (coalesce(fc.total_fuel_cost,0) + coalesce(mc.total_maint_cost,0))
    as total_cost,
  case
    when coalesce(d.distance_km,0) > 0
      then round( (coalesce(fc.total_fuel_cost,0) + coalesce(mc.total_maint_cost,0)) / d.distance_km, 4)
    else null
  end as cost_per_km
from fleet.vehicle v
left join dist d      on d.vehicle_id = v.vehicle_id
left join fuel_cost fc on fc.vehicle_id = v.vehicle_id
left join maint_cost mc on mc.vehicle_id = v.vehicle_id;

-- ===================================== PROŠIRENA POSLOVNA PRAVILA =====================================

-- ===================================== 1 =====================================
-- U tablici već napravljeno

-- ===================================== 2 =====================================
-- Blokada operacija za vozila sold
create or replace function fleet.ensure_vehicle_operable() returns trigger language plpgsql as $$
declare v_status text;
begin
  select status into v_status from fleet.vehicle where vehicle_id = new.vehicle_id;
  if v_status in ('sold') then
    raise exception 'Operacija nije dopuštena za vozilo u statusu %', v_status;
  end if;
  return new;
end$$;

drop trigger if exists btri_assign_operable on fleet.vehicle_assignment;
create trigger btri_assign_operable before insert on fleet.vehicle_assignment
for each row execute procedure fleet.ensure_vehicle_operable();

drop trigger if exists btri_fuel_operable on fleet.fuel_transaction;
create trigger btri_fuel_operable before insert on fleet.fuel_transaction
for each row execute procedure fleet.ensure_vehicle_operable();

drop trigger if exists btri_mo_operable on fleet.maintenance_order;
create trigger btri_mo_operable before insert on fleet.maintenance_order
for each row execute procedure fleet.ensure_vehicle_operable();

-- ===================================== 3 =====================================
-- U tablici već napravljeno

-- ===================================== 4 =====================================
-- Jedna aktivna dodjela
create unique index ux_vehicle_active_assignment
  on fleet.vehicle_assignment(vehicle_id) where end_at is null;

-- ===================================== 5 =====================================
-- Vozacka dozvola (license_expiry)
create or replace function fleet.ensure_driver_license_valid()
returns trigger
language plpgsql as $$
declare
  v_expire date;
  v_start  timestamptz;
begin
  select license_expire_at into v_expire
  from fleet.driver
  where driver_id = new.driver_id;

  v_start := coalesce(new.start_at, now());

  if v_expire is not null and v_start::date > v_expire then
    raise exception using message = format(
      'Vozaču (id=%s) je istekla vozačka (%s); početak dodjele: %s',
      new.driver_id, v_expire, v_start
    );
  end if;

  return new;
end$$;

drop trigger if exists btri_assignment_driver_license on fleet.vehicle_assignment;
create trigger btri_assignment_driver_license
before insert or update of driver_id, start_at on fleet.vehicle_assignment
for each row execute procedure fleet.ensure_driver_license_valid();

-- ===================================== 6 =====================================
-- Tablica reservation koju nemam

-- ===================================== 7 =====================================
-- Monotonost km (odometer_reading)
create or replace function fleet.trg_odo_monotonic() returns trigger language plpgsql as $$
begin
  if (tg_op = 'INSERT' or tg_op='UPDATE') then
    if exists (
      select 1 from fleet.odometer_reading r
      where r.vehicle_id = new.vehicle_id
        and r.reading_at < coalesce(new.reading_at, now())
        and r.odometer_km > new.odometer_km
    ) then
      raise exception 'Odometer mora biti monoton (vozilo=%)', new.vehicle_id;
    end if;
  end if;
  return new;
end$$;

drop trigger if exists btri_odo_monotonic on fleet.odometer_reading;
create trigger btri_odo_monotonic
before insert or update on fleet.odometer_reading
for each row execute procedure fleet.trg_odo_monotonic();

-- ===================================== 8 =====================================
-- Sanity check brzine rasta km 
create or replace view fleet.v_odo_speed_anomalies as
with r as (
  select
    vehicle_id,
    reading_id,
    reading_at,
    odometer_km,
    lag(odometer_km) over (partition by vehicle_id order by reading_at)      as prev_km,
    lag(reading_at)   over (partition by vehicle_id order by reading_at)      as prev_at
  from fleet.odometer_reading
)
select
  vehicle_id,
  reading_id,
  reading_at,
  odometer_km,
  case
    when prev_km is null then null
    else (odometer_km - prev_km)
  end                                as km_diff,
  case
    when prev_at is null then null
    else extract(epoch from (reading_at - prev_at)) / 86400.0
  end                                as days_diff,
  case
    when prev_km is null or prev_at is null then null
    else (odometer_km - prev_km) /
         nullif(extract(epoch from (reading_at - prev_at)) / 86400.0, 0)
  end                                as km_per_day,
  case
    when prev_km is null or prev_at is null then false
    else ( (odometer_km - prev_km) /
           nullif(extract(epoch from (reading_at - prev_at)) / 86400.0, 0) ) > 1500
  end                                as is_anomalous
from r;
  
-- ===================================== 9 =====================================
-- Dodatno, to nemam
-- === 9. TRIP LOG (opcionalno) ==========================================
-- Evidencija vožnji: vremenski raspon i kilometraža po vožnji

create table if not exists fleet.trip_log (
  trip_id            bigserial primary key,
  vehicle_id         bigint not null references fleet.vehicle(vehicle_id) on delete cascade,
  driver_id          bigint references fleet.driver(driver_id) on delete set null,
  start_at           timestamptz not null,
  end_at             timestamptz not null,
  start_odometer_km  numeric(10,1) not null check (start_odometer_km >= 0),
  end_odometer_km    numeric(10,1) not null check (end_odometer_km   >= 0),
  distance_km        numeric(10,1) not null default 0 check (distance_km >= 0),
  purpose            text,
  notes              text,
  -- Osnovna valjanost: vrijeme i km moraju rasti
  constraint chk_trip_time   check (end_at > start_at),
  constraint chk_trip_km     check (end_odometer_km >= start_odometer_km)
);

-- Auto-izračun distance_km = end - start
create or replace function fleet.trg_trip_calc_distance()
returns trigger language plpgsql as $$
begin
  new.distance_km := greatest(coalesce(new.end_odometer_km,0) - coalesce(new.start_odometer_km,0), 0);
  return new;
end$$;

drop trigger if exists btrb_trip_calc on fleet.trip_log;
create trigger btrb_trip_calc
before insert or update of start_odometer_km, end_odometer_km
on fleet.trip_log
for each row execute procedure fleet.trg_trip_calc_distance();

-- Indeksi za česte upite (po vozilu i vremenu)
create index if not exists ix_trip_vehicle_time on fleet.trip_log(vehicle_id, start_at);


-- ===================================== 10 =====================================
-- Konzistentnost km pri točenju
create or replace function fleet.ensure_fuel_odometer_consistent() returns trigger
language plpgsql as $$
declare last_read_km numeric(10,1);
begin
  select r.odometer_km
    into last_read_km
  from fleet.odometer_reading r
  where r.vehicle_id = new.vehicle_id
    and r.reading_at <= coalesce(new.posted_at, now())
  order by r.reading_at desc
  limit 1;

  if last_read_km is not null and new.odometer_km < last_read_km then
    raise exception 'Točenje ima odometer_km (%) manji od zadnjeg očitanja (%) za vozilo %',
      new.odometer_km, last_read_km, new.vehicle_id;
  end if;

  return new;
end$$;

drop trigger if exists btri_fuel_odo_consistency on fleet.fuel_transaction;
create trigger btri_fuel_odo_consistency
before insert or update of odometer_km, posted_at on fleet.fuel_transaction
for each row execute procedure fleet.ensure_fuel_odometer_consistent();


-- ===================================== 11 =====================================
-- Anomalije potrošnje
alter table fleet.fuel_transaction
  add column if not exists is_suspicious boolean not null default false;

create or replace function fleet.flag_fuel_suspicious() returns trigger
language plpgsql as $$
declare
  prev_km numeric(10,1);
  prev_at timestamptz;
  dist_km numeric;
  l_per_100 numeric;
  avg_l100 numeric;
begin
  -- najbliže prethodno očitanje do trenutka točenja
  select r.odometer_km, r.reading_at
    into prev_km, prev_at
  from fleet.odometer_reading r
  where r.vehicle_id = new.vehicle_id
    and r.reading_at <= coalesce(new.posted_at, now())
  order by r.reading_at desc
  limit 1;

  new.is_suspicious := false;

  if prev_km is not null and new.odometer_km > prev_km then
    dist_km := new.odometer_km - prev_km;
    l_per_100 := (new.liters * 100.0) / greatest(dist_km, 0.0001);

    -- prosjek vozila iz povijesnih točenja (gruba procjena)
    select avg( (ft.liters * 100.0) / nullif(ft.odometer_km - pr.prev_km, 0) )
      into avg_l100
    from fleet.fuel_transaction ft
    join lateral (
      select max(r2.odometer_km) as prev_km
      from fleet.odometer_reading r2
      where r2.vehicle_id = ft.vehicle_id
        and r2.reading_at <= coalesce(ft.posted_at, now())
    ) pr on true
    where ft.vehicle_id = new.vehicle_id
      and pr.prev_km is not null
      and ft.odometer_km > pr.prev_km;

    if avg_l100 is not null and l_per_100 > avg_l100 * 1.30 then
      new.is_suspicious := true;
    end if;
  end if;

  return new;
end$$;

drop trigger if exists btrb_fuel_suspicious on fleet.fuel_transaction;
create trigger btrb_fuel_suspicious
before insert or update of liters, odometer_km, posted_at on fleet.fuel_transaction
for each row execute procedure fleet.flag_fuel_suspicious();

alter table fleet.fuel_transaction
  add column if not exists is_card_mismatch boolean not null default false;

-- ===================================== 12 =====================================
-- Kartica goriva
create or replace function fleet.flag_fuel_card_mismatch() returns trigger
language plpgsql as $$
declare v_assigned bigint;
begin
  new.is_card_mismatch := false;

  if new.fuel_card_id is not null then
    select assigned_vehicle_id
      into v_assigned
    from fleet.fuel_card
    where fuel_card_id = new.fuel_card_id;

    if v_assigned is not null and v_assigned <> new.vehicle_id then
      new.is_card_mismatch := true; -- dokument kaže: “inače flag”
    end if;
  end if;

  return new;
end$$;

drop trigger if exists btrb_fuel_card_mismatch on fleet.fuel_transaction;
create trigger btrb_fuel_card_mismatch
before insert or update of vehicle_id, fuel_card_id on fleet.fuel_transaction
for each row execute procedure fleet.flag_fuel_card_mismatch();

-- ===================================== 13 =====================================
-- Workflow naloga
create or replace function fleet.ensure_mo_workflow_min() returns trigger
language plpgsql as $$
declare has_items int;
begin
  -- Ako mijenjamo status, dozvoljeni su SAMO:
  -- open -> in_progress | cancelled
  -- in_progress -> closed | cancelled
  -- closed/cancelled = terminalno
  if tg_op='UPDATE' and new.status <> old.status then
    if    (old.status='open'        and new.status in ('in_progress','cancelled'))
       or (old.status='in_progress' and new.status in ('closed','cancelled')) then
      null; -- ok
    elsif old.status in ('closed','cancelled') then
      raise exception 'Nalog je već % i status se više ne može mijenjati.', old.status;
    else
      raise exception 'Nedozvoljen prijelaz statusa: % -> %', old.status, new.status;
    end if;
  end if;


-- ===================================== 14 =====================================
-- Zatvaranje naloga
if new.status='closed' then
  select count(*) into has_items from fleet.maintenance_item where mo_id=new.mo_id;
  if has_items = 0 then
    raise exception 'Nalog % se ne može zatvoriti bez stavki.', new.mo_id;
  end if;
end if;

return new;
end$$;

drop trigger if exists btrb_mo_workflow on fleet.maintenance_order;
create trigger btrb_mo_workflow
before update on fleet.maintenance_order
for each row execute procedure fleet.ensure_mo_workflow_min();

-- ===================================== 15 =====================================
-- Trošak naloga
alter table fleet.maintenance_item
  add column if not exists line_total numeric(12,2) not null default 0
  check (line_total >= 0);

-- line_total = parts + labor
create or replace function fleet.mitem_calc_line_total()
returns trigger
language plpgsql as $$
begin
  new.line_total := coalesce(new.parts_cost, 0) + coalesce(new.labor_cost, 0);
  return new;
end$$;

drop trigger if exists btrb_mitem_line on fleet.maintenance_item;
create trigger btrb_mitem_line
before insert or update of parts_cost, labor_cost
on fleet.maintenance_item
for each row
execute procedure fleet.mitem_calc_line_total();

-- AFTER trigger: rollup u maintenance_order.total_cost
create or replace function fleet.trg_mo_rollup() returns trigger
language plpgsql as $$
begin
  update fleet.maintenance_order m
     set total_cost = coalesce((
       select sum(i.line_total) from fleet.maintenance_item i
       where i.mo_id = m.mo_id
     ),0)
  where m.mo_id = coalesce(new.mo_id, old.mo_id);
  return null;
end$$;

drop trigger if exists atru_mitem_rollup on fleet.maintenance_item;
create trigger atru_mitem_rollup
after insert or update or delete on fleet.maintenance_item
for each row execute procedure fleet.trg_mo_rollup();

-- ===================================== 16 =====================================
-- Planirani servis OPCIONALNO

create table if not exists fleet.planned_service (
  plan_id       bigserial primary key,
  vehicle_id    bigint not null
                 references fleet.vehicle(vehicle_id) on delete cascade,
  mo_id         bigint
                 references fleet.maintenance_order(mo_id) on delete set null,
  due_at        timestamptz,
  due_odometer  int,
  description   text not null,
  status        text not null default 'planned'
                 check (status in ('planned','done','cancelled')),
  created_at    timestamptz not null default now(),
  check (due_at is not null or due_odometer is not null)
);

-- Pogled: “uskoro”, tj. u sljedećih 30 dana ili u sljedećih 1000 km
create or replace view fleet.v_planned_service_upcoming as
select
  ps.plan_id,
  ps.vehicle_id,
  v.registration_number,
  ps.description,
  ps.due_at,
  ps.due_odometer,
  ps.status
from fleet.planned_service ps
join fleet.vehicle v on v.vehicle_id = ps.vehicle_id
where ps.status = 'planned'
  and (
        (ps.due_at is not null and ps.due_at <= now() + interval '30 days')
     or (ps.due_odometer is not null and ps.due_odometer <= (
            select coalesce(max(or1.odometer_km), 0)
            from fleet.odometer_reading or1
            where or1.vehicle_id = ps.vehicle_id
         ) + 1000)
      );

-- ===================================== 17 =====================================
-- Ne preklapanje razdoblja
alter table fleet.insurance_policy
  drop constraint if exists ex_insurance_no_overlap;
alter table fleet.insurance_policy
  add constraint ex_insurance_no_overlap
  exclude using gist (
    vehicle_id with =,
    tstzrange(valid_from, valid_to, '[)') with &&
  );

alter table fleet.registration_record
  drop constraint if exists ex_registration_no_overlap;
alter table fleet.registration_record
  add constraint ex_registration_no_overlap
  exclude using gist (
    vehicle_id with =,
    tstzrange(valid_from, valid_to, '[)') with &&
  );


-- ===================================== 18 =====================================
-- valid_to > valid_from imam u tablici napravljeno
-- Police: period je nepromjenjiv → produženje ide NOVIM zapisom
create or replace function fleet.prevent_insurance_period_update()
returns trigger
language plpgsql as $$
begin
  if new.valid_from is distinct from old.valid_from
     or new.valid_to   is distinct from old.valid_to then
    raise exception 'Period police je nepromjenjiv; za produženje kreiraj novi zapis.';
  end if;
  return new;
end$$;

drop trigger if exists btrb_no_edit_period_ins_pol on fleet.insurance_policy;
create trigger btrb_no_edit_period_ins_pol
before update of valid_from, valid_to on fleet.insurance_policy
for each row
execute procedure fleet.prevent_insurance_period_update();

-- Registracija: period je nepromjenjiv → produženje ide NOVIM zapisom
create or replace function fleet.prevent_registration_period_update()
returns trigger
language plpgsql as $$
begin
  if new.valid_from is distinct from old.valid_from
     or new.valid_to   is distinct from old.valid_to then
    raise exception 'Period registracije je nepromjenjiv; za produženje kreiraj novi zapis.';
  end if;
  return new;
end$$;

drop trigger if exists btrb_no_edit_period_reg_rec on fleet.registration_record;
create trigger btrb_no_edit_period_reg_rec
before update of valid_from, valid_to on fleet.registration_record
for each row
execute procedure fleet.prevent_registration_period_update();

-- ===================================== 19 =====================================
-- Inspection (opcionalno)
create table fleet.inspection (
  inspection_id bigserial primary key,
  vehicle_id    bigint not null references fleet.vehicle(vehicle_id) on delete cascade,
  inspected_at  timestamptz not null,
  result        text not null,
  note          text,
  constraint chk_inspection_one
    check (
      result in ('passed','failed') and
      (result <> 'failed' or nullif(trim(note), '') is not null)
    )
);

-- ===================================== 20 =====================================
-- Napravljeno u tablici

-- ===================================== 21 =====================================
-- Nezgoda: severity
-- Severity u tablici
-- Drugi dio:
create or replace function fleet.accident_auto_retire() returns trigger
language plpgsql as $$
begin
  if new.severity = 'total' then
    update fleet.vehicle
       set status = 'retired'
     where vehicle_id = new.vehicle_id
       and status not in ('retired','sold');  -- ne diraj već retired/sold
  end if;
  return new;
end$$;

drop trigger if exists atru_accident_auto_retire on fleet.accident;
create trigger atru_accident_auto_retire
after insert or update of severity on fleet.accident
for each row execute procedure fleet.accident_auto_retire();

-- ===================================== 22 =====================================
-- Drugi dio odrađen u tablici vehicle_document
-- 22 Dozvoljeni tipovi

insert into fleet.dc_document_type (code, description) values
  ('INSURANCE',    'Polica osiguranja'),
  ('REGISTRATION', 'Prometna dozvola / registracija'),
  ('SERVICE',      'Servisna dokumentacija'),
  ('OTHER',        'Ostalo')
on conflict (code) do nothing;

-- ===================================== 23 =====================================
-- Integritet dokumenta
-- Dio s URL i uploaded_by uraden u tablici
-- skini postojeći FK (ako je bio s CASCADE) pa dodaj RESTRICT
do $$
declare cname text;
begin
  select constraint_name into cname
  from information_schema.table_constraints
  where table_schema='fleet' and table_name='vehicle_document'
    and constraint_type='FOREIGN KEY'
    and constraint_name like '%document_id%';
  if cname is not null then
    execute format('alter table fleet.vehicle_document drop constraint %I', cname);
  end if;
end$$;

create or replace function fleet.trg_vehicle_document_no_deleted_doc()
returns trigger language plpgsql as $$
declare v_del boolean;
begin
  select is_deleted into v_del
  from fleet.document
  where document_id = new.document_id;

  if coalesce(v_del,false) then
    raise exception 'Dokument % je soft-deleted; povezivanje nije dopušteno.', new.document_id;
  end if;

  return new;
end$$;

drop trigger if exists btrb_vehicle_document_no_deleted on fleet.vehicle_document;
create trigger btrb_vehicle_document_no_deleted
before insert or update on fleet.vehicle_document
for each row execute procedure fleet.trg_vehicle_document_no_deleted_doc();


-- ===================================== 24 =====================================
-- Audit stupci

alter table if exists fleet.vehicle
  add column if not exists created_at  timestamptz,
  add column if not exists created_by  text,
  add column if not exists modified_at timestamptz,
  add column if not exists modified_by text;

alter table if exists fleet.driver
  add column if not exists created_at  timestamptz,
  add column if not exists created_by  text,
  add column if not exists modified_at timestamptz,
  add column if not exists modified_by text;

alter table if exists fleet.vendor
  add column if not exists created_at  timestamptz,
  add column if not exists created_by  text,
  add column if not exists modified_at timestamptz,
  add column if not exists modified_by text;

alter table if exists fleet.maintenance_order
  add column if not exists created_at  timestamptz,
  add column if not exists created_by  text,
  add column if not exists modified_at timestamptz,
  add column if not exists modified_by text;

alter table if exists fleet.maintenance_item
  add column if not exists created_at  timestamptz,
  add column if not exists created_by  text,
  add column if not exists modified_at timestamptz,
  add column if not exists modified_by text;

alter table if exists fleet.insurance_policy
  add column if not exists created_at  timestamptz,
  add column if not exists created_by  text,
  add column if not exists modified_at timestamptz,
  add column if not exists modified_by text;

alter table if exists fleet.registration_record
  add column if not exists created_at  timestamptz,
  add column if not exists created_by  text,
  add column if not exists modified_at timestamptz,
  add column if not exists modified_by text;

alter table if exists fleet.inspection
  add column if not exists created_at  timestamptz,
  add column if not exists created_by  text,
  add column if not exists modified_at timestamptz,
  add column if not exists modified_by text;

alter table if exists fleet.fine
  add column if not exists created_at  timestamptz,
  add column if not exists created_by  text,
  add column if not exists modified_at timestamptz,
  add column if not exists modified_by text;

alter table if exists fleet.accident
  add column if not exists created_at  timestamptz,
  add column if not exists created_by  text,
  add column if not exists modified_at timestamptz,
  add column if not exists modified_by text;

alter table if exists fleet.document
  add column if not exists created_at  timestamptz,
  add column if not exists created_by  text,
  add column if not exists modified_at timestamptz,
  add column if not exists modified_by text;

alter table if exists fleet.vehicle_document
  add column if not exists created_at  timestamptz,
  add column if not exists created_by  text,
  add column if not exists modified_at timestamptz,
  add column if not exists modified_by text;

alter table if exists fleet.fuel_card
  add column if not exists created_at  timestamptz,
  add column if not exists created_by  text,
  add column if not exists modified_at timestamptz,
  add column if not exists modified_by text;

alter table if exists fleet.fuel_transaction
  add column if not exists created_at  timestamptz,
  add column if not exists created_by  text,
  add column if not exists modified_at timestamptz,
  add column if not exists modified_by text;

-- ===================================== 25 =====================================
-- Soft delete
alter table if exists fleet.vehicle
  add column if not exists is_deleted boolean not null default false;

alter table if exists fleet.employee
  add column if not exists is_deleted boolean not null default false;

alter table if exists fleet.driver
  add column if not exists is_deleted boolean not null default false;

alter table if exists fleet.vendor
  add column if not exists is_deleted boolean not null default false;

create or replace function fleet.prevent_hard_delete()
returns trigger language plpgsql as $$
begin
  raise exception 'Hard delete nije dozvoljen; koristi soft-delete (is_deleted=true).';
end$$;

drop trigger if exists btrb_vehicle_no_delete  on fleet.vehicle;
drop trigger if exists btrb_employee_no_delete on fleet.employee;
drop trigger if exists btrb_driver_no_delete   on fleet.driver;
drop trigger if exists btrb_vendor_no_delete   on fleet.vendor;

create trigger btrb_vehicle_no_delete  before delete on fleet.vehicle  for each row execute procedure fleet.prevent_hard_delete();
create trigger btrb_employee_no_delete before delete on fleet.employee for each row execute procedure fleet.prevent_hard_delete();
create trigger btrb_driver_no_delete   before delete on fleet.driver   for each row execute procedure fleet.prevent_hard_delete();
create trigger btrb_vendor_no_delete   before delete on fleet.vendor   for each row execute procedure fleet.prevent_hard_delete();

-- ===================================== 26 =====================================
-- Indeksi
-- === Maintenance ===
create index if not exists ix_mo_status
  on fleet.maintenance_order(status);

create index if not exists ix_mo_vehicle_status
  on fleet.maintenance_order(vehicle_id, status);

create index if not exists ix_mo_vehicle_opened_at
  on fleet.maintenance_order(vehicle_id, opened_at);

create index if not exists ix_mitem_mo
  on fleet.maintenance_item(mo_id);

-- === Fuel (transakcije/gorivo) ===
create index if not exists ix_fuel_tx_vehicle_posted_at
  on fleet.fuel_transaction(vehicle_id, posted_at);

create index if not exists ix_fuel_tx_mismatch_only
  on fleet.fuel_transaction(vehicle_id)
  where is_card_mismatch = true;

create index if not exists ix_odo_vehicle_reading_at
  on fleet.odometer_reading (vehicle_id, reading_at);

create index if not exists ix_insurance_vehicle_period_gist
  on fleet.insurance_policy
  using gist (vehicle_id, tstzrange(valid_from, valid_to, '[)'));

create index if not exists ix_registration_vehicle_period_gist
  on fleet.registration_record
  using gist (vehicle_id, tstzrange(valid_from, valid_to, '[)'));

-- === Tehnički pregledi ===
create index if not exists ix_inspection_vehicle_at
  on fleet.inspection(vehicle_id, inspected_at);

-- === Kazne ===
create index if not exists ix_fine_vehicle_issued_at
  on fleet.fine(vehicle_id, issued_at);

create index if not exists ix_fine_driver_issued_at
  on fleet.fine(driver_id, issued_at);

create index if not exists ix_fine_unpaid_vehicle
  on fleet.fine(vehicle_id)
  where paid_at is null;

-- === Nezgode ===
create index if not exists ix_accident_vehicle_at
  on fleet.accident(vehicle_id, happened_at);

create index if not exists ix_accident_severity
  on fleet.accident(severity);

-- === Dokumenti ===
create index if not exists ix_vehicle_document_vehicle_type
  on fleet.vehicle_document(vehicle_id, doc_type_code);

create index if not exists ix_document_type
  on fleet.document(doc_type_code);

create index if not exists ix_document_uploaded_by_at
  on fleet.document(uploaded_by, uploaded_at);
