set search_path to fleet, public;

-- -------------------------------------------------
-- Šifrarnici 
-- -------------------------------------------------

-- Vehicle make
with x(name) as (
  values ('Volkswagen'), ('Ford'), ('Opel'), ('Renault'), ('Škoda')
)
insert into dc_vehicle_make(name)
select x.name
from x
where not exists (
  select 1 from dc_vehicle_make m where m.name = x.name
);

-- Modeli 
with v(make_name, model) as (
  values 
    ('Volkswagen','Golf'),
    ('Ford','Focus'),
    ('Opel','Astra'),
    ('Renault','Clio'),
    ('Škoda','Octavia')
)
insert into dc_vehicle_model(make_id, name)
select m.make_id, v.model
from v
join dc_vehicle_make m on m.name = v.make_name
where not exists (
  select 1 from dc_vehicle_model mm
  where mm.make_id = m.make_id and mm.name = v.model
);

-- Kategorije
with x(n) as (values ('Passenger'), ('Van'))
insert into dc_vehicle_category(name)
select n from x
where not exists (select 1 from dc_vehicle_category d where d.name = x.n);

-- Tipovi goriva
with x(code, label) as (
  values ('DIESEL','Diesel'), ('PETROL','Petrol')
)
insert into dc_fuel_type(code, label)
select x.code, x.label
from x
where not exists (select 1 from dc_fuel_type f where f.code = x.code);

-- -------------------------------------------------
-- Zaposlenici + vozači 
-- -------------------------------------------------
with e(first_name,last_name,email,phone) as (
  values
   ('Ivan','Ivić','ivan.ivic@example.com','091111111'),
   ('Ana','Anić','ana.anic@example.com','091111112'),
   ('Marko','Marić','marko.maric@example.com','091111113'),
   ('Iva','Ivić','iva.ivic@example.com','091111114'),
   ('Petar','Perić','petar.peric@example.com','091111115')
)
insert into employee(first_name,last_name,email,phone)
select e.first_name, e.last_name, e.email, e.phone
from e
where not exists (select 1 from employee t where t.email = e.email);

-- Mapiranje employee_id 
with x as (
  select employee_id, email
  from employee
  where email in ('ivan.ivic@example.com','ana.anic@example.com','marko.maric@example.com',
                  'iva.ivic@example.com','petar.peric@example.com')
)
insert into driver(driver_id, license_no, license_issue_at, license_expire_at)
select x.employee_id, ('B-'||x.employee_id)::text, (now() - interval '5 years')::date, (now() + interval '2 years')::date
from x
where not exists (select 1 from driver d where d.driver_id = x.employee_id);

-- -------------------------------------------------
-- Vozila 
-- -------------------------------------------------
with mdl as (
  select model_id, name
  from dc_vehicle_model
  where name in ('Golf','Focus','Astra','Clio','Octavia')
),
cat as (select category_id from dc_vehicle_category where name='Passenger'),
ft  as (select fuel_type_id from dc_fuel_type where code='DIESEL'),
src as (
  select 
    ('OS-'||to_char(g,'FM0000')||'-AB') as reg,
    ('WVWZZZ1JZ' || to_char(g,'FM000000')) as vin,
    (select model_id from mdl order by model_id limit 1 offset ((g-1) % 5)) as model_id,
    (select category_id from cat),
    (select fuel_type_id from ft),
    2020 + ((g-1) % 5) as year
  from generate_series(1,10) g
)
insert into vehicle(registration_number, vin, model_id, category_id, fuel_type_id, year, status, current_odometer_km)
select s.reg, s.vin, s.model_id, s.category_id, s.fuel_type_id, s.year, 'active', 120000 + (s.model_id % 5)*100
from src s
where not exists (
  select 1 from vehicle v where v.registration_number = s.reg or v.vin = s.vin
);

-- -------------------------------------------------
-- Početno očitanje km za sva vozila 
-- -------------------------------------------------
insert into odometer_reading(vehicle_id, reading_at, odometer_km)
select v.vehicle_id, now() - interval '10 days', v.current_odometer_km
from vehicle v
where not exists (
  select 1 from odometer_reading r 
  where r.vehicle_id = v.vehicle_id 
    and r.reading_at::date = (now() - interval '10 days')::date
);

-- -------------------------------------------------
-- Aktivne dodjele
-- -------------------------------------------------
with veh as (
  select vehicle_id, row_number() over(order by vehicle_id) as rn
  from vehicle
  order by vehicle_id
  limit 5
),
drv as (
  select driver_id, row_number() over(order by driver_id) as rn
  from driver
  order by driver_id
  limit 5
)
insert into vehicle_assignment(vehicle_id, driver_id, start_at, notes)
select v.vehicle_id, d.driver_id, now() - interval '60 days', 'Aktivna dodjela'
from veh v
join drv d on d.rn = v.rn
where not exists (
  select 1 from vehicle_assignment va
  where va.vehicle_id = v.vehicle_id and va.end_at is null
);

-- -------------------------------------------------
-- 20 točenja goriva 
-- -------------------------------------------------
with five as (select vehicle_id from vehicle order by vehicle_id limit 5),
tx as (
  select 
    v.vehicle_id,
    (now() - (interval '9 days') + make_interval(days := (i-1)*2)) as posted_at,
    7.0 + (i % 2)*7.0 as liters,
    1.70 as price_per_liter,
    120000.0 + (v.vehicle_id % 5)*100 + (i*100) as odo
  from five v
  cross join generate_series(1,4) as s(i)
)
insert into fuel_transaction(vehicle_id, driver_id, fuel_card_id, posted_at, liters, price_per_liter, odometer_km, station_name, receipt_number)
select 
  t.vehicle_id,
  (select d.driver_id from vehicle_assignment a join driver d on d.driver_id = a.driver_id 
     where a.vehicle_id = t.vehicle_id and a.end_at is null limit 1),
  null,
  t.posted_at, t.liters, t.price_per_liter, t.odo,
  'INA', 'R-'||t.vehicle_id||'-'||to_char(t.posted_at,'YYYYMMDDHH24MI')
from tx t
where not exists (
  select 1 from fuel_transaction f
  where f.vehicle_id = t.vehicle_id and f.receipt_number = ('R-'||t.vehicle_id||'-'||to_char(t.posted_at,'YYYYMMDDHH24MI'))
);

-- -------------------------------------------------
-- Vendori 
-- -------------------------------------------------
with x(n,a,p) as (
  values ('AutoServis d.o.o.','Av. Servisa 1','0123456'),
         ('GumiCentar d.o.o.','Ulica Guma 2','0654321')
)
insert into vendor(name,address,phone,active)
select n,a,p,true from x
where not exists (select 1 from vendor v where v.name = x.n);

-- -------------------------------------------------
-- Nalozi održavanja + stavke
-- -------------------------------------------------
-- Odaberi 6 vozila
with v6 as (
  select vehicle_id, row_number() over(order by vehicle_id) rn
  from vehicle order by vehicle_id limit 6
),
ven as (select vendor_id from vendor order by vendor_id limit 2),
mo_src as (
  select v.vehicle_id,
         (select vendor_id from ven order by vendor_id limit 1 offset ((v.rn-1) % 2)) as vendor_id,
         case when v.rn <= 4 then 'closed' else 'in_progress' end as status,
         now() - interval '10 days' as opened_at,
         'Redovni servis'::text as description
  from v6 v
)
-- Kreiraj nalog ako ne postoji isti 
insert into maintenance_order(vehicle_id, vendor_id, opened_at, status, description)
select s.vehicle_id, s.vendor_id, s.opened_at, s.status, s.description
from mo_src s
where not exists (
  select 1 from maintenance_order m
  where m.vehicle_id = s.vehicle_id and m.opened_at::date = s.opened_at::date
);

-- Stavke 
with closed_mo as (
  select m.mo_id
  from maintenance_order m
  where m.status = 'closed'
)
insert into maintenance_item(mo_id, work_type, parts_cost, labor_cost, description)
select mo_id, 'Oil&Filters', 80.00, 120.00, 'Zamjena ulja i filtera'
from closed_mo cm
where not exists (
  select 1 from maintenance_item i where i.mo_id = cm.mo_id
);

update maintenance_order
set closed_at = now()
where status = 'closed' and closed_at is null;

-- -------------------------------------------------
-- Police osiguranja
-- -------------------------------------------------
with v2 as (
  select vehicle_id, row_number() over(order by vehicle_id) rn
  from vehicle order by vehicle_id limit 2
),
pol as (
  select v.vehicle_id,
         'Croatia'::text as provider,
         ('POL-'||v.vehicle_id||'-A')::text as policy_no,
         (date_trunc('year', now()) - interval '1 year')::timestamptz as valid_from,
         (date_trunc('year', now()) - interval '1 day')::timestamptz     as valid_to,
         420.00::numeric as premium_amount,
         'Puno pokriće'::text as coverage_notes
  from v2 v
  union all
  select v.vehicle_id,
         'Croatia',
         ('POL-'||v.vehicle_id||'-B'),
         date_trunc('year', now())::timestamptz,
         (date_trunc('year', now()) + interval '1 year' - interval '1 day')::timestamptz,
         480.00,
         'Puno pokriće'
  from v2 v
)
insert into insurance_policy(vehicle_id, provider, policy_no, valid_from, valid_to, premium_amount, coverage_notes)
select p.*
from pol p
where not exists (
  select 1 from insurance_policy i
  where i.vehicle_id = p.vehicle_id and i.provider = p.provider and i.policy_no = p.policy_no
);

-- -------------------------------------------------
-- Registracije
-- -------------------------------------------------
with v2 as (
  select vehicle_id from vehicle order by vehicle_id limit 2
),
reg as (
  select v.vehicle_id,
         (date_trunc('year', now()) - interval '1 year')::timestamptz as valid_from,
         (date_trunc('year', now()) - interval '1 day')::timestamptz   as valid_to,
         90.00::numeric as fee_amount
  from v2 v
  union all
  select v.vehicle_id,
         date_trunc('year', now())::timestamptz,
         (now() + interval '10 days')::timestamptz,   -- ovo će pasti u v_expiring_30days
         95.00
  from v2 v
)
insert into registration_record(vehicle_id, valid_from, valid_to, fee_amount)
select r.*
from reg r
where not exists (
  select 1 from registration_record x
  where x.vehicle_id = r.vehicle_id
    and x.valid_from = r.valid_from
    and x.valid_to   = r.valid_to
);

set search_path to fleet, public;

-- =========================
-- Brze sanity provjere
-- =========================
select 'vehicle' src, count(*) cnt from vehicle
union all select 'driver', count(*) from driver
union all select 'assignment(active)', count(*) from vehicle_assignment where end_at is null
union all select 'fuel_tx', count(*) from fuel_transaction
union all select 'maintenance_order', count(*) from maintenance_order
union all select 'maintenance_item', count(*) from maintenance_item
union all select 'insurance_policy', count(*) from insurance_policy
union all select 'registration_record', count(*) from registration_record
order by 1;

-- =========================================
-- Trošak po kilometru
-- =========================================
select 
  v.vehicle_id,
  vh.registration_number,
  v.distance_km,
  v.total_fuel_cost,
  v.total_maint_cost,
  v.total_cost,
  v.cost_per_km
from v_cost_per_km_overall v
join vehicle vh using (vehicle_id)
order by vehicle_id;

-- =====================================================
-- Potrošnja po mjesecu 
-- =====================================================
select 
  m.vehicle_id,
  vh.registration_number,
  m.month,
  m.liters_total,
  m.fuel_cost_total,
  m.distance_km,
  m.l_per_100km
from v_monthly_fuel_stats m
join vehicle vh using (vehicle_id)
order by m.month desc, m.vehicle_id
limit 50;

-- ===================================
-- Aktivne dodjele 
-- ===================================
select 
  a.assignment_id,
  vh.registration_number,
  a.vehicle_id,
  a.driver_id,
  (e.first_name || ' ' || e.last_name) as driver_name,
  a.start_at,
  a.end_at,
  a.notes
from v_active_assignments a
join vehicle vh on vh.vehicle_id = a.vehicle_id
join employee e on e.employee_id = a.driver_id
order by a.assignment_id;

-- =========================================
-- Uskoro planirani servisi
-- =========================================
select
  psu.plan_id,
  psu.vehicle_id,
  vh.registration_number,
  psu.description,
  psu.due_at,
  psu.due_odometer,
  psu.status
from fleet.v_planned_service_upcoming psu
join fleet.vehicle vh on vh.vehicle_id = psu.vehicle_id
order by psu.due_at nulls last, psu.due_odometer nulls last, psu.vehicle_id
limit 50;


-- ==========================================================
-- Servisni nalozi + stavke
-- ==========================================================
-- Zaglavlja naloga
select 
  m.mo_id, m.vehicle_id, vh.registration_number,
  m.status, m.opened_at, m.started_at, m.closed_at, m.total_cost
from maintenance_order m
join vehicle vh using (vehicle_id)
order by m.mo_id;

-- Stavke + Line_total 
select 
  i.item_id, i.mo_id, i.work_type, i.parts_cost, i.labor_cost, i.line_total
from maintenance_item i
order by i.item_id;

-- Grupirani ukupni trošak po nalogu
select 
  i.mo_id,
  sum(i.line_total) as items_sum,
  (select m.total_cost from maintenance_order m where m.mo_id = i.mo_id) as header_total
from maintenance_item i
group by i.mo_id
order by i.mo_id;

-- =========================================
-- Dodatne kontrole kvalitete
-- =========================================

set search_path to fleet, public;

-- Sumnjiva točenja 
select * 
from fuel_transaction
where is_suspicious = true
order by posted_at;

-- Monotoni porast km 
select *
from v_odo_speed_anomalies
where is_anomalous = true
order by reading_id;

-- Planned service “uskoro”
select * from v_planned_service_upcoming order by due_at;

-- Uzmi jedan tx
select fuel_tx_id, liters, odometer_km, posted_at, is_suspicious
from fleet.fuel_transaction
order by posted_at
limit 1;

-- Pojačaj litre da ispadne suspicious
update fleet.fuel_transaction
set liters = liters * 4
where fuel_tx_id = 1;

select *
from fleet.fuel_transaction
where is_suspicious = true
order by posted_at;

