set search_path to fleet, public;

-- Šifrarnici 
with
mk as (
  insert into dc_vehicle_make(name) values ('Volkswagen')
  on conflict (name) do nothing
  returning make_id
),
mk_id as (
  select make_id from mk
  union all
  select make_id from dc_vehicle_make where name='Volkswagen'
),
mdl as (
  insert into dc_vehicle_model(make_id, name)
  select (select make_id from mk_id), 'Golf'
  on conflict (make_id, name) do nothing
  returning model_id
),
mdl_id as (
  select model_id from mdl
  union all
  select model_id from dc_vehicle_model where make_id=(select make_id from mk_id) and name='Golf'
),
cat as (
  insert into dc_vehicle_category(name) values ('Passenger')
  on conflict (name) do nothing
  returning category_id
),
cat_id as (
  select category_id from cat
  union all
  select category_id from dc_vehicle_category where name='Passenger'
),
ft as (
  insert into dc_fuel_type(code, label) values ('DIESEL','Diesel')
  on conflict (code) do nothing
  returning fuel_type_id
),
ft_id as (
  select fuel_type_id from ft
  union all
  select fuel_type_id from dc_fuel_type where code='DIESEL'
)
select 1;

-- Zaposlenik + Vozač (važeća dozvola)
with
e as (
  insert into employee(first_name,last_name,email,phone)
  values ('Ivan','Ivić','ivan.ivic@example.com','091111222')
  returning employee_id
),
eid as (
  select employee_id from e
  union all
  select employee_id from employee where email='ivan.ivic@example.com'
)
insert into driver(driver_id, license_no, license_issue_at, license_expire_at)
select employee_id, 'B-123456', (now() - interval '5 years')::date, (now() + interval '2 years')::date
from eid
on conflict (driver_id) do update
set license_no = excluded.license_no;

-- Vozilo ZG-1234-AB (diesel, active)
with
mdl as (select model_id from dc_vehicle_model m
        join dc_vehicle_make mk on mk.make_id=m.make_id
        where mk.name='Volkswagen' and m.name='Golf'),
veh as (
  insert into vehicle(registration_number, vin, model_id, category_id, fuel_type_id, year, status, current_odometer_km)
  values ('ZG-1234-AB', 'VIN-ZG-0001', (select model_id from mdl),
          (select category_id from dc_vehicle_category where name='Passenger'),
          (select fuel_type_id from dc_fuel_type where code='DIESEL'),
          2018, 'active', 120000)
  on conflict (registration_number) do update
    set model_id = excluded.model_id,
        fuel_type_id = excluded.fuel_type_id
  returning vehicle_id
)
select 1;

--  Aktivna dodjela vozaču 
do $$
declare v_vehicle bigint; v_driver bigint;
begin
  select vehicle_id into v_vehicle from vehicle where registration_number='ZG-1234-AB';
  select driver_id  into v_driver  from driver  d
    join employee e on e.employee_id=d.driver_id
   where e.email='ivan.ivic@example.com';

  -- Zatvori eventualnu staru aktivnu
  update vehicle_assignment
     set end_at = now() - interval '15 minutes'
   where vehicle_id=v_vehicle and end_at is null;

  insert into vehicle_assignment(vehicle_id, driver_id, start_at, notes)
  values (v_vehicle, v_driver, now() - interval '14 days', 'Aktivna dodjela');
end$$;

--  Očitanja kilometara 
do $$
declare v_vehicle bigint;
begin
  select vehicle_id into v_vehicle from vehicle where registration_number='ZG-1234-AB';

  -- Bazno očitanje
  insert into odometer_reading(vehicle_id, reading_at, odometer_km)
  values
    (v_vehicle, now() - interval '14 days', 120000.0)
  on conflict do nothing;

  -- Sljedeće očitanje
  insert into odometer_reading(vehicle_id, reading_at, odometer_km)
  values
    (v_vehicle, now() - interval '10 days', 120300.0)
  on conflict do nothing;

  -- Zadnje očitanje
  insert into odometer_reading(vehicle_id, reading_at, odometer_km)
  values
    (v_vehicle, now() - interval '2 days', 120800.0)
  on conflict do nothing;
end$$;

-- Točenja goriva
do $$
declare v_vehicle bigint; v_driver bigint;
begin
  select vehicle_id into v_vehicle from vehicle where registration_number='ZG-1234-AB';
  select driver_id  into v_driver from driver d join employee e on e.employee_id=d.driver_id
   where e.email='ivan.ivic@example.com';

  -- Normalno #1: 100 km, 7 L
  insert into fuel_transaction(vehicle_id, driver_id, posted_at, liters, price_per_liter, odometer_km, station_name, receipt_number)
  values (v_vehicle, v_driver, now() - interval '13 days', 7.0, 1.60, 120100.0, 'INA', 'R-001');

  -- Normalno #2: 200 km, 14 L
  insert into fuel_transaction(vehicle_id, driver_id, posted_at, liters, price_per_liter, odometer_km, station_name, receipt_number)
  values (v_vehicle, v_driver, now() - interval '9 days', 14.0, 1.60, 120300.0, 'INA', 'R-002');

  -- Ekstremno: 500 km, 60 L
  insert into fuel_transaction(vehicle_id, driver_id, posted_at, liters, price_per_liter, odometer_km, station_name, receipt_number)
  values (v_vehicle, v_driver, now() - interval '1 days', 60.0, 1.60, 120800.0, 'INA', 'R-003');
end$$;

--  Nalog održavanja: open → in_progress → stavka → closed 
do $$
declare v_vehicle bigint; v_vendor bigint; v_mo bigint;
begin
  select vehicle_id into v_vehicle from vehicle where registration_number='ZG-1234-AB';

  insert into fleet.vendor(name, active)
  select 'AutoServis d.o.o.', true
  where not exists (
  select 1 from fleet.vendor where name = 'AutoServis d.o.o.'
  );


  select vendor_id into v_vendor from vendor where name='AutoServis d.o.o.';

  -- Open
  insert into maintenance_order(vehicle_id, vendor_id, opened_at, status, description)
  values (v_vehicle, v_vendor, now() - interval '10 days', 'open', 'Redovni servis')
  returning mo_id into v_mo;

  -- In_progress
  update maintenance_order
     set status='in_progress', started_at=now() - interval '9 days'
   where mo_id=v_mo;

  -- Stavka 
  insert into maintenance_item(mo_id, work_type, parts_cost, labor_cost, description)
  values (v_mo, 'Oil&Filters', 80.00, 120.00, 'Zamjena ulja i filtera');

  -- Closed
  update maintenance_order
     set status='closed', closed_at=now() - interval '8 days'
   where mo_id=v_mo;
end$$;

-- Polica osiguranja: bez preklapanja, produženje = novi zapis (tekuća godina)
merge into insurance_policy t
using (
  select
    (select vehicle_id from vehicle where registration_number='ZG-1234-AB') as vehicle_id,
    'Croatia'                                            as provider,
    'POL-ZG-001'                                         as policy_no,
    date_trunc('year', now())::date                      as valid_from,
    (date_trunc('year', now())::date + interval '1 year' - interval '1 day')::timestamptz as valid_to,
    480.00                                               as premium_amount,
    'Puno pokriće'                                       as coverage_notes
) s
on (t.vehicle_id=s.vehicle_id and t.provider=s.provider and t.policy_no=s.policy_no)
when not matched then
  insert (vehicle_id, provider, policy_no, valid_from, valid_to, premium_amount, coverage_notes)
  values (s.vehicle_id, s.provider, s.policy_no, s.valid_from, s.valid_to, s.premium_amount, s.coverage_notes)
when matched then
  update set premium_amount = s.premium_amount,
             coverage_notes = s.coverage_notes;


set search_path to fleet, public;

-- Servisi uskoro
insert into fleet.planned_service (vehicle_id, due_at, description)
select vehicle_id, now() + interval '14 days', 'Mali servis'
from fleet.vehicle
limit 1;

select *
from fleet.v_planned_service_upcoming
order by due_at nulls last, due_odometer nulls last, vehicle_id
limit 50;

-- Aktivne dodjele
select * from v_active_assignments order by start_at desc;

-- Potrošnja / mjesec 
select * from v_monthly_fuel_stats order by month;

-- Trošak po km
select * from v_cost_per_km_overall;
