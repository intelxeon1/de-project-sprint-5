CREATE TABLE stg.deliverysystem_restaurants (
	id serial,
	object_value json NULL,
	CONSTRAINT deliversystem_restaurants_pkey PRIMARY KEY (id)
);

CREATE TABLE stg.deliverysystem_deliveries (
	id serial,
	object_value json NULL,
	CONSTRAINT deliversystem_deliveries_pkey PRIMARY KEY (id)
);

CREATE TABLE stg.deliverysystem_couriers (
	id serial,
	object_value json NULL,
	CONSTRAINT deliversystem_couriers_pkey PRIMARY KEY (id)
);

CREATE SCHEMA ods AUTHORIZATION jovyan;

CREATE TABLE ods.deliverysystem_restaurants (
	id serial,
	object_id varchar(24) not NULL,
	"name" text NOT NULL,
	updated_ts timestamp NOT NULL,
	CONSTRAINT deliversystem_restaurants_pkey PRIMARY KEY (id),
	CONSTRAINT deliversystem_restaurants_restaurant_id_un UNIQUE (object_id)
);

CREATE TABLE ods.deliverysystem_deliveries (
	id serial,
	order_id varchar(24) NOT NULL,
	order_ts timestamp NOT NULL,
	delivery_id varchar(24) NOT NULL,
	courier_id varchar(24) NOT NULL,
	address text NOT NULL,
	delivery_ts timestamp NOT NULL,
	rate int4 NOT NULL,
	sum numeric(17, 2) NOT NULL,
	tip_sum numeric(17, 2) NOT NULL,
	updated_ts timestamp NOT NULL,
	CONSTRAINT deliversystem_deliveries_delivery_id_unk UNIQUE (delivery_id),
	CONSTRAINT deliversystem_deliveries_pkey PRIMARY KEY (id)
);

CREATE TABLE ods.deliverysystem_couriers (
	id serial,
	object_id varchar(24) not NULL,
	"name" text not NULL,
	updated_ts timestamp not NULL,
	CONSTRAINT deliversystem_couriers_object_id_un UNIQUE (object_id),
	CONSTRAINT deliversystem_couriers_pkey PRIMARY KEY (id)
);

drop table if exists cdm.dm_courier_ledger ;
create table cdm.dm_courier_ledger (
	id serial primary key,
	courier_id int4 references dds.dm_couriers(id),
	courier_name text,
	settlement_year int4,
	settlement_month int4,
	orders_count int4,
	order_total_sum numeric(14,2),
	rate_avg numeric,
	order_processing_fee numeric(14,2),
	courier_order_sum numeric(14,2),
	courier_tips_sum numeric(14,2),
	courier_reward_sum numeric(14,2),	
	
	CONSTRAINT dm_courier_ledger_un_check UNIQUE (courier_id, settlement_year,settlement_month),
	CONSTRAINT dm_courier_ledger_oc_check CHECK (orders_count >= 0),
	CONSTRAINT dm_courier_ledger_ots_check CHECK (order_total_sum >= 0),
	CONSTRAINT dm_courier_ledger_ra_check CHECK (rate_avg >= 0 and rate_avg <= 5),
	CONSTRAINT dm_courier_ledger_opf_check CHECK (order_processing_fee >= 0),
	CONSTRAINT dm_courier_ledger_cos_check CHECK (courier_order_sum >= 0),
	CONSTRAINT dm_courier_ledger_cts_check CHECK (courier_tips_sum >= 0),
	CONSTRAINT dm_courier_ledger_crs_check CHECK (courier_reward_sum >= 0),
	CONSTRAINT dm_courier_ledger_sm_check CHECK (settlement_month>=1 and settlement_month<=12),
	CONSTRAINT dm_courier_ledger_sy_check CHECK (settlement_year>=2022 and settlement_year<=2500)
)

CREATE FUNCTION cdm.calc_oder_sum(in_s numeric,rate numeric) RETURNS numeric AS $$
DECLARE
    pay numeric := 0;
   	coeff numeric :=0;
    min_p numeric := 0;
BEGIN
	if rate < 4 then 
		coeff = 0.05;
		min_p = 100;
	elsif rate >= 4 and rate < 4.5 then
		coeff = 0.07;
		min_p = 150;
	elsif rate >= 4.5 and rate < 4.9 then
		coeff = 0.08;
		min_p = 175;	
	elsif rate >= 4.9 then
		coeff = 0.1;
		min_p = 200;	
	end if;
	pay := GREATEST( in_s * coeff,min_p);	
    return pay;
END;
$$ LANGUAGE plpgsql;	
	