drop table forecast_data.forecast_model_values;
drop table forecast_data.forecasts;
drop table forecast_data.model_runs;

create table forecast_data.model_runs (
    id serial not null,
    node_number int not null,
    model_run_date date not null,
    model_type int not null,
    model_run_day_metadata_actual text null,
    model_run_day_inferred text not null,
    forecast_site text not null,
    source_file text not null,
    constraint model_runs_pk primary key(id) 
); 

comment on column forecast_data.model_runs.model_run_date is 'The metadata last updated value';
comment on column forecast_data.model_runs.model_run_day_metadata_actual is 'The metadata Model value. In some cases this is blank';
comment on column forecast_data.model_runs.model_run_day_inferred is 'The metadata Model value inferred from the metadata last updated value. This may be different to the actual day value.';

create table forecast_data.forecasts (
    id serial not null,
    model_run_id integer not null,
    forecast_date date not null,
    forecast_day text not null,
    day_period text not null,
    forecast_hour text not null,
    to_include integer null,
    constraint forecasts_pk primary key(id),
    constraint forecasts_model_run_id_fk foreign key (model_run_id) references model_runs(id)
);

create table forecast_data.forecast_model_values (
	id serial not null,
	forecast_id int not null,
	variable text not null,
	value numeric null,
	model integer not null,
	constraint forecast_model_values_pk primary key(id),
    constraint forecast_model_values_forecasts_id_fk foreign key (forecast_id) references forecasts(id)
);




--select id from forecast_data.model_runs order by id desc limit 1;