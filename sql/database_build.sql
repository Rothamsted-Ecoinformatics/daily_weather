create table lte_data.factor_level_terms (  
	id smallserial not null,
	preferred_term_id int2 null,
	term_label text not null,
	term_uri text null,
	ontology_id text null,
	constraint factor_level_terms_pk primary key(id),
	constraint factor_level_terms_ontology_id_fk foreign key (ontology_id) references ontologies(id),
	constraint factor_level_terms_preferred_term_id foreign key (preferred_term_id) references factor_level_terms(id),
	constraint factor_level_terms_term_uri_unq unique (term_uri)
);


create table forecast_data.model_runs (
    id serial not null,
    node_number int not null,
    model_run_date date not null,
    model_type int not null,
    model_run_day text not null,
    forecast_site text not null,
    constraint model_runs_pk primary key(id) 
); 

create table forecast_data.forecasts (
    id serial not null,
    model_run_id integer not null,
    forecast_date text not null,
    forecast_day text not null,
    day_period text not null,
    forecast_hour text not null,
    to_include integer null,
    constraint forecasts_pk primary key(id),
    constraint forecasts_model_run_id_fk foreign key (model_run_id) references model_runs(id)
);

create table forecast_model_values (
	id serial not null,
	forecast_id int not null,
	variable text not null,
	value numeric null,
	model integer not null,
	constraint forecast_model_values_pk primary key(id),
    constraint forecast_model_values_forecasts_id_fk foreign key (forecast_id) references forecasts(id)
);