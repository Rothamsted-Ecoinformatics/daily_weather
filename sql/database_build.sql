DROP TABLE forecast_data.day10_forecast_model_values CASCADE;

DROP TABLE forecast_data.day10_forecasts CASCADE;

DROP TABLE forecast_data.day10_forecast_variable_summaries CASCADE;

DROP TABLE forecast_data.day10_model_runs CASCADE;

DROP TABLE forecast_data.day28_forecast_model_values CASCADE;

DROP TABLE forecast_data.day28_forecasts CASCADE;

DROP TABLE forecast_data.day28_forecast_variable_summaries CASCADE;

DROP TABLE forecast_data.day28_model_runs CASCADE;

CREATE TABLE day10_model_runs (
	id serial4 NOT NULL,
	node_number smallint NULL,
	last_updated timestamp NOT NULL,
	model_run date NOT NULL,
	forecast_site text NOT NULL,
	source_file text NOT NULL,
	CONSTRAINT pk_day10_model_runs PRIMARY KEY (id)
);

CREATE TABLE day10_forecasts (
	id serial4 NOT NULL,
	model_run_id smallint NOT NULL,
	forecast_date date NOT NULL,
	forecast_day smallint NOT NULL,
	forecast_hour smallint NOT NULL,
	CONSTRAINT pk_day10_forecasts PRIMARY KEY (id),
	CONSTRAINT day10_forecasts_model_run_id_fk FOREIGN KEY (model_run_id) REFERENCES day10_model_runs(id)
);

CREATE TABLE day10_forecast_variable_summaries (
	id serial4 NOT NULL,
	forecast_id int4 NOT NULL,
	variable text not null,
	mean_value numeric null,
	max_value numeric null,
	min_value numeric null,
	nof_members smallint null,
	units text null,
	orig_member_value text null,
	CONSTRAINT pk_day10_forecast_variable_summaries PRIMARY KEY (id),
	CONSTRAINT day10_forecasts_forecast_id_fk FOREIGN KEY (forecast_id) REFERENCES day10_forecasts(id)
);

CREATE TABLE day10_forecast_model_values (
	id serial4 NOT NULL,
	forecast_id int4 NOT NULL,
	variable text NOT NULL,
	value numeric NULL,
	model_number smallint NOT NULL,
	CONSTRAINT pk_day10_forecast_model_values PRIMARY KEY (id),
	CONSTRAINT day10_forecast_model_values_forecasts_id_fk FOREIGN KEY (forecast_id) REFERENCES day10_forecasts(id)
);

CREATE TABLE day28_model_runs (
	id serial4 NOT NULL,
	node_number smallint NULL,
	last_updated timestamp NOT NULL,
	model text NULL,
	forecast_site text NOT NULL,
	source_file text NOT NULL,
	CONSTRAINT pk_day28_model_runs PRIMARY KEY (id)
);

CREATE TABLE day28_forecasts (
	id serial4 NOT NULL,
	model_run_id int4 NOT NULL,
	forecast_date date NOT NULL,
	forecast_day smallint NOT NULL,
	forecast_hour smallint NOT NULL,
	CONSTRAINT pk_day28_forecasts PRIMARY KEY (id),
	CONSTRAINT day28_forecasts_model_run_id_fk FOREIGN KEY (model_run_id) REFERENCES day28_model_runs(id)
);

CREATE TABLE day28_forecast_variable_summaries (
	id serial4 NOT NULL,
	forecast_id int4 NOT NULL,
	variable text not null,
	mean_value numeric null,
	max_value numeric null,
	min_value numeric null,
	nof_members smallint null,
	units text null,
	orig_member_value text null,
	CONSTRAINT pk_day28_forecast_variable_summaries PRIMARY KEY (id),
	CONSTRAINT day28_forecasts_forecast_id_fk FOREIGN KEY (forecast_id) REFERENCES day28_forecasts(id)
);

CREATE TABLE day28_forecast_model_values (
	id serial4 NOT NULL,
	forecast_id int4 NOT NULL,
	variable text NOT NULL,
	value numeric NULL,
	model_number smallint NOT NULL,
	CONSTRAINT pk_day28_forecast_model_values PRIMARY KEY (id),
	CONSTRAINT day28_forecast_model_values_forecasts_id_fk FOREIGN KEY (forecast_id) REFERENCES day28_forecasts(id)
);