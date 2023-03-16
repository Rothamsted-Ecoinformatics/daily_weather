-- Returns the 10 day forecast daily min, max and avg for both sites. 
-- Min is calculated as the lowest night time temperature 
-- Max is calculated as the highest daytime temperature
-- Avg is calculated as the mean of the min and max.
-- Also included are tests for day max being < night max and day min being < night min

select mn.site, mn.model_run_date, mn.forecast_day, mn.model_type, mn.min_temp, mx.max_temp, (mn.min_temp+mx.max_temp)/2 as avg_temp,
iif (min_day_temp < min_temp, 'Y', 'N') as is_min_day_lt_min_night, min_day_temp,
iif (max_night_temp > max_temp, 'Y', 'N') as is_max_night_gt_max_day, max_night_temp 
from (
	select site, model_run_date, forecast_day, model_type, min(value) as min_temp, max(value) as max_night_temp
	from model_runs mr
	inner join forecasts f on mr.id = f.model_run_id
	inner join forecast_model_values fmv on f.id = fmv.forecast_id
	where variable = 'mintemp'
	--and model_run_date = '2022-01-13'
	and include = 1
	and day_period = 'N'
	and model_type = '10' 
	group by site, model_run_date, forecast_day, model_type
) mn inner join (
	select site, model_run_date, forecast_day, model_type, max(value) as max_temp, min(value) as min_day_temp
	from model_runs mr
	inner join forecasts f on mr.id = f.model_run_id
	inner join forecast_model_values fmv on f.id = fmv.forecast_id
	where variable = 'maxtemp'
	--and model_run_date = '2022-01-13'
	and include = 1
	and day_period = 'D'
	and model_type = '10'
	group by site, model_run_date, forecast_day, model_type
) mx on mn.site = mx.site and mn.model_run_date = mx.model_run_date and mn.forecast_day = mx.forecast_day
order by mn.site, mn.forecast_day, mn.model_run_date, mn.model_type;
