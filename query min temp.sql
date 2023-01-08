select forecast_day, day_period, min(value) as min_min_temp, max(value) as max_min_temp  
from model_runs mr
inner join forecasts f on mr.id = f.model_run_id
inner join forecast_model_values fmv on f.id = fmv.forecast_id
where model_run_date = '2022-01-13'
and variable = 'mintemp'
and include = 1
group by forecast_day, day_period