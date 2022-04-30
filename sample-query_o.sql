select * 
from one_source_table 
where somefield='&&B_VALUE' and 
    a_timestamp between to_date("&&DT_INI", 'yyyymmdd') and to_date("&&DT_FIN", 'yyyymmdd')