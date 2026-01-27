{{
    config(
        materialized='view',
        tags="Dimensjon_bank"
    )
}}

select
    distinct
    nåværende_fylke_nr_navn
   ,nåværende_fylke_nr
   ,nåværende_fylkenavn
   ,1 as fylke_nivaa
   ,row_number() over (order by nåværende_fylke_nr) as sortering
from {{ source('bt_statistikk_bank_dt_kodeverk', 'dim_geografi_fylke') }}
where trunc(sysdate, 'dd') between funk_gyldig_fra_dato and funk_gyldig_til_dato
--Ta med fylke står på https://www.nav.no/no/nav-og-samfunn/statistikk/familie-statistikk/tabeller/barnetrygd-september-2025.fylke.antall
and nåværende_fylke_nr not in (21, 22, 23, 97) --Svalbard, Jan Mayen, Kontinentalsokkelen, Svalbard og øvrige områder

union all
select
    '00 I alt' as nåværende_fylke_nr_navn
   ,'00' as nåværende_fylke_nr
   ,'I alt' as nåværende_fylkenavn
   ,2 as fylke_nivaa
   ,0 as sortering
from dual

order by nåværende_fylke_nr