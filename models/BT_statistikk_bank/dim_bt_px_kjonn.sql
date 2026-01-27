{{
    config(
        materialized='view',
        tags="Dimensjon_bank"
    )
}}

select
    distinct kjonn_kode, kjonn_flertall_besk as kjonn_besk, kjonn_nr
   ,1 as kjonn_nivaa
   ,case when kjonn_flertall_besk = 'Menn' then 1
         when kjonn_flertall_besk = 'Kvinner' then 2
    end sortering
from {{ source('bt_statistikk_bank_dt_kodeverk', 'dim_kjonn') }}
where kjonn_flertall_besk in ('Kvinner','Menn')

union all
select
    'ALT' as kjonn_kode, 'I alt' as kjonn_besk, -1 as kjonn_nr
   ,2 as kjonn_nivaa
   ,0 as sortering
from dual

order by kjonn_nr