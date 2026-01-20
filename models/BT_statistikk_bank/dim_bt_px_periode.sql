{{
    config(
        materialized='view'
    )
}}

select aar, aar_kvartal, kvartal, kvartal_besk, forste_dato_i_perioden, siste_dato_i_perioden
from {{ source('bt_statistikk_bank_dt_kodeverk', 'dim_tid') }}
where gyldig_flagg = 1
and dim_nivaa = 4 --Kvartal nivå
and aar between to_char(sysdate, 'yyyy') - 10 and to_char(sysdate, 'yyyy') --Hent ut siste 10 år
and siste_dato_i_perioden < trunc(sysdate, 'dd') --Ikke produsere kvartal frem i tid
order by aar_kvartal desc