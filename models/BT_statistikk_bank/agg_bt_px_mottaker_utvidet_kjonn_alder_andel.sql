{{
    config(
        materialized='table'
    )
}}

with utvidet as (
    select *
    from {{ ref('agg_bt_px_mottaker_utvidet_kjonn_alder') }}
)
,

--Returnere en full liste med alle kjønn, aldersgrupper og alle perioder. Dette er PX format spesifikk.
full_liste as (
    select
        kjonn.kjonn_besk
       ,kjonn.kjonn_nivaa
       ,kjonn.sortering as sortering_kjonn
       ,alder_gruppe.alder_fra_og_med
       ,alder_gruppe.alder_til_og_med
       ,alder_gruppe.alder_gruppe_besk
       ,alder_gruppe.sortering as sortering_alder
       ,periode.aar_kvartal
       ,periode.forste_dato_i_perioden
       ,periode.siste_dato_i_perioden
    from {{ ref('dim_bt_px_kjonn') }} kjonn

    full outer join
    (
        select utvidet_alder_gruppe_besk as alder_gruppe_besk
              ,min(alder_fra_og_med) as alder_fra_og_med
              ,max(alder_til_og_med) as alder_til_og_med
              ,min(sortering) as sortering
        from {{ ref('dim_bt_px_alder_gruppe') }}
        group by utvidet_alder_gruppe_besk
    ) alder_gruppe
    on 1 = 1

    full outer join {{ ref('dim_bt_px_periode') }} periode
    on 1 = 1
)
,

alle as (
    select
        mottaker.aar_kvartal
       ,mottaker.kjonn
       ,full_liste.sortering_kjonn
       ,full_liste.alder_gruppe_besk
       ,full_liste.sortering_alder
       ,sum(mottaker.antall) as antall
    from {{ ref('agg_bt_px_mottaker_kjonn_alder') }} mottaker

    join
    (
        select
            alder_gruppe_besk as alder_gruppe_besk
           ,min(alder_fra_og_med) as alder_fra_og_med
           ,max(alder_til_og_med) as alder_til_og_med
        from {{ ref('dim_bt_px_alder_gruppe') }}
        group by alder_gruppe_besk
    ) alder_gruppe
    on mottaker.aldersgruppe = alder_gruppe.alder_gruppe_besk

    join full_liste
    on full_liste.aar_kvartal = replace(mottaker.aar_kvartal,'K',0) --Erstatte K med 0.
    and full_liste.kjonn_besk = mottaker.kjonn
    and alder_gruppe.alder_fra_og_med >= full_liste.alder_fra_og_med
    and alder_gruppe.alder_til_og_med <= full_liste.alder_til_og_med

    group by
        mottaker.aar_kvartal
       ,mottaker.kjonn
       ,full_liste.sortering_kjonn
       ,full_liste.alder_gruppe_besk
       ,full_liste.sortering_alder
)
,

andel as (
    select
        full_liste.aar_kvartal
       ,full_liste.kjonn_besk
       ,full_liste.sortering_kjonn
       ,full_liste.alder_gruppe_besk
       ,full_liste.sortering_alder
       ,case when alle.antall != 0 then round(utvidet.antall/alle.antall*100,1) else 0 end prosent
    from full_liste

    left outer join alle
    on full_liste.aar_kvartal = replace(alle.aar_kvartal,'K',0) --Erstatte K med 0.
    and full_liste.kjonn_besk = alle.kjonn
    and full_liste.alder_gruppe_besk = alle.alder_gruppe_besk

    left outer join utvidet
    on full_liste.aar_kvartal = replace(utvidet.aar_kvartal,'K',0) --Erstatte K med 0.
    and full_liste.kjonn_besk = utvidet.kjonn
    and full_liste.alder_gruppe_besk = utvidet.aldersgruppe
)
select
    substr(aar_kvartal,1,4)||'K'||substr(aar_kvartal,6,1) as aar_kvartal
   ,kjonn_besk as kjonn
   ,alder_gruppe_besk as aldersgruppe
   ,prosent
   ,sortering_kjonn||sortering_alder as sortering
   ,localtimestamp as lastet_dato
from andel
order by
    aar_kvartal
   ,kjonn_besk
   ,alder_gruppe_besk
   ,prosent
   ,sortering_kjonn
   ,sortering_alder