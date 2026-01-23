{{
    config(
        materialized='table'
    )
}}

with mottaker as (
    select *
    from {{ ref('fak_bt_px_mottaker') }}
    where belop_utvidet > 0 --Etterbetalt telles ikke
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

--Gruppere på nivå aar_kvartal, kjønn og aldersgruppe.
agg as (
    select
        full_liste.aar_kvartal
       ,full_liste.kjonn_besk
       ,full_liste.kjonn_nivaa
       ,full_liste.sortering_kjonn
       ,full_liste.alder_gruppe_besk
       ,full_liste.sortering_alder
       --,count(distinct fk_person1) as antall
       ,case when count(distinct fk_person1) < 10 then
                  round(count(distinct fk_person1)+5, -1) --Prikking: Round antall mindre enn 5 oppover til nærmeste tier
             else count(distinct fk_person1)
        end antall
    from full_liste

    left outer join mottaker
    on full_liste.aar_kvartal = mottaker.aar_kvartal
    and full_liste.kjonn_besk = mottaker.kjonn_besk
    and mottaker.alder between full_liste.alder_fra_og_med and full_liste.alder_til_og_med

    where full_liste.kjonn_besk != 'I alt'
    and full_liste.alder_gruppe_besk != 'I alt'

    group by
        full_liste.aar_kvartal
       ,full_liste.kjonn_besk
       ,full_liste.kjonn_nivaa
       ,full_liste.sortering_kjonn
       ,full_liste.alder_gruppe_besk
       ,full_liste.sortering_alder
)
,

--kjonn_besk = 'I alt'
kjonn_i_alt as (
    select
        full_liste.aar_kvartal
       ,full_liste.kjonn_besk
       ,full_liste.kjonn_nivaa
       ,full_liste.sortering_kjonn
       ,full_liste.alder_gruppe_besk
       ,full_liste.sortering_alder
       ,sum(agg.antall) as antall
    from full_liste

    left outer join agg
    on full_liste.aar_kvartal = agg.aar_kvartal
    and full_liste.alder_gruppe_besk = agg.alder_gruppe_besk

    where full_liste.kjonn_besk = 'I alt'
    and full_liste.alder_gruppe_besk != 'I alt'

    group by
        full_liste.aar_kvartal
       ,full_liste.kjonn_besk
       ,full_liste.kjonn_nivaa
       ,full_liste.sortering_kjonn
       ,full_liste.alder_gruppe_besk
       ,full_liste.sortering_alder
)
,

--alder_gruppe_besk = 'I alt'
aldersgruppe_i_alt as (
    select
        full_liste.aar_kvartal
       ,full_liste.kjonn_besk
       ,full_liste.kjonn_nivaa
       ,full_liste.sortering_kjonn
       ,full_liste.alder_gruppe_besk
       ,full_liste.sortering_alder
       ,sum(agg.antall) as antall
    from full_liste

    left outer join agg
    on full_liste.aar_kvartal = agg.aar_kvartal
    and full_liste.kjonn_besk = agg.kjonn_besk

    where full_liste.alder_gruppe_besk = 'I alt'
    and full_liste.kjonn_besk != 'I alt'

    group by
        full_liste.aar_kvartal
       ,full_liste.kjonn_besk
       ,full_liste.kjonn_nivaa
       ,full_liste.sortering_kjonn
       ,full_liste.alder_gruppe_besk
       ,full_liste.sortering_alder
)
,

--kjonn_besk = 'I alt' og alder_gruppe_besk = 'I alt'
i_alt as (
    select
        full_liste.aar_kvartal
       ,full_liste.kjonn_besk
       ,full_liste.kjonn_nivaa
       ,full_liste.sortering_kjonn
       ,full_liste.alder_gruppe_besk
       ,full_liste.sortering_alder
       ,sum(agg.antall) as antall
    from full_liste

    left outer join agg
    on full_liste.aar_kvartal = agg.aar_kvartal

    where full_liste.alder_gruppe_besk = 'I alt'
    and full_liste.kjonn_besk = 'I alt'

    group by
        full_liste.aar_kvartal
       ,full_liste.kjonn_besk
       ,full_liste.kjonn_nivaa
       ,full_liste.sortering_kjonn
       ,full_liste.alder_gruppe_besk
       ,full_liste.sortering_alder
)
,

alle as (
    select
        agg.aar_kvartal
       ,agg.kjonn_besk
       ,agg.kjonn_nivaa
       ,agg.sortering_kjonn
       ,agg.alder_gruppe_besk
       ,agg.sortering_alder
       ,agg.antall
       ,case when aldersgruppe_i_alt.antall != 0 then round(agg.antall/aldersgruppe_i_alt.antall*100,1) else 0 end prosent
    from agg
    join aldersgruppe_i_alt
    on agg.aar_kvartal = aldersgruppe_i_alt.aar_kvartal
    and agg.kjonn_besk = aldersgruppe_i_alt.kjonn_besk

    union all
    select
        kjonn_i_alt.aar_kvartal
       ,kjonn_i_alt.kjonn_besk
       ,kjonn_i_alt.kjonn_nivaa
       ,kjonn_i_alt.sortering_kjonn
       ,kjonn_i_alt.alder_gruppe_besk
       ,kjonn_i_alt.sortering_alder
       ,kjonn_i_alt.antall
       ,case when i_alt.antall != 0 then round(kjonn_i_alt.antall/i_alt.antall*100,1) else 0 end prosent
    from kjonn_i_alt
    join i_alt
    on kjonn_i_alt.aar_kvartal = i_alt.aar_kvartal

    union all
    select
        aar_kvartal
       ,kjonn_besk
       ,kjonn_nivaa
       ,sortering_kjonn
       ,alder_gruppe_besk
       ,sortering_alder
       ,antall
       ,100 as prosent
    from aldersgruppe_i_alt

    union all
    select
        aar_kvartal
       ,kjonn_besk
       ,kjonn_nivaa
       ,sortering_kjonn
       ,alder_gruppe_besk
       ,sortering_alder
       ,antall
       ,100 as prosent
    from i_alt
)
select
    substr(aar_kvartal,1,4)||'K'||substr(aar_kvartal,6,1) as aar_kvartal
   ,kjonn_besk as kjonn
   ,alder_gruppe_besk as aldersgruppe
   ,antall
   ,prosent
   ,sortering_kjonn||sortering_alder as sortering
   ,localtimestamp as lastet_dato
from alle
order by
    aar_kvartal
   ,kjonn_besk
   ,alder_gruppe_besk
   ,antall
   ,prosent
   ,sortering_kjonn
   ,sortering_alder