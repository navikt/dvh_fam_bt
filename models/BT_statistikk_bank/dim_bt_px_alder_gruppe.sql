{{
    config(
        materialized='view',
        tags="Dimensjon_bank"
    )
}}

with gruppe as (
    select
        -1 as alder_fra_og_med
       ,-1 as alder_til_og_med
       ,'I alt' as alder_gruppe_besk
       ,'I alt' as utvidet_alder_gruppe_besk
       ,2 as alder_gruppe_nivaa
       ,0 as sortering
    from dual

    union
    select
        0 as alder_fra_og_med
       ,20 as alder_til_og_med
       ,'20 år og yngre' as alder_gruppe_besk
       ,'Yngre enn 30 år' as utvidet_alder_gruppe_besk
       ,1 as alder_gruppe_nivaa
       ,1 as sortering
    from dual

    union
    select
        21 as alder_fra_og_med
       ,29 as alder_til_og_med
       ,'21-29 år' as alder_gruppe_besk
       ,'Yngre enn 30 år' as utvidet_alder_gruppe_besk
       ,1 as alder_gruppe_nivaa
       ,2 as sortering
    from dual

    union
    select
        30 as alder_fra_og_med
       ,39 as alder_til_og_med
       ,'30-39 år' as alder_gruppe_besk
       ,'30-39 år' as utvidet_alder_gruppe_besk
       ,1 as alder_gruppe_nivaa
       ,3 as sortering
    from dual

    union
    select
        40 as alder_fra_og_med
       ,49 as alder_til_og_med
       ,'40-49 år' as alder_gruppe_besk
       ,'40-49 år' as utvidet_alder_gruppe_besk
       ,1 as alder_gruppe_nivaa
       ,4 as sortering
    from dual

    union
    select
        50 as alder_fra_og_med
       ,59 as alder_til_og_med
       ,'50-59 år' as alder_gruppe_besk
       ,'50-59 år' as utvidet_alder_gruppe_besk
       ,1 as alder_gruppe_nivaa
       ,5 as sortering
    from dual

    union
    select
        60 as alder_fra_og_med
       ,200 as alder_til_og_med
       ,'60 år+' as alder_gruppe_besk
       ,'60 år+' as utvidet_alder_gruppe_besk
       ,1 as alder_gruppe_nivaa
       ,6 as sortering
    from dual
)

select
    alder_gruppe_besk
   ,utvidet_alder_gruppe_besk
   ,alder_fra_og_med
   ,alder_til_og_med
   ,alder_gruppe_nivaa
   ,sortering
from gruppe
order by alder_fra_og_med