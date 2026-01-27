{{
    config(
        materialized='incremental'
    )
}}

--Hent ut barn for aktuell periode
--Finn ut om barn selv er mottaker
with barn_periode as (
    select
        barn.stat_aarmnd
       ,barn.fkb_person1
       ,barn.fk_person1
       ,periode.aar
       ,periode.aar_kvartal
       ,periode.kvartal
       ,periode.kvartal_besk
       ,max(case when barn.fkb_person1 = barn.fk_person1 then 1 else 0 end) as barn_selv_mottaker_flagg
    from {{ source('bt_statistikk_bank_dvh_fam_bt', 'fak_bt_barn') }} barn

    join {{ ref('dim_bt_px_periode') }} periode
    on barn.stat_aarmnd = to_char(periode.siste_dato_i_perioden, 'yyyymm') --Siste måned i kvartal

    group by
        barn.stat_aarmnd
       ,barn.fkb_person1
       ,barn.fk_person1
       ,periode.aar
       ,periode.aar_kvartal
       ,periode.kvartal
       ,periode.kvartal_besk
)
,
--Hent ut nåværende fylkesnummer til mottaker
--Returnere Kvinne hvis det er flere mottakere for barna (50% på barnetrygd)
barn_navarende_fylke_nr as (
    select *
    from
    (
        select barn.*
            ,mottaker.navarende_fylke_nr
            ,mottaker.belop
            ,row_number() over (partition by barn.fkb_person1, barn.stat_aarmnd order by mottaker.kjonn) as nr -- Kvinne har høyere prioritering enn mann
        from barn_periode barn

        join
        (
            select stat_aarmnd, fk_person1, kjonn, max(navarende_fylke_nr) as navarende_fylke_nr, sum(belop) as belop
            from {{ ref('fak_bt_px_mottaker') }}
            group by stat_aarmnd, fk_person1, kjonn
        ) mottaker
        on barn.stat_aarmnd = mottaker.stat_aarmnd
        and barn.fk_person1 = mottaker.fk_person1
    )
    where nr = 1
)

select
    stat_aarmnd
   ,fkb_person1
   ,fk_person1
   ,aar
   ,aar_kvartal
   ,kvartal
   ,kvartal_besk
   ,navarende_fylke_nr
   ,belop
from barn_navarende_fylke_nr


--Last opp kun ny periode siden siste periode fra tabellen
--Tidligste periode fra tabellen er 201401
{% if is_incremental() %}

where stat_aarmnd > (select coalesce(max(stat_aarmnd), 201500) from {{ this }})

{% endif %}