{{
    config(
        materialized='incremental'
    )
}}

with geografi as (
    select
        pk_dim_geografi
       ,navarende_fylke_nr
       ,gtverdi
       ,gyldig_fra_dato
       ,gyldig_til_dato
       ,kommune_navn
       ,fylke_nr
    from {{ source('bt_statistikk_bank_dt_kodeverk', 'dim_geografi') }}
)
,

--Ekskludere barn som selv mottar barnetrygd
barn_periode as (
    select
        barn.stat_aarmnd
       ,barn.fkb_person1
       ,barn.fk_dim_person_barn
       ,periode.aar
       ,periode.aar_kvartal
       ,periode.kvartal
       ,periode.kvartal_besk
       ,max(case when barn.fkb_person1 = barn.fk_person1 then 1 else 0 end) as barn_selv_mottaker_flagg
    from {{ source('bt_statistikk_bank_dvh_fam_bt', 'fam_bt_barn') }} barn

    join {{ source('bt_statistikk_bank_dvh_fam_bt', 'dim_bt_periode') }} periode
    on barn.stat_aarmnd = to_char(periode.siste_dato_i_perioden, 'yyyymm') --Siste måned i kvartal

    where barn.gyldig_flagg = 1

    group by
        barn.stat_aarmnd
       ,barn.fkb_person1
       ,barn.fk_dim_person_barn
       ,periode.aar
       ,periode.aar_kvartal
       ,periode.kvartal
       ,periode.kvartal_besk
)
,

--Legg til fylkenr basert på fremmednøkkel til dim_person
barn_fylkenr as (
    select
        barn.*
       ,dim_person.gt_verdi
       ,geografi.navarende_fylke_nr
       ,geografi.kommune_navn
       ,geografi.fylke_nr
    from barn_periode barn

    join {{ source('bt_statistikk_bank_dt_person', 'dim_person') }} dim_person
    on barn.fk_dim_person_barn = dim_person.pk_dim_person

    left join geografi
    on dim_person.fk_dim_geografi_bosted = geografi.pk_dim_geografi
)
,

--Hent ut fylkenr basert på gtverdi for de som har Ukjent nåværende fylkenr etter forrige steg
barn_ukjent_gtverdi as
(
    select
        barn_fylkenr.fkb_person1
       ,barn_fylkenr.aar
       ,barn_fylkenr.aar_kvartal
       ,barn_fylkenr.kvartal
       ,barn_fylkenr.kvartal_besk
       ,barn_fylkenr.stat_aarmnd
       ,barn_fylkenr.gt_verdi
       ,barn_fylkenr.barn_selv_mottaker_flagg
       ,dim_land.land_iso_3_kode
       ,case when dim_land.land_iso_3_kode is not null then '98'
             else gt_verdi.navarende_fylke_nr
        end navarende_fylke_nr --Når det er landskode på gtverdi, tilhører det Utland(fylkenr=98)
       ,gt_verdi.gtverdi
    from barn_fylkenr

    left outer join
    (
        select distinct land_iso_3_kode
        from dt_kodeverk.dim_land
    ) dim_land
    on barn_fylkenr.gt_verdi = dim_land.land_iso_3_kode

    left join
    (
        select gtverdi
              ,max(navarende_fylke_nr) keep (dense_rank first order by gyldig_fra_dato desc) navarende_fylke_nr
        from geografi
        group by gtverdi
    ) gt_verdi
    on barn_fylkenr.gt_verdi = gt_verdi.gtverdi

    where barn_fylkenr.navarende_fylke_nr = 'Ukjent'
)
--select * from barn_ukjent_gtverdi;
,

barn_fylkenr_alle as
(
    select
        fkb_person1
       ,aar
       ,aar_kvartal
       ,kvartal
       ,kvartal_besk
       ,stat_aarmnd
       ,navarende_fylke_nr
       ,barn_selv_mottaker_flagg
    from barn_fylkenr
    where navarende_fylke_nr != 'Ukjent' or navarende_fylke_nr is null

    union all
    select
        fkb_person1
       ,aar
       ,aar_kvartal
       ,kvartal
       ,kvartal_besk
       ,stat_aarmnd
       ,navarende_fylke_nr
       ,barn_selv_mottaker_flagg
    from barn_ukjent_gtverdi
)
--select * from barn_alle;
,

barn_navarende_fylke as (
  select
      barn.*
     ,fylke.nåværende_fylke_nr_navn
     ,fylke.nåværende_fylke_nr
     ,fylke.nåværende_fylkenavn
  from barn_fylkenr_alle barn

  left join {{ source('bt_statistikk_bank_dvh_fam_bt', 'dim_bt_navarende_fylke') }} fylke
  on fylke.nåværende_fylke_nr = coalesce(barn.navarende_fylke_nr, '99')
)

select *
from barn_navarende_fylke


--Last opp kun ny periode siden siste periode fra tabellen
--Tidligste periode fra tabellen er 201401
{% if is_incremental() %}

where stat_aarmnd > (select coalesce(max(stat_aarmnd), 201500) from {{ this }})

{% endif %}