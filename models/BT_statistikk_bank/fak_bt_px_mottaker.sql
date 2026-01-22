{{
    config(
        materialized='incremental'
    )
}}

--Tabellen inneholder navarende_kommune_nr for alle kommuner
with geografi as (
    select
        kommune_nr
       ,max(navarende_kommune_nr) navarende_kommune_nr
    from {{ source('bt_statistikk_bank_dt_kodeverk', 'dim_geografi_kommune') }}
    group by kommune_nr --Kommune_nr fra tabellen er unik per i dag
)
,
--Hent ut mottaker for aktuell periode
--Finn ut om mottaker er barn
mottaker_periode as (
    select
        periode.aar
       ,periode.aar_kvartal
       ,periode.kvartal
       ,periode.kvartal_besk
       ,mottaker.*
       ,case when barn.fk_person1 is not null then 1 else 0 end barn_selv_mottaker_flagg
    from {{ source('bt_statistikk_bank_dvh_fam_bt', 'fak_bt_mottaker') }} mottaker

    left join
    (
        select stat_aarmnd, fk_person1
        from {{ source('bt_statistikk_bank_dvh_fam_bt', 'fak_bt_barn') }}
        where fk_person1 = fkb_person1 --Barn selv er mottaker
        group by stat_aarmnd, fk_person1
    ) barn
    on mottaker.fk_person1 = barn.fk_person1
    and mottaker.stat_aarmnd = barn.stat_aarmnd

    join {{ source('bt_statistikk_bank_dvh_fam_bt', 'dim_bt_px_periode') }} periode
    on mottaker.stat_aarmnd = to_char(periode.siste_dato_i_perioden, 'yyyymm') --Siste måned i kvartal

    where ((mottaker.statusk != 4 and mottaker.stat_aarmnd <= 202212) --Publisert statistikk(nav.no) til og med 2022, har filtrert vekk Institusjon(statusk=4).
            or mottaker.stat_aarmnd >= 202301 --Statistikk fra og med 2023, inkluderer Institusjon.
          )
)
,
--Hent ut bosted_kommune_nr på nytt basert på gt_verdi fra dim_person for mottaker som ikke har bosted_kommune_nr
--Denne logikken gjelder hovedsakelig for data fra Infotrygd
mottaker_blank_bosted_kommunenr as (
    select mottaker.*
          ,case when dim_person.bosted_kommune_nr = '----' and dim_person.getitype in ('K', 'B') then substr(dim_person.gt_verdi, 1, 4)
                else dim_person.bosted_kommune_nr
           end bosted_kommune_nr_dim --Data fra Infotrygd har sannsynligvis ikke verdi på bosted_kommune_nr. Samme logikken ble allerede implementert i månedsprosessering for nye data.
          ,dim_person.gt_verdi as gt_verdi_dim
    from mottaker_periode mottaker

    join {{ source('bt_statistikk_bank_dt_person', 'dim_person') }} dim_person
    on mottaker.fk_dim_person = dim_person.pk_dim_person

    where mottaker.bosted_kommune_nr is null
)
,

mottaker_bosted_kommunenr_alle as (
    select
        fk_person1
       ,aar
       ,aar_kvartal
       ,kvartal
       ,kvartal_besk
       ,stat_aarmnd
       ,barn_selv_mottaker_flagg
       ,kjonn
       ,fk_dim_alder
       ,belop
       ,bosted_kommune_nr
       ,dim_gt_verdi as mottaker_gt_verdi

       --Utvidet info
       ,statusk
       ,belop_utvidet
       ,belop_smabarnstillegg
    from mottaker_periode
    where bosted_kommune_nr is not null

    union all
    select
        fk_person1
       ,aar
       ,aar_kvartal
       ,kvartal
       ,kvartal_besk
       ,stat_aarmnd
       ,barn_selv_mottaker_flagg
       ,kjonn
       ,fk_dim_alder
       ,belop
       ,bosted_kommune_nr_dim as bosted_kommune_nr
       ,gt_verdi_dim as mottaker_gt_verdi

       --Utvidet info
       ,statusk
       ,belop_utvidet
       ,belop_smabarnstillegg
    from mottaker_blank_bosted_kommunenr
)
,

--Hent ut nåværende kommunenr basert på bosted_kommune_nr
mottaker_navarende_kommune_nr as (
    select
        mottaker.*
       ,geografi.navarende_kommune_nr
    from mottaker_bosted_kommunenr_alle mottaker

    left join geografi
    on mottaker.bosted_kommune_nr = geografi.kommune_nr
)
--select * from mottaker_geografi where kommune_navn = 'Lunner';
,
--Hent ut fylkenr fra de to første sifre av bosted_kommune_nr for numerisk bosted_kommune_nr
--Sett fylkenr til 98(utland) for ikke numerisk bosted_kommune_nr og gt_verdi finnes i tabellen dim_land.land_iso_3_kode
--Resten settes til 99(ukjent)
mottaker_navarende_fylke as (
  select
      mottaker.*
     ,case when (bosted_kommune_nr is null or not regexp_like(bosted_kommune_nr, '^[[:digit:]]+$')) and dim_land.land_iso_3_kode is not null then '98' --Når gtverdi peker på en landskode, setter det til Utland(fylkenr=98)
           when (bosted_kommune_nr is not null and regexp_like(bosted_kommune_nr, '^[[:digit:]]+$')) then substr(navarende_kommune_nr,1,2)
           else '99' --Ukjent
      end navarende_fylke_nr
  from mottaker_navarende_kommune_nr mottaker

  left outer join
  (
      select distinct land_iso_3_kode
      from dt_kodeverk.dim_land
  ) dim_land
  on mottaker.mottaker_gt_verdi = dim_land.land_iso_3_kode
)
,

--Legg til kjønn og alder
--Bruk inner join mot alder, kjønn og fylke for å beholde rad som har full info.
  --Hensikten er å holde likt totalt antall for ulike statistikk.
navarende_fylke_kjonn_alder as (
    select
        mottaker.*
       ,kjonn.kjonn_besk
       ,dim_alder.alder
       ,alder_gruppe.alder_gruppe_besk

    from mottaker_navarende_fylke mottaker

    join {{ source('bt_statistikk_bank_dt_kodeverk', 'dim_alder') }} dim_alder
    on mottaker.fk_dim_alder = dim_alder.pk_dim_alder

    join
    (
        select distinct alder_fra_og_med, alder_til_og_med, alder_gruppe_besk
        from {{ source('bt_statistikk_bank_dvh_fam_bt', 'dim_bt_px_alder_gruppe') }}
    ) alder_gruppe
    on dim_alder.alder between alder_gruppe.alder_fra_og_med and alder_gruppe.alder_til_og_med

    join {{ source('bt_statistikk_bank_dvh_fam_bt', 'dim_bt_px_kjonn') }} kjonn
    on mottaker.kjonn = kjonn.kjonn_kode

    join {{ source('bt_statistikk_bank_dvh_fam_bt','dim_bt_px_navarende_fylke') }} fylke
    on mottaker.navarende_fylke_nr = fylke.nåværende_fylke_nr
)
select *
from navarende_fylke_kjonn_alder


--Last opp kun ny periode siden siste periode fra tabellen
--Tidligste periode fra tabellen er 201401
{% if is_incremental() %}

where stat_aarmnd > (select coalesce(max(stat_aarmnd), 201500) from {{ this }})

{% endif %}