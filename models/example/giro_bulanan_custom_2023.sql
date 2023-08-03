{{ config(materialized='table') }}

with source_data as (

	SELECT DISTINCT nama_nasabah,
            nomor_rekening
           FROM {{ source('postgre_source', 'inforeklogview_sipening_info_rekening') }}    

), giro_bulanan_evaluate as (
	SELECT nama_nasabah AS namarekening,
    nomor_rekening AS nomorrekening,
    get_giro(concat('2022', '-12-27')::character varying, concat('2023', '-01-26')::character varying, nomor_rekening::character varying) AS januari,
    get_giro(concat('2023', '-01-27')::character varying, concat('2023', '-02-26')::character varying, nomor_rekening::character varying) AS februari,
    get_giro(concat('2023', '-02-27')::character varying, concat('2023', '-03-26')::character varying, nomor_rekening::character varying) AS maret,
    get_giro(concat('2023', '-03-27')::character varying, concat('2023', '-04-26')::character varying, nomor_rekening::character varying) AS april,
    get_giro(concat('2023', '-04-27')::character varying, concat('2023', '-05-26')::character varying, nomor_rekening::character varying) AS mei,
    get_giro(concat('2023', '-05-27')::character varying, concat('2023', '-06-26')::character varying, nomor_rekening::character varying) AS juni,
    get_giro(concat('2023', '-06-27')::character varying, concat('2023', '-07-26')::character varying, nomor_rekening::character varying) AS juli,
    get_giro(concat('2023', '-07-27')::character varying, concat('2023', '-08-26')::character varying, nomor_rekening::character varying) AS agustus,
    get_giro(concat('2023', '-08-27')::character varying, concat('2023', '-09-26')::character varying, nomor_rekening::character varying) AS september,
    get_giro(concat('2023', '-09-27')::character varying, concat('2023', '-10-26')::character varying, nomor_rekening::character varying) AS oktober,
    get_giro(concat('2023', '-10-27')::character varying, concat('2023', '-11-26')::character varying, nomor_rekening::character varying) AS november,
    get_giro(concat('2023', '-11-27')::character varying, concat('2023', '-12-26')::character varying, nomor_rekening::character varying) AS desember,
	get_giro_dki(1,nomor_rekening::numeric) as januari_dki,
	get_giro_dki(2,nomor_rekening::numeric) as februari_dki,
	get_giro_dki(3,nomor_rekening::numeric) as maret_dki,
	get_giro_dki(4,nomor_rekening::numeric) as april_dki,
	get_giro_dki(5,nomor_rekening::numeric) as mei_dki,
	get_giro_dki(6,nomor_rekening::numeric) as juni_dki,
	get_giro_dki(7,nomor_rekening::numeric) as juli_dki,
	get_giro_dki(8,nomor_rekening::numeric) as agustus_dki,
	get_giro_dki(9,nomor_rekening::numeric) as september_dki,
	get_giro_dki(10,nomor_rekening::numeric) as oktober_dki,
	get_giro_dki(11,nomor_rekening::numeric) as november_dki,
	get_giro_dki(12,nomor_rekening::numeric) as desember_dki
   FROM source_data
)

select *
from giro_bulanan_evaluate

