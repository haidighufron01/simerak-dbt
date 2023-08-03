
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with source_data as (

    SELECT 
saldo, 
cif_no, 
kategori, 
deskripsi, 
nama_nasabah, 
tanggal_data, 
to_char(tanggal_data, 'dd') hari_data,
to_char(tanggal_data, 'MM') bulan_data,
to_char(tanggal_data, 'YYYY') tahun_data,
nomor_rekening, 
status_rekening
FROM {{ source('postgre_source', 'inforeklogview_sipening_info_rekening') }}

)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
