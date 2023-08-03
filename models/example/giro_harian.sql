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
status_rekening,
 case
		when saldo < coalesce ((select t2.nominal_1 from trbungagiro t2 
		where tanggal_data >= date(t2.d_awal) and 
				tanggal_data <= date(t2.d_akhir) and t2.c_aktive = '1' limit 1),(select t4.nominal_1 from trbungagiro t4 where c_aktive = '3' limit 1)
		) then 0
		when saldo < coalesce ((select t2.nominal_2 from trbungagiro t2 
		where tanggal_data >= date(t2.d_awal) and 
				tanggal_data <= date(t2.d_akhir) and t2.c_aktive = '1' limit 1),(select t4.nominal_2 from trbungagiro t4 where c_aktive = '3' limit 1)
		) then floor((saldo * (
		coalesce ((select t2.bunga_1/100 from trbungagiro t2 
		where tanggal_data >= date(t2.d_awal) and 
				tanggal_data <= date(t2.d_akhir) and t2.c_aktive = '1' limit 1),(select t4.bunga_1 from trbungagiro t4 where c_aktive = '3' limit 1)/100
		)
		))/365)
		when saldo < coalesce ((select t2.nominal_3 from trbungagiro t2 
		where tanggal_data >= date(t2.d_awal) and 
				tanggal_data <= date(t2.d_akhir) and t2.c_aktive = '1' limit 1),(select t4.nominal_3 from trbungagiro t4 where c_aktive = '3' limit 1)
		) then floor((saldo * (
		coalesce ((select t2.bunga_2/100 from trbungagiro t2 
		where tanggal_data >= date(t2.d_awal) and 
				tanggal_data <= date(t2.d_akhir) and t2.c_aktive = '1' limit 1),(select t4.bunga_2 from trbungagiro t4 where c_aktive = '3' limit 1)/100
		)
		))/365)
		when saldo < coalesce ((select t2.nominal_4 from trbungagiro t2 
		where tanggal_data >= date(t2.d_awal) and 
				tanggal_data <= date(t2.d_akhir) and t2.c_aktive = '1' limit 1),(select t4.nominal_4 from trbungagiro t4 where c_aktive = '3' limit 1)
		) then floor((saldo * (
		coalesce ((select t2.bunga_3/100 from trbungagiro t2 
		where tanggal_data >= date(t2.d_awal) and 
				tanggal_data <= date(t2.d_akhir) and t2.c_aktive = '1' limit 1),(select t4.bunga_3 from trbungagiro t4 where c_aktive = '3' limit 1)/100
		)
		))/365)
		when saldo > coalesce ((select t2.nominal_4 from trbungagiro t2 
		where tanggal_data >= date(t2.d_awal) and 
				tanggal_data <= date(t2.d_akhir) and t2.c_aktive = '1' limit 1),(select t4.nominal_4 from trbungagiro t4 where c_aktive = '3' limit 1)
		) then floor((saldo * (
		coalesce ((select t2.bunga_4/100 from trbungagiro t2 
		where tanggal_data >= date(t2.d_awal) and 
				tanggal_data <= date(t2.d_akhir) and t2.c_aktive = '1' limit 1),(select t4.bunga_4 from trbungagiro t4 where c_aktive = '3' limit 1)/100)
		))/365)
		else 0 end 
from {{ source('postgre_source', 'inforeklogview_sipening_info_rekening') }}

)

select *
from source_data

