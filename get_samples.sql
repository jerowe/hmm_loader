select * from hit limit 10;
select sample_id, hit_name, hit_len, MIN(hit_evalue) from hit group by sample_id;
