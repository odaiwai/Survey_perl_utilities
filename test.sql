Drop Table if exists [test_table];
create table if not exists [test_table] as
    select date, sum(volume) as daily,
    (select sum(volume) as am_vol where time in ('07:00:00', '08:00:00')
--    (select sum(volume) from [traffic_counts] where (road_number = '0006092' and link_number = '0010' and carriageway = 'B' and direction = '8124'and time in ('09:00:00', '10:00:00', '11:00:00', '12:00:00', '13:00:00', '14:00:00') and date = thisdate)),
--   (select sum(volume) from [traffic_counts] where (road_number = '0006092' and link_number = '0010' and carriageway_id = 'B' and direction = '8124'and time in ('15:00:00', '16:00:00', '17:00:00') and date = thisdate)),
--    (select sum(volume) from [traffic_counts] where (road_number = '0006092' and link_number = '0010' and carriageway = 'B' and direction = '8124'and time in ('00:00:00', '01:00:00', '02:00:00', '03:00:00', '04:00:00', '05:00:00', '06:00:00', '18:00:00', '19:00:00', '20:00:00', '21:00:00', '22:00:00', '23:00:00', ) and date = thisdate));
    from [traffic_counts] where (road_number = '0006092' and link_number = '0010' and carriageway = 'B' and direction = '8124');
select * from [test_table];
