USE kaixindou;
CREATE TABLE IF NOT EXISTS kaixindou.hago_friend_game_push_train_data_v2(
  
  hdid STRING ,
  uid BIGINT ,
  friend_uid STRING ,
  game_id STRING ,
  push_id STRING ,
  is_show int,
  is_click int,
  
  country STRING ,
  sex int ,
  age int ,
  os STRING ,  -- 操作系统
  sjp STRING, --设备厂商

  country_f STRING ,
  sex_f int ,
  age_f int ,
  os_f STRING ,
  sjp_f STRING,
  
  uid_game_statistic struct<game_time:bigint, game_count:bigint, finish_count:bigint, escape_count:bigint> ,
  friend_game_statistic struct<game_time:bigint, game_count:bigint, finish_count:bigint, escape_count:bigint> ,
  
  uid_game_stat array<struct<game_id:STRING,game_time:bigint,win_count:bigint,finish_count:bigint,escape_count:bigint>> ,
  uid_game_type_stat array<struct<game_type:STRING,game_time:bigint,win_count:bigint,finish_count:bigint,escape_count:bigint>>,

  friend_game_stat array<struct<game_id:STRING,game_time:bigint,win_count:bigint,finish_count:bigint,escape_count:bigint>> ,
  friend_game_type_stat array<struct<game_type:STRING,game_time:bigint,win_count:bigint,finish_count:bigint,escape_count:bigint>>,
  
  game_count bigint,
  game_time bigint,
  game_push_show_count_7d bigint,
  game_push_click_count_7d bigint,
  game_type STRING,
  game_covers array<STRING>,
  game_country STRING,
  
  is_friend int,
  is_sameip int,
  is_second_relation int,
  
  uid_push_show_count_7d bigint,
  uid_push_click_count_7d bigint,
  
  friend_push_show_count_7d bigint,
  friend_push_click_count_7d bigint
  
) PARTITIONED BY (dt STRING);

--------------------------------------------------------------------------------
-- 一些临时表
with user_base_profile as (
select * from
(select hdid
    , uid
    , sex
    , age
    , friend_num
    , fb_friend_num
    , addr_friend_num
    , appkey as os
    , sjp
    , sjm
    , nation as  country
    , row_number() over(partition by hdid order by last_login_date desc) rank 
from kaixindou.hago_dwd_user_uid_total_detail
where dt='${date-2}') a
where rank=1
),

user_game_stat as(
select uid
  , game_id
  , named_struct(
    'game_time', sum(cast(game_end_time as bigint)-cast(game_start_time as bigint)),
    'win_count', sum(if(is_win=1,1,0)),
    'finish_count', sum(is_finish),
    'escape_count', sum(is_escape)) as uid_game_stat
from kaixindou.hago_ods_game_finish_day_detail
where dt>='${date-8}' and dt<='${date-2}'
group by uid,game_id
),

user_push_click_statistic_7d AS(
  select  hdid
    , sum(is_show) as push_show_count_7d
    , sum(is_click) as push_click_count_7d
  from kaixindou.hago_friend_game_push_info
  where dt>='${date-8}' and dt<='${date-2}'
  group by hdid
),

-- 目前只选取hdid和uid一一对应的用户
hago_friend_game_push_info_with_uid AS (
  select a.*, b.uid as uid
    from hago_friend_game_push_info a
    join hago_dwd_user_uid_total_detail b
    on a.hdid=b.hdid and a.dt='${date-1}' and b.dt='${date-2}'
    join (select hdid, count(distinct uid) as cnt from hago_dwd_user_uid_total_detail where dt='${date-2}' group by hdid) c 
    on c.hdid=a.hdid and c.cnt=1
),

hago_game_push_stat_with_game_type AS (
  select a.* , b.game_type, b.covers as game_covers, b.country_code as game_country
  from hago_game_push_stat a
  join hago_push_game_meta_info b
  on a.game_id=b.game_id and a.dt='${date-2}' and b.dt='${date-2}'
),

hago_uid_friend_info AS (
  select uid, friend_uid, 1 as is_friend from hago_dwd_friend_hago_day_detail
  where dt='${date-2}'
)

-----------------------------------------------------------------------------------------
-- join 产出最后的结果
INSERT OVERWRITE TABLE kaixindou.hago_friend_game_push_train_data_v2 PARTITION (dt='${date-1}')
select
  -- hago_friend_game_push_info
  t0.hdid as hdid
  , t0.uid as uid
  , t0.friend_uid as friend_uid
  , t0.game_id as game_id
  , t0.push_id as push_id
  , t0.is_show as is_show
  , t0.is_click as is_click
  
  -- user_base_profile
  , t1_1.country as country
  , t1_1.sex as sex
  , t1_1.age as age
  , t1_1.os as os
  , t1_1.sjp as sjp

  -- usere_base_profile
  , t1_2.country as country_f
  , t1_2.sex as sex_f
  , t1_2.age as age_f
  , t1_2.os as os_f
  , t1_2.sjp as sjp_f

  -- user_game_stat
  , t2_1.uid_game_stat as uid_game_statistic
  
  -- user_game_stat
  , t2_2.uid_game_stat as friend_game_statistic

  -- hago_user_play_game_info_stat
  , t3_1.game_stat as uid_game_stat
  , t3_1.game_type_stat as uid_game_type_stat
  
  -- hago_user_play_game_info_stat
  , t3_2.game_stat as friend_game_stat
  , t3_2.game_type_stat as friend_game_type_stat

  -- hago_game_push_stat
  , t4.game_count as game_count
  , t4.game_time as game_time
  , t4.push_show_count_7d as game_push_show_count_7d
  , t4.push_click_count_7d as game_push_click_count_7d
  , t4.game_type as game_type
  , t4.game_covers as game_covers
  , t4.game_country as game_country

  -- t5
  , t5.is_friend as is_friend
  , null as is_sameip
  , null as is_second_relation

  -- user_push_click_statistic_7d
  , t6.push_show_count_7d as uid_push_show_count_7d
  , t6.push_click_count_7d as uid_push_click_count_7d

  -- hago_game_push_friend_stat
  , t7.push_show_count_7d as friend_push_show_count_7d
  , t7.push_click_count_7d as friend_push_click_count_7d

from 
hago_friend_game_push_info_with_uid t0 
left outer join
user_base_profile t1_1 on t0.uid=t1_1.uid  
left outer join
user_base_profile t1_2 on t0.friend_uid=t1_2.uid 
left outer join
user_game_stat t2_1 on t0.uid=t2_1.uid  and t0.game_id=t2_1.game_id
left outer join
user_game_stat t2_2 on t0.friend_uid=t2_2.uid and t0.game_id=t2_2.game_id
left outer join
hago_user_play_game_info_stat t3_1 on t0.uid=t3_1.uid and t3_1.dt='${date-2}'
left outer join
hago_user_play_game_info_stat t3_2 on t0.friend_uid=t3_2.uid and t3_2.dt='${date-2}'
left outer join 
hago_game_push_stat_with_game_type t4 on t0.game_id=t4.game_id
left outer join
hago_uid_friend_info t5 on t0.uid=t5.uid and t0.friend_uid=t5.friend_uid
left outer join
user_push_click_statistic_7d t6 on t0.hdid=t6.hdid
left outer join
hago_game_push_friend_stat t7 on t0.friend_uid=t7.friend_uid and t7.dt='${date-2}'





