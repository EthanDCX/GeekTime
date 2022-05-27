CREATE EXTERNAL TABLE `t_movie_dcx`(
  `movieid` int COMMENT '电影 ID', 
  `moviename` string COMMENT '电影名', 
  `movietype` string COMMENT '电影类型')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='::') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://emr-header-1.cluster-285604:9000/data/hive/movies'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1648967349')
  
  
CREATE EXTERNAL TABLE `t_user_dcx`(
  `userid` int COMMENT '', 
  `sex` string COMMENT '', 
  `age` int COMMENT '', 
  `occupation` int COMMENT '职业', 
  `zipcode` int COMMENT '邮编')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='::') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://emr-header-1.cluster-285604:9000/data/hive/users'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1648967215');
  
CREATE EXTERNAL TABLE `t_rating_dcx`(
  `userid` int COMMENT '', 
  `movieid` int COMMENT '', 
  `rate` int COMMENT '评分', 
  `times` bigint COMMENT '评分时间')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='::') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://emr-header-1.cluster-285604:9000/data/hive/rating'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1648967291');
  
  
-- 展示电影 ID 为 2116 这部电影各年龄段的平均影评分。
select u.age as age , avg(r.rate) as avgrate 
from t_rating_dcx r 
join t_user_dcx u on r.userid = u.userid
where r.movieid = 2116 group by u.age;

解析: 
特定条件 要有where
平均评分 要用avg()
各年龄段 要有分组
涉及多表 要有关联join


-- 找出男性评分最高且评分次数超过 50 次的 10 部电影，展示电影名，平均影评分和评分次数。

select collect_set(u.sex)[0], collect_set(m.moviename)[0] ,avg(r.rate) as avgrate,count(m.movieid) as total 
from t_rating_dcx r  
join t_user_dcx u on  r.userid = u .userid 
join t_movie_dcx m on r.movieid = m.movieid
where u.sex = "M" 
group by m.movieid
having total > 50 
order by avgrate desc
limit 10;

select u.sex, m.moviename,avg(r.rate) as avgrate,count(m.moviename) as total 
from t_rating_dcx r  
join t_user_dcx u on  r.userid = u .userid 
join t_movie_dcx m on r.movieid = m.movieid
where u.sex = "M" 
group by m.moviename , u.sex
having total > 50 
order by avgrate desc
limit 10;

解析:
top10 要有order by desc limit 
统计电影评分 次数 , 要有聚合子查询 having count(movie) > 
核心是找top10 电影 ,所以要有group by movie,用movieId和name都可以
用到了三表的字段,所以要关联,关联顺序无所谓.最终都是一张大宽表.

遇到的问题:
Expression Not In Group By Key

Hive 中所有 select 后面非聚合函数字段, 都要出现在 group by 后面,如果不想group by非聚合字段,可以使用collect_set()包裹改字段,返回一个数组,使用数组下标访问数据.


-- 找出影评次数最多的女士所给出最高分的 10 部电影的平均影评分，展示电影名和平均影评分（可使用多行 SQL）。

思路:
1. 找出最爱看电影的女性id
2. 要有个中间表来放特定的用户(评分次数最多的女性),电影id 影评 用movieid和rate表关联
3. 中间表和电影表 影评表关联,钻石电影名 平均分
 

select u.userid ,count(r.rate) as total from t_user_dcx u 
join t_rating_dcx r on r.userid = u.userid 
where u.sex = "F"
group by u.userid
order by total desc
limit 1;
给影评数降序排序取第一行数据,拿到影评最大值的userid


create table middle_table_dcx as
select r.movieid , r.rate from t_rating_dcx r 
where r.userid = 1150 
order by r.rate desc
limit 10;
中间表存放该用户评分最高的10部电影id.



select  m.moviename as moviename , avg(r.rate) as avgrate  
from t_movie_dcx m 
join t_rating_dcx r on m.movieid = r.movieid 
join middle_table_dcx t on t.movieid = m.movieid  
group by  moviename
order by avgrate desc;
与中间表关联,拿到这些电影的平均评分

因为使用了avg聚合函数,所以要group by
这里不用limit了,因为和中间表join出来最多只能有10条数据




