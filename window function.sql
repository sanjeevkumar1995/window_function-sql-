--row_number,rank,dense_rank,lag ,lead,fist_value,last_value,ntile,cume_dist,percent_rank

select * ,max(population) over(partition by district) as max   from data2 order by max desc
--row_number

select *,row_number()  over( partition by state order by population desc) as r_n   from data2

--rank
 
select a.* from (
select *, rank() over(partition by state order by population desc ) as rank  from data2 ) a
where a.rank<4

--dense_rank
select a.* from (
select *, dense_rank() over(partition by state order by population desc ) as Drank  from data2 ) a
where a.Drank<4

--lag
select*,lag(population,4,0) over(partition by state order by population desc) as lagging 
from data2
--use case of lag

select*,lag(population) over(partition by state order by population desc) as lagging,
case when population>lag(population) over(partition by state order by population desc)
then 'higher than previous'
when population<lag(population) over(partition by state order by population desc) then 
'lower than previous'
when population=lag(population) over(partition by state order by population desc) then 
'same as salary' end as sal_range
from data2


--lead
select*,lead(population) over(partition by state order by population desc) leading  from data2

--first_value
select *,first_value(District) over(partition by state order by population desc) as
first_value   from data2

--last_value
select *,last_value(District) over(partition by state order by population desc range between unbounded preceding and unbounded following ) as
last_value   from data2

--nTILE

select* , 
case 
when x.category=1 then 'high population'
when x.category=2 then 'moderate population'
when x.category=3 then 'low population' end as distribution
from
(select*,NTILE(3) over(order by population desc) as category from data2 where state =
'uttar pradesh') x

--cume_dist
select*,round(CUME_DIST() over(order by population desc)*100,1) as cume_dist from Data2
                                    
--use case(getting only first 30% data of whole data)
select district,cume_dist  as cumulative_distribution from
(select*,round(CUME_DIST() over(order by population desc)*100,1) as cume_dist from Data2)X
where x.cume_dist<=30

--percent_rank

select*,round(PERCENT_RANK() over(order by population) *100,1) as per_rank from data2

--use case
select * from 
(select*,round(PERCENT_RANK() over(order by population) *100,1) as per_rank from data2)X
where x.District='moradabad'
















