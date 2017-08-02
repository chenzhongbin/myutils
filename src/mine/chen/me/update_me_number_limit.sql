select * from(
  select 
    t.limit_year,
    t2.organ_name,
    t2.rank,
    (select dept_name from me.spt_dept where dept_id=t.dept_id) dept_name,
    t.max_number,
    t.apply_number,
    case 
      when t.is_sum='1' and t2.rank='1' then t_r1.count --省级
      when t.is_sum='1' and t2.rank='2' and t.organ_id='530100000' then t_r2_km.count --州市（昆明）
      when t.is_sum='1' and t2.rank='2' and t.organ_id<>'530100000' then t_r2.count --州市（其它）
      when t.is_sum='1' and t2.rank='3' then t_r3.count --区县
      when t.is_sum='1' and t2.rank='4' and t.dept_id is null then nvl(t_r4.count,0) --分所
      when t.is_sum='1' and t2.rank='4' and t.dept_id is not null then nvl(t_r4_dept.count,0) --分所
      when t.is_sum ='2' then nvl(t3.count,0) --本级部门
      else null
    end count,
    t.is_sum,
    t.organ_id,
    t.dept_id,
    t.id
  from me.me_number_limit t 
  join (
    select * from me.spt_organ where 1=1
--      and rank='4'
  ) t2 on t.organ_id=t2.organ_id
  
  --查询本级部门承办扶持数 is_sum=2
  left join (
    select receive_organ,RECEIVE_DEPT,count(1) count 
    from me.me_info where 1=1
      and receive_organ is not null
      and receive_dept is not null
      and flow_status is not null 
      and validity in('1','2') 
      and flow_status!='0000'
      and app_type='1'
    group by receive_organ,RECEIVE_DEPT
    order by receive_organ,RECEIVE_DEPT
  ) t3 on t.organ_id=t3.receive_organ and t.DEPT_ID=t3.receive_organ||t3.RECEIVE_DEPT
  
--  条件：is_sum=1 and rank=1,省级
  left join(
    select substr(receive_organ,1,2) receive_organ,count(1) count 
    from me.me_info where 1=1
      and receive_organ is not null
      and receive_dept is not null
      and flow_status is not null 
      and validity in('1','2') 
      and flow_status!='0000'
      and app_type='1'
    group by substr(receive_organ,1,2)
    order by receive_organ
  ) t_r1 on t_r1.receive_organ=substr(t.organ_id,1,2)--省级
  
--  条件：is_sum=1 and rank=2，州市（非昆明市）
  left join(
    select substr(receive_organ,1,4) receive_organ,count(1) count 
    from me.me_info where 1=1
      and receive_organ is not null
      and receive_dept is not null
      and flow_status is not null 
      and validity in('1','2') 
      and flow_status!='0000'
      and app_type='1'
    group by substr(receive_organ,1,4)
    order by receive_organ
  ) t_r2 on t_r2.receive_organ=substr(t.organ_id,1,4)--州市级（不包含昆明）
  
--  条件：is_sum=1 and rank=2,州市（昆明市）
  left join(
    select count(1) count 
    from me.me_info where 1=1
      and (receive_organ like '5301%' or receive_organ like '5340%')
      and receive_organ is not null
      and receive_dept is not null
      and flow_status is not null 
      and validity in('1','2') 
      and flow_status!='0000'
      and app_type='1'
    order by receive_organ
  ) t_r2_km on 1=1--昆明市
  
--  条件：is_sum=1 and rank=3 区县
  left join(
    select substr(receive_organ,1,6) receive_organ,count(1) count 
    from me.me_info where 1=1
      and receive_organ is not null
      and receive_dept is not null
      and flow_status is not null 
      and validity in('1','2') 
      and flow_status!='0000'
      and app_type='1'
    group by substr(receive_organ,1,6)
    order by receive_organ
  ) t_r3 on t_r3.receive_organ=substr(t.organ_id,1,6)--区县级
  
-- 条件：is_sum=1 and rank=4 分所不区别部门
  left join(
    select receive_organ,count(1) count 
    from me.me_info where 1=1
      and receive_organ is not null
      and receive_dept is not null
      and flow_status is not null 
      and validity in('1','2') 
      and flow_status!='0000'
      and app_type='1'
    group by receive_organ
    order by receive_organ
  ) t_r4 on t.organ_id=t_r4.receive_organ--分所不区别部门
  
-- 条件：is_sum=1 and rank=4 分所区别部门
  left join(
    select receive_organ,receive_dept,count(1) count 
    from me.me_info where 1=1
      and receive_organ is not null
      and receive_dept is not null
      and flow_status is not null 
      and validity in('1','2') 
      and flow_status!='0000'
      and app_type='1'
    group by receive_organ,receive_dept
    order by receive_organ,receive_dept
  ) t_r4_dept on t.organ_id=t_r4_dept.receive_organ 
    and t.dept_id=t_r4_dept.receive_organ||t_r4_dept.receive_dept--分所区别部门
  
order by t.organ_id,t.is_sum
) TX
where 1=1
  and (tx.apply_number<>tx.count or tx.apply_number is null)
