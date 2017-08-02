select t1.* from(
  select * from me.me_number_limit
  where is_sum='2' and limit_year='2016'
)t1 join(
  select * from me.spt_organ where rank='4'
)t2 on t1.organ_id=t2.organ_id