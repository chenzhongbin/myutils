update me.me_info t set t.app_date=(
  select max(OPERATOR_DATE)OPERATOR_DATE from me.me_audit_node
  where me_id=t.me_id and result_id='010001' group by me_id
)
where t.VALIDITY in('1','2')
  and t.flow_status is not null
  and t.app_date is null
  and t.flow_status<>'0000'
  and t.app_type='1' 