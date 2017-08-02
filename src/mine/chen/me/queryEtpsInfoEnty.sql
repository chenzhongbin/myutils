	SELECT 
		a.ETPS_ID AS ENT_ID, 
		a.REG_NO, 
		b.UNI_SCID, 
		a.ETPS_NAME AS ENT_NAME, 
		a.ETPS_TYPE_GB AS TYPE_GB, 
		a.ADDRESS, 
		a.ESTABLISH_DATE, 
		a.TELEPHONE, 
		CAST( 
			CASE WHEN a.EXIST_STATUS IN('0001','0007','0008','0009')  THEN  '1' --存续（在营、开业、在册）
			     WHEN a.EXIST_STATUS='0006'  THEN  '2'  --吊销，未注销
			     WHEN a.EXIST_STATUS='0010'  THEN  '3'  --吊销，已注销
			     WHEN a.EXIST_STATUS='0004'  THEN  '4'  --注销
			     WHEN a.EXIST_STATUS='0012'  THEN  '5'  --撤销
			     WHEN a.EXIST_STATUS IN ('0011','0002')  THEN  '6' --迁出
			     ELSE '9'                              --其他
	    	END  AS  VARCHAR(2)           
	    ) AS EXIST_STATUS,     -- 经营状态
	    '1' SRC --ETPS
	FROM etps.ETPS_INFO_ENTY a 	
		left join (
			select etps_id,uni_scid from etps.ETPS_SZHY_INFO_ENTY
		) b on a.etps_id=b.etps_id 