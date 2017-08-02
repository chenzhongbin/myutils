	SELECT 
		A.PE_ID AS ENT_ID,
		A.REG_NO AS REG_NO,
		NULL AS UNI_SCID,
		A.NAME AS ENT_NAME,
		'9500' AS TYPE_GB,
		A.ADDRESS,
		A.ESTABLISH_DATE ,
		A.TELEPHONE,
		CAST( CASE 
			WHEN A.EXIST_STATUS IN('0001','0007','0008','0009')  THEN  '1' --存续（在营、开业、在册）
            WHEN A.EXIST_STATUS='0006'  THEN  '2'  --吊销，未注销
            WHEN A.EXIST_STATUS='0010'  THEN  '3'  --吊销，已注销
            WHEN A.EXIST_STATUS='0004'  THEN  '4'  --注销
            WHEN A.EXIST_STATUS='0012'  THEN  '5'  --撤销
            WHEN A.EXIST_STATUS IN ('0011','0002')  THEN  '6' --迁出
            ELSE '9'                              --其他
            END  AS  VARCHAR(2)           
	    ) AS  EXIST_STATUS,     -- 经营状态
	    '2' SRC --PE
	FROM ETPS.PE_INFO_ENTY A