WITH TL AS
(
  SELECT B1.CPC,
    B1.CSC,
    B1.CSN,
    B1.CSC_P,
    B1.CCC,
    B1.NSD,
    CASE 
     WHEN B1.CSL = 4 THEN
      B5.CSN
     ELSE
      B1.CSN
    END AS GL_ATTR,
    B1.CSL,
    CASE WHEN B1.CSL = 4 THEN B4.CSN 
         WHEN B1.CSL = 3 THEN B3.CSN
         WHEN B1.CSL = 2 THEN B2.CSN
         WHEN B1.CSL = 1 THEN B1.CSN
    END AS FACCTNAME_LV1,
    CASE WHEN B1.CSL = 4 THEN B3.CSN 
         WHEN B1.CSL = 3 THEN B2.CSN
         WHEN B1.CSL = 2 THEN B1.CSN
    END AS FACCTNAME_LV2,
    CASE WHEN B1.CSL = 4 THEN B2.CSN 
         WHEN B1.CSL = 3 THEN B1.CSN
    END AS FACCTNAME_LV3,
    CASE WHEN B1.CSL = 4 THEN B1.CSN
    END AS FACCTNAME_LV4
  FROM (SELECT CPC, CSC, CSN, CSC_P, CCC
        , NSD, CSL
        FROM ODS.FVS
        WHERE HDATASRC = 'A'
        ) B1
      LEFT JOIN 
       (SELECT CPC, CSC, CSN, CSC_P, CCC
        , NSD, CSL
        FROM ODS.FVS 
        WHERE HDATASRC = 'A'
        ) B2
        ON B1.CPC = B2.CPC
       AND B1.CSC_P = B2.CSC
      LEFT JOIN 
       (SELECT CPC, CSC, CSN, CSC_P, CCC
        , NSD, CSL
        FROM ODS.FVS 
        WHERE HDATASRC = 'A'
        ) B3
        ON B2.CPC = B3.CPC
       AND B2.CSC_P = B3.CSC
      LEFT JOIN 
       (SELECT CPC, CSC, CSN, CSC_P, CCC
        , NSD, CSL
        FROM ODS.FVS 
        WHERE HDATASRC = 'A'
        ) B4
        ON B3.CPC = B4.CPC
       AND B3.CSC_P = B4.CSC
      LEFT JOIN 
       (SELECT CPC, CSC, CSN, CSC_P, CCC
        , NSD, CSL
        FROM ODS.FVS 
        WHERE HDATASRC = 'A'
        ) B5
        ON B1.CPC = B5.CPC
       AND B1.CSC_P = B5.CSC
)
INSERT OVERWRITE TABLE TMP.TFVDM1 PARTITION (HDATASRC = 'A')
SELECT  qt_sequence("UUID", A.CAC)                          AS UUID,            
        C.PH                                                                                                                    AS PH,            
        A.CAC                                                                                                                                               AS PC,             
        A.CPC                                                                                                                                             AS ASS,       
        A.D_BIZ                                                                                                                                       AS BD,              
              E.CH                                                                                                                                        AS CH,             
        F.EH                                                                                                                                              AS EH,             
        A.CSC                                                                                                                                     AS GL_CODE,                
        CASE
          WHEN A.CSN = ' ' THEN
           A.C_KEY_NAME
          ELSE
           NVL(A.CSN,A.C_KEY_NAME)
        END                                                                                                                                          AS GL_NAME,                
        A.N_VALPRICE                                                                                                                                   AS ATPRICE,            
        A.N_HLDAMT                                                                                                                                     AS ATQTY,              
        A.N_HLDCST_LOCL                                                                                                                            AS ATCOST,             
                A.N_HLDCST                                                                                                                           AS ATCOST_ORICUR,      
        A.N_HLDMKV_LOCL                                                                                                                              AS ATMKTVAL,           
        A.N_HLDMKV                                                                                                                                     AS ATMKTVAL_ORICUR,    
        A.N_HLDVVA_L                                                                                                                                 AS ATVAL_ADDED,        
        A.N_HLDVVA                                                                                                                                   AS ATVAL_ADDED_ORICUR, 
              A.N_VALRATE                                                                                                                            AS ATEXRATE,           
        NULL                                                                                                                                                 AS COST_TO_AT_RIO,    
        NULL                                                                                                                                         AS MKTVAL_TO_AT_RIO,  
        B.NSD                                                                                                                                    AS IS_DETAIL_GL,           
        A.C_PA_CODE                                                                                                                                    AS ATITEM,
        A.C_IVT_CLSS                                                                                                                                 AS INVEST_CLASS,
        A.C_ML_ATTR                                                                                                                                  AS ISSUE_MODE,
        A.C_FEE_CODE                                                                                                             AS FEE_CODE,
        A.C_SEC_VAR_MX                                                                                                                               AS SEC_KIND,                                                      
        A.C_TD_ATTR                                            AS TRADE_ATTR,             
        H.C_CA_ATTR                                            AS CASH_ACCOUNT,           
        A.GL_LV1                                                                                                   AS GL_LV1,                 
        B.FACCTNAME_LV1                                                                                                                              AS GL_NAME_LV1,            
        B.FACCTNAME_LV2                                                                                                                              AS GL_NAME_LV2,            
        B.FACCTNAME_LV3                                                                                                                              AS GL_NAME_LV3,            
        B.FACCTNAME_LV4                                                                                                                          AS GL_NAME_LV4,            
        NULL                                                                                                                                         AS GL_NAME_LV5,            
        NULL                                                                                                                                         AS GL_NAME_LV6,            
        A.CSC_T                                                                                                                                  AS GL_ATTR_CODE,           
        CASE WHEN B.GL_ATTR = '<CA>' THEN A.CSN 
                ELSE B.GL_ATTR END                                                                                                                   AS GL_ATTR,                
        NVL(B.CSN, A.C_KEY_NAME)                                                                             AS GL_FNAME,               
        A.C_SEC_CODE                                           AS SEC_CODE_FA,            
              NULL                                                                                                                                   AS SYMBOL_ORI,             
        NULL                                                                                                                                         AS SYMBOL,                 
        NULL                                                                                                                                         AS SEC_TYPE                          ,
       FROM_UNIXTIME(UNIX_TIMESTAMP(CURRENT_TIMESTAMP()),'yyyy-MM-dd HH:mm:ss')                    AS HLOADTIME,              
       '20190101'                                              AS HBATCHDATE              
       FROM (SELECT SUBSTR(T.CSC, 1, 4) AS GL_LV1,
                 T.* 
          FROM ODS.FVRV T
          WHERE T.D_BIZ IN (SELECT BD
                           FROM CTL.CFD
                          WHERE HDATASRC = 'A')
            AND T.HDATASRC = 'A'
         ) A
       LEFT JOIN TL B
          ON NVL(A.CSC_T, A.CSC) = B.CSC 
         AND A.CPC = B.CPC
       LEFT JOIN DW.PPCM C
          ON A.CPC = C.ORI_SYS_PC 
         AND C.ORI_SYS_HCODE = 'A' 
         AND A.D_BIZ BETWEEN C.STDATE AND C.ENDDATE
       LEFT JOIN DW.RCM E 
          ON A.CCC = E.ORI_SYS_CR_CODE 
         AND E.ORI_SYS_HCODE = 'A'
       LEFT JOIN DW.REM F 
          ON A.C_MKT_CODE = F.ORI_SYS_EXCH_CODE 
         AND F.ORI_SYS_HCODE = 'A' 
       LEFT JOIN (SELECT C_CA_CODE, MAX(C_CA_ATTR) AS C_CA_ATTR
                   FROM ODS.FVC
                  GROUP BY C_CA_CODE) H
          ON A.C_CA_CODE = H.C_CA_CODE

