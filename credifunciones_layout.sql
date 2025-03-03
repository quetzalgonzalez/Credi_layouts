---- Función de Amaya 

CREATE OR REPLACE FUNCTION `CDC_DWH_BI_BEC.nombre_funcion`(
  campos STRING, 
  param_fechaejecucion String, 
  param_dependencia String
  ) 
  RETURNS STRING AS ((FORMAT("""  
SELECT %s di.idcliente,
di.idcredito,
'ALTA' AS TIPO,
'A' as MOV,
CURRENT_DATE() as FECHASISTEMA,
  ca.periodo_sep as PERIODO 
FROM  
`RAW_DWH_BI.tblcreddictaminacion` di  
INNER JOIN `RAW_ZELL.catAfiliate` c On di.iddependenciaactual = c.vAfiliateId  
INNER JOIN `CDC_DWH_BI.view_calendar` ca  
ON c.iAfiliateId = ca.iddependencia AND ca.fechaejecucion ='%s' 
 
WHERE ca.iddependencia = '%s' 
AND di.producto NOT LIKE 'REFINANCIAMIENTO%%' 
AND di.estatuscredito = 'ACTIVO'  
AND di.fechacobro BETWEEN ca.fechainicial AND ca.fechafinal
""",
campos,CAST(param_fechaejecucion as STRING),param_dependencia))
); 


----- 3) Función altas_confianza


CREATE OR REPLACE FUNCTION `CDC_DWH_BI_BEC.fn_altas_confianza`(
  param_fechaejecucion STRING,
  param_dependencia STRING
) RETURNS STRING AS (
  FORMAT("""
    SELECT 
    di.idcliente, 
    di.idcredito, 
    'ALTA' 
    AS TIPO, 
    'A' AS MOV,
    CURRENT_DATE() AS FECHASISTEMA, 
    ca.periodo_sep AS PERIODO
    FROM `RAW_DWH_BI.tblcreddictaminacion` di
    INNER JOIN `RAW_ZELL.catAfiliate` c 
      ON di.iddependenciaactual = c.vAfiliateId
    INNER JOIN `CDC_DWH_BI.view_calendar` ca
    ON di.dependenciahomologado = ca2.dependencia
    AND ca2.fechaejecucion = '%s'
    
    LEFT JOIN `cfl-inf-ana-dev.RAW_ZELL.optCommonData` cc3 
      ON di.idSolicitud = CAST(cc3.iReferenceId AS STRING) 
      AND cc3.iDataId = 59
    LEFT JOIN `cfl-inf-ana-dev.RAW_ZELL.catDataOption` cdo3 
      ON cc3.iDataId = cdo3.iDataId 
      AND cc3.vValue = cdo3.vValue
    WHERE ca.iddependencia = '%s'
      AND di.producto NOT LIKE 'REFINANCIAMIENTO%%'
      AND di.estatuscredito = 'ACTIVO'
      AND di.fechacobro BETWEEN ca.fechainicial AND ca.fechafinal
      AND cdo3.vLabel = 'CONFIANZA'
  """, param_fechaejecucion, param_dependencia)
);


----- 4) Función altas_docentes

CREATE OR REPLACE FUNCTION `CDC_DWH_BI_BEC.fn_altas_docentes`(
  campos STRING,
  param_fechaejecucion STRING,
  param_dependencia STRING
) RETURNS STRING AS (
  FORMAT("""
    SELECT %s, di.idcliente,
           di.idcredito, 
           'ALTA' AS TIPO,
           'A' AS MOV,
           CURRENT_DATE() AS FECHASISTEMA,
           ca.periodo_sep AS PERIODO
    FROM `RAW_DWH_BI.tblcreddictaminacion` di
    INNER JOIN `RAW_ZELL.catAfiliate` c 
      ON di.iddependenciaactual = c.vAfiliateId
    INNER JOIN `CDC_DWH_BI.view_calendar` ca
      ON di.dependenciahomologado = ca.dependencia 
      AND ca.fechaejecucion = '%s'
    WHERE ca.iddependencia = '%s'
      AND di.producto NOT LIKE 'REFINANCIAMIENTO%%'
      AND di.estatuscredito = 'ACTIVO'
      AND di.fechacobro BETWEEN ca.fechainicial AND ca.fechafinal
  """, campos, param_fechaejecucion, param_dependencia)
);

----- 5) Función alta crédito

CREATE OR REPLACE FUNCTION `CDC_DWH_BI_BEC.fn_alta_credito`(param_dependencia STRING, param_fechaejecucion STRING, campos STRING)
RETURNS STRING AS (FORMAT("""    
SELECT %s di.idcliente,di.idcredito,    
"ALTA" AS TIPO,"A" as MOV,    
CURRENT_DATE() as FECHASISTEMA,
    ca.periodo_sep as PERIODO    
FROM `RAW_DWH_BI.tblcreddictaminacion` di    
INNER JOIN `RAW_ZELL.catAfiliate` c On di.iddependenciaactual = c.vAfiliateId    
INNER JOIN `CDC_DWH_BI.view_calendar` ca    
ON c.iAfiliateId = ca.iddependencia AND ca.fechaejecucion ='%s'    
AND di.dependenciahomologado = ca.dependencia    
AND di.fechacobro BETWEEN ca.fechainicial AND ca.fechafinal     
WHERE ca.iddependencia = '%s' AND di.producto NOT LIKE "REFINANCIAMIENTO%%" AND di.estatuscredito = "ACTIVO"  
""",
  campos, CAST(param_fechaejecucion AS STRING),param_dependencia
  )  
/*
  CONCAT('SELECT ',campos,', di.idcliente,di.idcredito, "ALTA" AS TIPO,"A" as MOV,CURRENT_DATE() as FECHASISTEMA, ca.periodo_sep as PERIODO FROM ',
'`cfl-inf-ana-dev.RAW_DWH_BI.tblcreddictaminacion` di INNER JOIN `cfl-inf-ana-dev.CDC_DWH_BI.view_calendar` ca ',
'ON di.dependenciahomologado = ca.dependencia AND ca.fechaejecucion = "',param_fechaejecucion,
'" AND di.fechacobro BETWEEN ca.fechainicial AND ca.fechafinal ',
'WHERE di.dependenciahomologado = "',param_dependencia,'" AND di.producto NOT LIKE "REFINANCIAMIENTO%" AND di.estatuscredito = "ACTIVO"' )
*/
);


---  6) Baja cliente    


CREATE OR REPLACE FUNCTION `CDC_DWH_BI_BEC.fn_baja_cliente`(
  param_dependencia STRING,
  param_fechaejecucion DATE,
  param_campos STRING
)
RETURNS ARRAY<STRING> AS (
  [    
FORMAT("""      
CREATE or REPLACE TABLE `CDC_DWH_BI.%s` AS      
SELECT * FROM `CDC_DWH_BI.%s`    
""",      
`CDC_DWH_BI.fn_tabla_layout`(param_dependencia, 'BAJA_CLIENTE'),      
`CDC_DWH_BI.fn_tabla_layout`(param_dependencia, 'BAJA')
    ),     
FORMAT("""      
CREATE TEMP TABLE TEMPORAL_BAJA AS      
SELECT 1 as idcliente      
UNION ALL      
SELECT DISTINCT al.idcliente      
FROM `CDC_DWH_BI.%s` al      
INNER JOIN `RAW_DWH_BI.tblcreddictaminacion` di        
ON al.idcliente = di.idcliente AND al.idcredito <> di.idcredito      
LEFT OUTER JOIN `RAW_ZELL.catAfiliate` c        
ON di.iddependenciaactual = c.vAfiliateId      
LEFT OUTER JOIN `cfl-inf-ana-dev.RAW_DWH_BI.dwh_catcalendario` cc        
ON c.iAfiliateId = cc.iddependencia      
WHERE di.EstatusCredito = 'ACTIVO'        
AND di.SaldoActual > 0;    
""",      
`CDC_DWH_BI.fn_tabla_layout`(param_dependencia, 'BAJA_CLIENTE')
    ),     
FORMAT("""      
DELETE FROM `CDC_DWH_BI.%s`      
WHERE idcliente IN (SELECT idcliente FROM TEMPORAL_BAJA)    
""",      
`CDC_DWH_BI.fn_tabla_layout`(param_dependencia, 'BAJA_CLIENTE')
    ),     
FORMAT("""      
SELECT * FROM `CDC_DWH_BI.%s`    
""",      
`CDC_DWH_BI.fn_tabla_layout`(param_dependencia, 'BAJA_CLIENTE')
    )
  ]
); 

---  7) Baja confianza 

CREATE OR REPLACE FUNCTION `CDC_DWH_BI_BEC.fn_baja_confianza`(param_dependencia STRING, param_fechaejecucion DATE, campos STRING) RETURNS STRING AS (
  FORMAT("""
  SELECT
    %s,
    di.idcliente,
    di.idcredito,
    'BAJA' AS TIPO,
    'B' AS MOV,
    CURRENT_DATE() AS FECHASISTEMA,
    ca.periodo_sep AS PERIODO
  FROM `RAW_DWH_BI.tblcreddictaminacion` di
    INNER JOIN `CDC_DWH_BI.view_calendar` ca 
      ON di.dependenciahomologado = ca.dependencia 
      AND ca.fechaejecucion = '%s'
    LEFT JOIN `RAW_DWH_BI.dwh_bisaldoscarteraf` sa 
      ON ca.periodo_trans = sa.periodo_actual 
      AND ca.dependencia = sa.dependencia 
      AND di.idcredito = sa.idcredito
    LEFT JOIN `cfl-inf-ana-dev.RAW_ZELL.optCommonData` cc3 
      ON CAST(di.idSolicitud AS STRING) = CAST(cc3.iReferenceId AS STRING) 
      AND cc3.iDataId = 59
    LEFT JOIN `cfl-inf-ana-dev.RAW_ZELL.catDataOption` cdo3 
      ON cc3.iDataId = cdo3.iDataId 
      AND cc3.vValue = cdo3.vValue
  WHERE ca.iddependencia = '%s'
    AND cdo3.vLabel = 'CONFIANZA' 
    AND (di.saldoactual - di.montoentransito) <= 0
    AND di.creditorefinanciamiento = 0
    AND IFNULL(sa.saldoactual, 0) > 0;
  """, 
  campos, CAST(param_fechaejecucion AS STRING), param_dependencia)
);


----8) Baja docentes
CREATE OR REPLACE FUNCTION `CDC_DWH_BI_BEC.fn_baja_docente`(campos STRING, param_fechaejecucion String, param_dependencia String) RETURNS STRING AS ( FORMAT(""" SELECT %s, di.idcliente, di.idcredito, 'BAJA' AS TIPO, 'B' AS MOV, CURRENT_DATE() AS FECHASISTEMA, ca.periodo_sep AS PERIODO FROM `RAW_DWH_BI.tblcreddictaminacion` di INNER JOIN `CDC_DWH_BI.view_calendar` ca ON c.iAfiliateId = ca.iddependencia AND ca.fechaejecucion ='%s' INNER JOIN `RAW_ZELL.catAfiliate` c On di.iddependenciaactual = c.vAfiliateId LEFT JOIN `RAW_DWH_BI.dwh_bisaldoscarteraf` sa ON ca.periodo_trans = sa.periodo_actual AND ca.dependencia = sa.dependencia AND di.idcredito = sa.idcredito LEFT JOIN `cfl-inf-ana-dev.RAW_ZELL.optCommonData` cc3 ON CAST(di.idSolicitud AS STRING) = CAST(cc3.iReferenceId AS STRING) AND cc3.iDataId = 59 LEFT JOIN `cfl-inf-ana-dev.RAW_ZELL.catDataOption` cdo3 ON cc3.iDataId = cdo3.iDataId AND cc3.vValue = cdo3.vValue WHERE ca.iddependencia = '%s' AND cdo3.vLabel = 'DOCENTE' AND (di.saldoactual - di.montoentransito) <= 0 AND di.creditorefinanciamiento = 0 AND IFNULL(sa.saldoactual, 0) > 0; """, campos, CAST(param_fechaejecucion AS STRING), param_dependencia));

-----
CREATE OR REPLACE FUNCTION `CDC_DWH_BI_BEC.fn_baja_docente`(
  campos STRING, 
  param_fechaejecucion STRING, 
  param_dependencia STRING
) 
RETURNS STRING AS (
  FORMAT("""
    SELECT 
      %s,
      di.idcliente, 
      di.idcredito, 
      'BAJA' AS TIPO, 
      'B' AS MOV, 
      CURRENT_DATE() AS FECHASISTEMA, 
      ca.periodo_sep AS PERIODO
    FROM `RAW_DWH_BI.tblcreddictaminacion` di
    INNER JOIN `CDC_DWH_BI.view_calendar` ca 
      ON c.iAfiliateId = ca.iddependencia 
      AND ca.fechaejecucion = '%s'
    INNER JOIN `RAW_ZELL.catAfiliate` c 
      ON di.iddependenciaactual = c.vAfiliateId  
    LEFT JOIN `RAW_DWH_BI.dwh_bisaldoscarteraf` sa 
      ON ca.periodo_trans = sa.periodo_actual 
      AND ca.dependencia = sa.dependencia 
      AND di.idcredito = sa.idcredito
    LEFT JOIN `cfl-inf-ana-dev.RAW_ZELL.optCommonData` cc3 
      ON CAST(di.idSolicitud AS STRING) = CAST(cc3.iReferenceId AS STRING) 
      AND cc3.iDataId = 59
    LEFT JOIN `cfl-inf-ana-dev.RAW_ZELL.catDataOption` cdo3 
      ON cc3.iDataId = cdo3.iDataId 
      AND cc3.vValue = cdo3.vValue
    WHERE ca.iddependencia = '%s'
      AND cdo3.vLabel = 'DOCENTE'
      AND (di.saldoactual - di.montoentransito) <= 0
      AND di.creditorefinanciamiento = 0
      AND IFNULL(sa.saldoactual, 0) > 0;
    """,
    campos, 
    CAST(param_fechaejecucion AS STRING), 
    param_dependencia
  )
);




---9) Cambio baja
CREATE OR REPLACE FUNCTION `CDC_DWH_BI_BEC.fn_cambio_baja`(param_campos STRING, param_fechaejecucion String, param_dependencia String , campos_agrupados STRING) RETURNS STRING AS ( (FORMAT(""" SELECT %s di.idcliente, max(di.idcredito) as idcredito, "CAMBIO BAJA" AS TIPO, "CB" AS MOV, a.FECHASISTEMA, a.PERIODO FROM `RAW_DWH_BI.tblcreddictaminacion` di inner JOIN `CDC_DWH_BI.%s` a on di.idcliente = a.idcliente WHERE di.estatuscredito = 'ACTIVO' AND di.saldoactual > 0 GROUP BY %s di.idcliente, a.FECHASISTEMA, a.PERIODO ; """, param_campos,`CDC_DWH_BI.fn_tabla_layout`(param_dependencia,'baja'),campos_agrupados) ));
 ---
CREATE OR REPLACE FUNCTION `CDC_DWH_BI_BEC.fn_cambio_baja`(
  param_campos STRING, 
  param_fechaejecucion STRING, 
  param_dependencia STRING, 
  campos_agrupados STRING
) 
RETURNS STRING AS (
  FORMAT("""
    SELECT 
      %s,
      di.idcliente, 
      MAX(di.idcredito) AS idcredito, 
      'CAMBIO BAJA' AS TIPO, 
      'CB' AS MOV, 
      a.FECHASISTEMA, 
      a.PERIODO
    FROM `RAW_DWH_BI.tblcreddictaminacion` di
    INNER JOIN `CDC_DWH_BI.%s` a 
      ON di.idcliente = a.idcliente
    WHERE di.estatuscredito = 'ACTIVO' 
      AND di.saldoactual > 0
    GROUP BY %s, di.idcliente, a.FECHASISTEMA, a.PERIODO;
    """,
    param_campos,
    `CDC_DWH_BI.fn_tabla_layout`(param_dependencia, 'baja'),
    campos_agrupados
  )
);


---10) Cambio alta
CREATE OR REPLACE FUNCTION `CDC_DWH_BI_BEC.fn_cambio_alta`(param_dependencia STRING, param_fechaejecucion DATE, campos STRING, campos_agrupados STRING) RETURNS STRING AS (
 (FORMAT("""  
SELECT
    %s
    di.idcliente,    
max(di.idcredito) as idcredito,    
"CAMBIO ALTA" AS TIPO,    
"CA" AS MOV,
    a.FECHASISTEMA,
    a.PERIODO    
FROM `cfl-inf-ana-dev.RAW_DWH_BI.tblcreddictaminacion` di inner JOIN `CDC_DWH_BI.%s` a on di.idcliente = a.idcliente    
WHERE di.estatuscredito = 'ACTIVO' AND di.saldoactual > 0    
GROUP BY
    %s
    di.idcliente,
    a.FECHASISTEMA,
    a.PERIODO;
""",
campos,`CDC_DWH_BI.fn_tabla_layout`(param_dependencia,'alta'),campos_agrupados
) 
)
);

---- 11) Baja Credito
CREATE OR REPLACE FUNCTION `CDC_DWH_BI_BEC.baja_credito`(
  campos STRING, 
  param_fechaejecucion STRING, 
  param_dependencia STRING
) 
RETURNS STRING AS (
  FORMAT("""
    SELECT
      %s,
      "BAJA" AS TIPO,
      "B" AS MOV, 
      CURRENT_DATE() AS FECHASISTEMA,
      ca.periodo_sep AS PERIODO
    FROM `CDC_DWH_BI.view_calendar` ca
    LEFT JOIN `RAW_DWH_BI.dwh_bisaldoscarteraf` sa 
      ON ca.periodo_trans = sa.periodo_actual 
      AND ca.dependencia = sa.dependencia
    LEFT JOIN `RAW_DWH_BI.tblcreddictaminacion` di 
      ON sa.idcredito = di.idcredito
    INNER JOIN `RAW_ZELL.catAfiliate` c 
      ON di.iddependenciaactual = c.vAfiliateId
    WHERE (di.saldoactual - di.montoentransito) <= 0
      AND di.creditorefinanciamiento = 0
      AND ca.fechaejecucion = '%s'
      AND ca.dependencia = '%s'
      AND IFNULL(sa.saldoactual, 0) > 0;
    """,
    campos, 
    CAST(param_fechaejecucion AS STRING), 
    param_dependencia
  )
);


