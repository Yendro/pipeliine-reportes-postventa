--Ultima modificacion: Luis Rayas 12/11/2025, se agrega la fecha de cracion de la tabla de ingresos, 
-- esto para poder ordenar por fecha de creación en manivela
--REPORTE CON TODO LOS INGRESOS DE TODOS LOS DESARROLLOS Y EN TODOS LOS ESTATUS (Proceso, Aprobado juridico, direcicon ventas, etc)

--CREATE OR REPLACE VIEW `FlujodeCaja.FlujodeCaja_Completa` AS  
WITH VENTASCOMPLETAS AS( 
(
--TERRAVIVAw
 WITH ingresos_terraviva AS (
    WITH temp_nombres_asesor AS (
        SELECT  
            id_usuario,
            -- Asesor   
            TRIM(CONCAT(
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(NombreAsesor), ' ')) AS word), ' '), ' ', 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_paterno, '-', ''), '.', '')), ' ')) AS word), ' '), ' ', 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_materno, '-', ''), '.', '')), ' ')) AS word), ' ')))
            AS Asesor
        FROM EXTERNAL_QUERY("terraviva-439415.us.terraviva", "SELECT u.id_usuario, u.nombre AS NombreAsesor, u.apellido_paterno, u.apellido_materno FROM usuario AS u")
    ),
    temp_nombres_clientes AS (
        SELECT  
            id_cliente,
            -- Cliente
            TRIM(CONCAT( 
                TRIM(ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(nombre, ' ')) AS word), ' ')), ' ', 
                TRIM(ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_p, '-', ''), '.', '')), ' ')) AS word), ' ')), ' ', 
                TRIM(ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_m, '-', ''), '.', '')), ' ')) AS word), ' '))))
            AS Cliente
        FROM EXTERNAL_QUERY("terraviva-439415.us.terraviva", "SELECT c.id_cliente, c.nombre, c.apellido_p, c.apellido_m FROM cliente AS c")
    ),
    temp_desarrollos_marcas AS (
        SELECT
            id_desarrollo,
            nombre_desarrollo
        FROM EXTERNAL_QUERY("terraviva-439415.us.terraviva", "SELECT d.id_desarrollo, d.nombre_desarrollo FROM desarrollo AS d")
    ),
    estatusventas AS (
        SELECT
            id_status,
            nombre
        FROM EXTERNAL_QUERY("terraviva-439415.us.terraviva", "SELECT id_status, nombre FROM status_venta")
    ),
    temp_unidades AS (
        SELECT
            id_unidad,
            id_desarrollo,
            -- Unidad, Modelo, M2, PrecioM2, PrecioVenta   
            --'Regular' AS  Modelo,
            modelo AS Modelo,
            numero_unidad AS Unidad,
            metros_cuadrados_totales AS M2, 
            precio_metros_cuadrados,
            privada, 
            referencia_banco,
            numero_etapa AS Etapa
        FROM EXTERNAL_QUERY("terraviva-439415.us.terraviva", "SELECT uni.id_desarrollo,uni.referencia_banco, uni.privada, uni.numero_etapa, uni.id_unidad, uni.numero_unidad, uni.modelo, uni.metros_cuadrados_totales, uni.precio_metros_cuadrados FROM unidades AS uni")
    ),
    temp_ventas AS (
        SELECT
            id_venta,
            id_usuario,
            id_cliente, 
            id_unidad,
            precio_venta AS PrecioVenta,  
            --Proceso  
            fecha_venta AS Proceso, 
            numero_acciones,
            aportacion_accion AS PU_Capital,
            aportacion_prim_accion AS PU_Prima,
            aportacion_accion_total AS Capital,
            aportacion_prim_accion_total AS Prima,
            total_pagado,
            saldo_total,
            numero_pagos,
            --Finalizado
            fecha_carga_contrato,
            status_venta,
            fecha_cierre_venta AS Finalizado,
            cuentaBeneficiario


        FROM EXTERNAL_QUERY("terraviva-439415.us.terraviva", "SELECT v.id_venta, NULLIF(v.fecha_cierre_venta, '0000-00-00') AS fecha_cierre_venta, v.id_unidad, stp.cuentaBeneficiario, v.id_usuario, v.id_cliente, v.precio_venta, v.fecha_venta, v.numero_acciones, v.aportacion_accion, v.aportacion_prim_accion, v.status_venta, v.aportacion_accion_total, v.aportacion_prim_accion_total, v.total_pagado, v.status_venta AS status, v.saldo_total, v.numero_pagos, NULLIF(v.fecha_carga_contrato, '0000-00-00') AS fecha_carga_contrato FROM venta AS v LEFT JOIN (SELECT id_venta, cuentaBeneficiario FROM stp_bitacora WHERE status = 1 GROUP BY id_venta) AS stp ON v.id_venta = stp.id_venta")

    ),
    temp_normalizacion_nombre AS (
        SELECT 
            -- Asesor
            id_usuario,
            TRIM(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    REPLACE(Asesor, 'Merida', ''), 'Miami', ''), 'Cdmx', ''), 'Dam', ''), 'Interno', ''), 'Externo', '')) AS Asesor
        FROM temp_nombres_asesor
    ),
    temp_ingreso AS(
        SELECT
            id_venta,
            id_ingreso,
            DATE(NULLIF(fecha_ingreso, '0000-00-00')) AS fecha_ingreso,
            DATE(NULLIF(fecha_creacion, '0000-00-00')) AS fecha_creacion,
            monto_ingresado AS Cantidad,
            TRIM(ARRAY_TO_STRING(ARRAY( SELECT  INITCAP(word) FROM UNNEST(SPLIT(forma_de_pago, ' ')) AS word), ' '))
            AS forma_de_pago,
            TRIM(ARRAY_TO_STRING(ARRAY( SELECT  INITCAP(word) FROM UNNEST(SPLIT(concepto, ' ')) AS word), ' '))
            AS concepto,
            folio_seguimiento,
            clave_ingreso,
            nombre_banco,
            fecha_aprobacion
        FROM EXTERNAL_QUERY("terraviva-439415.us.terraviva", "SELECT i.id_venta, i.id_ingreso, NULLIF(i.fecha_ingreso, '0000-00-00') AS fecha_ingreso, NULLIF(i.fecha_creacion, '0000-00-00') AS fecha_creacion,i.id_banco, i.folio_seguimiento, b.nombre_banco, i.clave_ingreso, i.monto_ingresado,  i.id_forma_pago, i.concepto, i.status, fp.nombre AS forma_de_pago, NULLIF(i.fecha_aprobacion, '0000-00-00') AS fecha_aprobacion FROM ingreso AS i  LEFT JOIN forma_pago AS fp ON i.id_forma_pago = fp.id_forma_pago  LEFT JOIN banco AS b ON i.id_banco = b.id_banco WHERE i.status =1") WHERE fecha_aprobacion IS NOT NULL
    )
    -- Consulta final
    SELECT
        tv.id_venta,
        CONCAT (tdm.nombre_desarrollo, ' ', Unidad) AS id,
        va.Marca,
        va.Desarrollo,
        tu.Privada,
        tu.referencia_banco,
        tu.Etapa,
        tu.Unidad,  
        tu.Modelo,
        tu.M2,
        tu.precio_metros_cuadrados AS PrecioM2,
        tv.PrecioVenta,
        tnn.Asesor,
        ts.Sucursal,
        ts.Tipo,
        ts.Equipo,
        tnc.Cliente,
        tv.cuentaBeneficiario,
        sv.nombre AS Estatus,
        ti.id_ingreso,
        ti.fecha_ingreso,
        ti.fecha_creacion,
        ti.Cantidad,
        ti.forma_de_pago,
        ti.concepto,
        ti.folio_seguimiento, 
        ti.clave_ingreso,
        ti.nombre_banco,


    FROM temp_ingreso AS ti 
    LEFT JOIN temp_ventas AS tv ON tv.id_venta = ti.id_venta
    LEFT JOIN temp_nombres_clientes AS tnc ON tv.id_cliente = tnc.id_cliente
    LEFT JOIN temp_unidades AS tu ON tv.id_unidad = tu.id_unidad 
    LEFT JOIN temp_normalizacion_nombre AS tnn ON tnn.id_usuario = tv.id_usuario  
    LEFT JOIN temp_desarrollos_marcas AS tdm ON tdm.id_desarrollo = tu.id_desarrollo   
    LEFT JOIN estatusventas AS sv ON sv.id_status = tv.status_venta
    LEFT JOIN `Dimensiones.NombreDesarrollo` AS va ON va.id_nombre_desarrollo = tdm.nombre_desarrollo 
    LEFT JOIN `Dimensiones.NombresVendedores` AS ts ON ts.Vendedor = tnn.Asesor  
 )SELECT * FROM ingresos_terraviva ORDER BY id_venta
-- ;
)UNION ALL( 

--Dam 
    WITH temp_nombres_asesor AS (
        SELECT  
            id_usuario,
            -- Asesor   
            TRIM(CONCAT(
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(NombreAsesor), ' ')) AS word), ' '), ' ', 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_paterno, '-', ''), '.', '')), ' ')) AS word), ' '), ' ', 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_materno, '-', ''), '.', '')), ' ')) AS word), ' ')))
            AS Asesor
        FROM EXTERNAL_QUERY("terraviva-439415.us.dam", "SELECT u.id_usuario, u.nombre AS NombreAsesor, u.apellido_paterno, u.apellido_materno FROM usuario AS u")
    ),
    temp_nombres_clientes AS (
        SELECT  
            id_cliente,
            -- Cliente
            TRIM(CONCAT( 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(nombre, ' ')) AS word), ' '), ' ', 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_p, '-', ''), '.', '')), ' ')) AS word), ' '), ' ', 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_m, '-', ''), '.', '')), ' ')) AS word), ' ')))
            AS Cliente
        FROM EXTERNAL_QUERY("terraviva-439415.us.dam", "SELECT c.id_cliente, c.nombre, c.apellido_p, c.apellido_m FROM cliente AS c")
    ),
    temp_desarrollos_marcas AS (
        SELECT
            id_desarrollo,
            nombre_desarrollo
        FROM EXTERNAL_QUERY("terraviva-439415.us.dam", "SELECT d.id_desarrollo, d.nombre_desarrollo FROM desarrollo AS d")
    ),
    estatusventas AS (
        SELECT
            id_status,
            nombre
        FROM EXTERNAL_QUERY("terraviva-439415.us.dam", "SELECT id_status, nombre FROM status_venta")
    ),
    temp_unidades AS (
        SELECT
            id_unidad,
            id_desarrollo,
            -- Unidad, Modelo, M2, PrecioM2, PrecioVenta   
            --'Regular' AS  Modelo,
            modelo AS Modelo,
            numero_unidad AS Unidad,
            metros_cuadrados_totales AS M2, 
            precio_metros_cuadrados,
            privada, 
            referencia_banco,
            numero_etapa AS Etapa
        FROM EXTERNAL_QUERY("terraviva-439415.us.dam", "SELECT uni.id_desarrollo, uni.privada,uni.referencia_banco, uni.numero_etapa, uni.id_unidad, uni.numero_unidad, uni.modelo, uni.metros_cuadrados_totales, uni.precio_metros_cuadrados FROM unidades AS uni")
    ),
    temp_ventas AS (
        SELECT
            id_venta,
            id_usuario,
            id_cliente, 
            id_unidad,
            precio_venta AS PrecioVenta,  
            --Proceso  
            fecha_venta AS Proceso, 
            numero_acciones,
            aportacion_accion AS PU_Capital,
            aportacion_prim_accion AS PU_Prima,
            aportacion_accion_total AS Capital,
            aportacion_prim_accion_total AS Prima,
            total_pagado,
            saldo_total,
            numero_pagos,
            --Finalizado
            fecha_carga_contrato,
            status_venta,
            fecha_cierre_venta AS Finalizado,
            cuentaBeneficiario

        FROM EXTERNAL_QUERY("terraviva-439415.us.dam", "SELECT v.id_venta, v.id_unidad, NULLIF(v.fecha_cierre_venta, '0000-00-00') AS fecha_cierre_venta, stp.cuentaBeneficiario, v.id_usuario, v.id_cliente, v.precio_venta, v.fecha_venta, v.numero_acciones, v.aportacion_accion, v.aportacion_prim_accion, v.status_venta, v.aportacion_accion_total, v.aportacion_prim_accion_total, v.total_pagado, v.status_venta AS status, v.saldo_total, v.numero_pagos, NULLIF(v.fecha_carga_contrato, '0000-00-00') AS fecha_carga_contrato FROM venta AS v LEFT JOIN (SELECT id_venta, cuentaBeneficiario FROM stp_bitacora WHERE status = 1 GROUP BY id_venta) AS stp ON v.id_venta = stp.id_venta ")

    ),
    temp_normalizacion_nombre AS (
        SELECT 
            -- Asesor
            id_usuario,
            TRIM(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    REPLACE(Asesor, 'Merida', ''), 'Miami', ''), 'Cdmx', ''), 'Dam', ''), 'Interno', ''), 'Externo', '')) AS Asesor
        FROM temp_nombres_asesor
    ),
    temp_ingreso AS(
        SELECT
            id_venta,
            id_ingreso,
            DATE(NULLIF(fecha_ingreso, '0000-00-00')) AS fecha_ingreso,
            DATE(NULLIF(fecha_creacion, '0000-00-00')) AS fecha_creacion,
            monto_ingresado AS Cantidad,
            TRIM(ARRAY_TO_STRING(ARRAY( SELECT  INITCAP(word) FROM UNNEST(SPLIT(forma_de_pago, ' ')) AS word), ' '))
            AS forma_de_pago,
            TRIM(ARRAY_TO_STRING(ARRAY( SELECT  INITCAP(word) FROM UNNEST(SPLIT(concepto, ' ')) AS word), ' '))
            AS concepto,
            folio_seguimiento,
            clave_ingreso,
            nombre_banco,
            fecha_aprobacion
        FROM EXTERNAL_QUERY("terraviva-439415.us.dam", "SELECT i.id_venta, i.id_ingreso, NULLIF(i.fecha_ingreso, '0000-00-00') AS fecha_ingreso,NULLIF(i.fecha_creacion, '0000-00-00') AS fecha_creacion, i.id_banco, i.folio_seguimiento, b.nombre_banco, i.clave_ingreso, i.monto_ingresado,  i.id_forma_pago, i.concepto, i.status, fp.nombre AS forma_de_pago, NULLIF(i.fecha_aprobacion, '0000-00-00') AS fecha_aprobacion FROM ingreso AS i  LEFT JOIN forma_pago AS fp ON i.id_forma_pago = fp.id_forma_pago  LEFT JOIN banco AS b ON i.id_banco = b.id_banco WHERE i.status =1") WHERE fecha_aprobacion IS NOT NULL
    )
    -- Consulta final
    SELECT
        tv.id_venta,
        CONCAT (tdm.nombre_desarrollo, ' ', Unidad) AS id,
        va.Marca,
        va.Desarrollo,
        tu.Privada,
        tu.referencia_banco,
        tu.Etapa,
        tu.Unidad,  
        CASE
            WHEN va.Desarrollo = 'Parque Pimienta' THEN 'Accion'
            WHEN va.Desarrollo = 'Playaviva Apartments' THEN 'Unidad'
            WHEN va.Desarrollo = 'Business Center' THEN 'Unidad'
            WHEN va.Desarrollo = 'Centro Corporativo' THEN 'Oficina'
            ELSE NULL 
        END AS Modelo,
        CASE 
            WHEN va.Desarrollo = 'Centro Corporativo' THEN tu.M2
            WHEN va.Desarrollo = 'Parque Pimienta' THEN tv.numero_acciones
            WHEN va.Desarrollo = 'Business Center' THEN tv.numero_acciones
            WHEN va.Desarrollo = 'Playaviva Apartments' THEN tv.numero_acciones
            ELSE NULL 
        END AS M2,
        COALESCE(
            SAFE_DIVIDE(
                tv.PrecioVenta,
                CASE 
                    WHEN va.Desarrollo = 'Centro Corporativo' AND tu.M2 > 0 THEN tu.M2
                    WHEN va.Desarrollo IN ('Parque Pimienta', 'Business Center', 'Playaviva Apartments') 
                        AND tv.numero_acciones > 0 THEN tv.numero_acciones
                    ELSE NULL 
                END
            ),
            0
        ) AS PrecioM2,
        tv.PrecioVenta,
        tnn.Asesor,
        ts.Sucursal,
        ts.Tipo,
        ts.Equipo,
        tnc.Cliente,
        tv.cuentaBeneficiario,
        sv.nombre AS Estatus,
        ti.id_ingreso,
        ti.fecha_ingreso,
        ti.fecha_creacion,
        ti.Cantidad,
        ti.forma_de_pago,
        ti.concepto,
        ti.folio_seguimiento, 
        ti.clave_ingreso,
        ti.nombre_banco

    FROM temp_ingreso AS ti
    LEFT JOIN temp_ventas AS tv ON tv.id_venta = ti.id_venta
    LEFT JOIN temp_nombres_asesor AS tna ON tv.id_usuario = tna.id_usuario
    LEFT JOIN temp_nombres_clientes AS tnc ON tv.id_cliente = tnc.id_cliente
    LEFT JOIN temp_unidades AS tu ON tv.id_unidad = tu.id_unidad
    LEFT JOIN temp_desarrollos_marcas AS tdm ON tdm.id_desarrollo = tu.id_desarrollo
    LEFT JOIN temp_normalizacion_nombre AS tnn ON tnn.id_usuario = tv.id_usuario  
    LEFT JOIN estatusventas AS sv ON sv.id_status = tv.status_venta
    LEFT JOIN `Dimensiones.NombreDesarrollo` AS va ON va.id_nombre_desarrollo = tdm.nombre_desarrollo 
    LEFT JOIN `Dimensiones.NombresVendedores` AS ts ON ts.Vendedor = tnn.Asesor 
  

-- ;
)UNION ALL( 

--Custo 
    WITH temp_ingreso AS(
        SELECT
            id_venta,
            id_ingreso,
            DATE(NULLIF(fecha_ingreso, '0000-00-00')) AS fecha_ingreso,
            DATE(NULLIF(fecha_creacion, '0000-00-00')) AS fecha_creacion,
            monto_ingresado AS Cantidad,
            TRIM(ARRAY_TO_STRING(ARRAY( SELECT  INITCAP(word) FROM UNNEST(SPLIT(forma_de_pago, ' ')) AS word), ' '))
            AS forma_de_pago,
            TRIM(ARRAY_TO_STRING(ARRAY( SELECT  INITCAP(word) FROM UNNEST(SPLIT(concepto, ' ')) AS word), ' '))
            AS concepto,
            folio_seguimiento,
            clave_ingreso,
            nombre_banco,
            fecha_aprobacion
        FROM EXTERNAL_QUERY("terraviva-439415.us.custo", "SELECT i.id_venta, i.id_ingreso, NULLIF(i.fecha_ingreso, '0000-00-00') AS fecha_ingreso,NULLIF(i.fecha_creacion, '0000-00-00') AS fecha_creacion, i.id_banco, i.folio_seguimiento, b.nombre_banco, i.clave_ingreso, i.monto_ingresado,  i.id_forma_pago, i.concepto, i.status, fp.nombre AS forma_de_pago, NULLIF(i.fecha_aprobacion, '0000-00-00') AS fecha_aprobacion FROM ingreso AS i  LEFT JOIN forma_pago AS fp ON i.id_forma_pago = fp.id_forma_pago  LEFT JOIN banco AS b ON i.id_banco = b.id_banco WHERE i.status =1") WHERE fecha_aprobacion IS NOT NULL
    ),
    temp_nombres_asesor AS (
        SELECT  
            id_usuario,
            -- Asesor   
            TRIM(CONCAT(
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(NombreAsesor), ' ')) AS word), ' '), ' ', 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_paterno, '-', ''), '.', '')), ' ')) AS word), ' '), ' ', 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_materno, '-', ''), '.', '')), ' ')) AS word), ' ')))
            AS Asesor
        FROM EXTERNAL_QUERY("terraviva-439415.us.custo", "SELECT u.id_usuario, u.nombre AS NombreAsesor, u.apellido_paterno, u.apellido_materno FROM usuario AS u")
    ),
    temp_nombres_clientes AS (
        SELECT  
            id_cliente,
            -- Cliente
            TRIM(CONCAT( 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(nombre, ' ')) AS word), ' '), ' ', 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_p, '-', ''), '.', '')), ' ')) AS word), ' '), ' ', 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_m, '-', ''), '.', '')), ' ')) AS word), ' ')))
            AS Cliente
        FROM EXTERNAL_QUERY("terraviva-439415.us.custo", "SELECT c.id_cliente, c.nombre, c.apellido_p, c.apellido_m FROM cliente AS c")
    ),
    temp_desarrollos_marcas AS (
        SELECT
            id_desarrollo,
            nombre_desarrollo
        FROM EXTERNAL_QUERY("terraviva-439415.us.custo", "SELECT d.id_desarrollo, d.nombre_desarrollo FROM desarrollo AS d")
    ),
    estatusventas AS (
        SELECT
            id_status,
            nombre
        FROM EXTERNAL_QUERY("terraviva-439415.us.custo", "SELECT id_status, nombre FROM status_venta")
    ),
    temp_unidades AS (
        SELECT
            id_unidad,
            id_desarrollo,
            -- Unidad, Modelo, M2, PrecioM2, PrecioVenta   
            --'Regular' AS  Modelo,
            modelo AS Modelo,
            numero_unidad AS Unidad,
            metros_cuadrados_totales AS M2, 
            precio_metros_cuadrados,
            privada, 
            referencia_banco,
            numero_etapa AS Etapa
        FROM EXTERNAL_QUERY("terraviva-439415.us.custo", "SELECT uni.id_desarrollo, uni.privada, uni.referencia_banco,uni.numero_etapa, uni.id_unidad, uni.numero_unidad, uni.modelo, uni.metros_cuadrados_totales, uni.precio_metros_cuadrados FROM unidades AS uni")
    ),
    temp_ventas AS (
        SELECT
            id_venta,
            id_usuario,
            id_cliente, 
            id_unidad,
            precio_venta AS PrecioVenta,  
            --Proceso  
            fecha_venta AS Proceso, 
            numero_acciones,
            aportacion_accion AS PU_Capital,
            aportacion_prim_accion AS PU_Prima,
            aportacion_accion_total AS Capital,
            aportacion_prim_accion_total AS Prima,
            total_pagado,
            saldo_total,
            numero_pagos,
            --Finalizado
            fecha_carga_contrato,
            status_venta,
            fecha_cierre_venta AS Finalizado,
            cuentaBeneficiario

        FROM EXTERNAL_QUERY("terraviva-439415.us.custo", "SELECT v.id_venta, NULLIF(v.fecha_cierre_venta, '0000-00-00') AS fecha_cierre_venta, stp.cuentaBeneficiario, v.id_unidad, v.id_usuario, v.id_cliente, v.precio_venta, v.fecha_venta, v.numero_acciones, v.aportacion_accion, v.aportacion_prim_accion, v.status_venta, v.aportacion_accion_total, v.aportacion_prim_accion_total, v.total_pagado, v.status_venta AS status, v.saldo_total, v.numero_pagos, NULLIF(v.fecha_carga_contrato, '0000-00-00') AS fecha_carga_contrato FROM venta AS v LEFT JOIN (SELECT id_venta, cuentaBeneficiario FROM stp_bitacora WHERE status = 1 GROUP BY id_venta) AS stp ON v.id_venta = stp.id_venta")

    ),
    temp_fecha_ingreso AS (
        SELECT
            DISTINCT id_venta,   
            MAX(NULLIF(fecha_ingreso, '0000-00-00')) OVER(PARTITION BY id_venta) AS fecha_ingreso
        FROM EXTERNAL_QUERY("terraviva-439415.us.custo", "SELECT id_venta, NULLIF(fecha_ingreso, '0000-00-00') AS fecha_ingreso FROM ingreso")
    ), 
    temp_normalizacion_nombre AS (
        SELECT 
            -- Asesor
            id_usuario,
            TRIM(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    REPLACE(Asesor, 'Merida', ''), 'Miami', ''), 'Cdmx', ''), 'Dam', ''), 'Interno', ''), 'Externo', '')) AS Asesor
        FROM temp_nombres_asesor
    )
    -- Consulta final
    SELECT
        tv.id_venta,
        CONCAT (tdm.nombre_desarrollo, ' ', Unidad) AS id,
        va.Marca,
        va.Desarrollo,
        tu.Privada,
        tu.referencia_banco,
        tu.Etapa,
        tu.Unidad,  
        tu.Modelo,
        tu.M2,
        tu.precio_metros_cuadrados AS PrecioM2,
        tv.PrecioVenta,
        tnn.Asesor,
        ts.Sucursal,
        ts.Tipo,
        ts.Equipo,
        tnc.Cliente,
        tv.cuentaBeneficiario,
        sv.nombre AS Estatus,
        ti.id_ingreso,
        ti.fecha_ingreso,
        ti.fecha_creacion,
        ti.Cantidad,
        ti.forma_de_pago,
        ti.concepto,
        ti.folio_seguimiento, 
        ti.clave_ingreso,
        ti.nombre_banco

    FROM temp_ingreso AS ti
    LEFT JOIN temp_ventas AS tv ON tv.id_venta = ti.id_venta
    LEFT JOIN temp_nombres_asesor AS tna ON tv.id_usuario = tna.id_usuario
    LEFT JOIN temp_nombres_clientes AS tnc ON tv.id_cliente = tnc.id_cliente
    LEFT JOIN temp_unidades AS tu ON tv.id_unidad = tu.id_unidad
    LEFT JOIN temp_desarrollos_marcas AS tdm ON tdm.id_desarrollo = tu.id_desarrollo
    LEFT JOIN temp_normalizacion_nombre AS tnn ON tnn.id_usuario = tv.id_usuario  
    LEFT JOIN temp_fecha_ingreso AS tfi ON tfi.id_venta = tv.id_venta
    LEFT JOIN estatusventas AS sv ON sv.id_status = tv.status_venta
    LEFT JOIN `Dimensiones.NombreDesarrollo` AS va ON va.id_nombre_desarrollo = tdm.nombre_desarrollo 
    LEFT JOIN `Dimensiones.NombresVendedores` AS ts ON ts.Vendedor = tnn.Asesor 
-- ;
)UNION ALL(

-- Almaviva 
    WITH temp_ingreso AS(
        SELECT
            id_venta,
            id_ingreso,
            DATE(NULLIF(fecha_ingreso, '0000-00-00')) AS fecha_ingreso,
            DATE(NULLIF(fecha_creacion, '0000-00-00')) AS fecha_creacion,
            monto_ingresado AS Cantidad,
            TRIM(ARRAY_TO_STRING(ARRAY( SELECT  INITCAP(word) FROM UNNEST(SPLIT(forma_de_pago, ' ')) AS word), ' '))
            AS forma_de_pago,
            TRIM(ARRAY_TO_STRING(ARRAY( SELECT  INITCAP(word) FROM UNNEST(SPLIT(concepto, ' ')) AS word), ' '))
            AS concepto,
            folio_seguimiento,
            clave_ingreso,
            nombre_banco,
            fecha_aprobacion
        FROM EXTERNAL_QUERY("terraviva-439415.us.bq_almaviva", "SELECT i.id_venta, i.id_ingreso, NULLIF(i.fecha_ingreso, '0000-00-00') AS fecha_ingreso, NULLIF(i.fecha_creacion, '0000-00-00') AS fecha_creacion,i.id_banco, i.folio_seguimiento, b.nombre_banco, i.clave_ingreso, i.monto_ingresado,  i.id_forma_pago, i.concepto, i.status, fp.nombre AS forma_de_pago, NULLIF(i.fecha_aprobacion, '0000-00-00') AS fecha_aprobacion FROM ingreso AS i  LEFT JOIN forma_pago AS fp ON i.id_forma_pago = fp.id_forma_pago  LEFT JOIN banco AS b ON i.id_banco = b.id_banco WHERE i.status =1") WHERE fecha_aprobacion IS NOT NULL
    ),
    temp_nombres_asesor AS (
        SELECT  
            id_usuario,
            -- Asesor   
            TRIM(CONCAT(
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(NombreAsesor), ' ')) AS word), ' '), ' ', 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_paterno, '-', ''), '.', '')), ' ')) AS word), ' '), ' ', 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_materno, '-', ''), '.', '')), ' ')) AS word), ' ')))
            AS Asesor
        FROM EXTERNAL_QUERY("terraviva-439415.us.bq_almaviva", "SELECT u.id_usuario, u.nombre AS NombreAsesor, u.apellido_paterno, u.apellido_materno FROM usuario AS u")
    ),
    temp_nombres_clientes AS (
        SELECT  
            id_cliente,
            -- Cliente
            TRIM(CONCAT( 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(nombre, ' ')) AS word), ' '), ' ', 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_p, '-', ''), '.', '')), ' ')) AS word), ' '), ' ', 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_m, '-', ''), '.', '')), ' ')) AS word), ' ')))
            AS Cliente
        FROM EXTERNAL_QUERY("terraviva-439415.us.bq_almaviva", "SELECT c.id_cliente, c.nombre, c.apellido_p, c.apellido_m FROM cliente AS c")
    ),
    temp_desarrollos_marcas AS (
        SELECT
            id_desarrollo,
            nombre_desarrollo
        FROM EXTERNAL_QUERY("terraviva-439415.us.bq_almaviva", "SELECT d.id_desarrollo, d.nombre_desarrollo FROM desarrollo AS d")
    ),
    estatusventas AS (
        SELECT
            id_status,
            nombre
        FROM EXTERNAL_QUERY("terraviva-439415.us.bq_almaviva", "SELECT id_status, nombre FROM status_venta")
    ),
    temp_unidades AS (
        SELECT
            id_unidad,
            id_desarrollo,
            -- Unidad, Modelo, M2, PrecioM2, PrecioVenta   
            --'Regular' AS  Modelo,
            modelo AS Modelo,
            numero_unidad AS Unidad,
            metros_cuadrados_totales AS M2, 
            precio_metros_cuadrados,
            privada,
            referencia_banco, 
            numero_etapa AS Etapa
        FROM EXTERNAL_QUERY("terraviva-439415.us.bq_almaviva", "SELECT uni.id_desarrollo, uni.privada,uni.referencia_banco, uni.numero_etapa, uni.id_unidad, uni.numero_unidad, uni.modelo, uni.metros_cuadrados_totales, uni.precio_metros_cuadrados FROM unidades AS uni")
    ),
    temp_ventas AS (
        SELECT
            id_venta,
            id_usuario,
            id_cliente, 
            id_unidad,
            precio_venta AS PrecioVenta,  
            --Proceso  
            fecha_venta AS Proceso, 
            numero_acciones,
            aportacion_accion AS PU_Capital,
            aportacion_prim_accion AS PU_Prima,
            aportacion_accion_total AS Capital,
            aportacion_prim_accion_total AS Prima,
            total_pagado,
            saldo_total,
            numero_pagos,
            --Finalizado
            fecha_carga_contrato,
            status_venta,
            fecha_cierre_venta AS Finalizado,
            cuentaBeneficiario

        FROM EXTERNAL_QUERY("terraviva-439415.us.bq_almaviva", "SELECT v.id_venta, NULLIF(v.fecha_cierre_venta, '0000-00-00') AS fecha_cierre_venta,  stp.cuentaBeneficiario, v.id_unidad, v.id_usuario, v.id_cliente, v.precio_venta, v.fecha_venta, v.numero_acciones, v.aportacion_accion, v.aportacion_prim_accion, v.status_venta, v.aportacion_accion_total, v.aportacion_prim_accion_total, v.total_pagado, v.status_venta AS status, v.saldo_total, v.numero_pagos, NULLIF(v.fecha_carga_contrato, '0000-00-00') AS fecha_carga_contrato FROM venta AS v LEFT JOIN (SELECT id_venta, cuentaBeneficiario FROM stp_bitacora WHERE status = 1 GROUP BY id_venta) AS stp ON v.id_venta = stp.id_venta ")

    ),  
    temp_normalizacion_nombre AS (
        SELECT 
            -- Asesor
            id_usuario,
            TRIM(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    REPLACE(Asesor, 'Merida', ''), 'Miami', ''), 'Cdmx', ''), 'Dam', ''), 'Interno', ''), 'Externo', '')) AS Asesor
        FROM temp_nombres_asesor
    )
    -- Consulta final 126,382
    SELECT
        tv.id_venta,
        CONCAT (tdm.nombre_desarrollo, ' ', Unidad) AS id,
        va.Marca,
        va.Desarrollo,
        tu.Privada,
        tu.referencia_banco,
        tu.Etapa,
        tu.Unidad,  
        tu.Modelo,
        tu.M2,
        tu.precio_metros_cuadrados AS PrecioM2,
        tv.PrecioVenta,
        tnn.Asesor,
        ts.Sucursal,
        ts.Tipo,
        ts.Equipo,
        tnc.Cliente,
        tv.cuentaBeneficiario,
        sv.nombre AS Estatus,
        ti.id_ingreso,
        ti.fecha_ingreso,
        ti.fecha_creacion,
        ti.Cantidad,
        ti.forma_de_pago,
        ti.concepto,
        ti.folio_seguimiento, 
        ti.clave_ingreso,
        ti.nombre_banco

    FROM temp_ingreso AS ti
    LEFT JOIN temp_ventas AS tv ON tv.id_venta = ti.id_venta
    LEFT JOIN temp_nombres_asesor AS tna ON tv.id_usuario = tna.id_usuario
    LEFT JOIN temp_nombres_clientes AS tnc ON tv.id_cliente = tnc.id_cliente
    LEFT JOIN temp_unidades AS tu ON tv.id_unidad = tu.id_unidad
    LEFT JOIN temp_desarrollos_marcas AS tdm ON tdm.id_desarrollo = tu.id_desarrollo
    LEFT JOIN temp_normalizacion_nombre AS tnn ON tnn.id_usuario = tv.id_usuario   
    LEFT JOIN estatusventas AS sv ON sv.id_status = tv.status_venta
    LEFT JOIN `Dimensiones.NombreDesarrollo` AS va ON va.id_nombre_desarrollo = tdm.nombre_desarrollo 
    LEFT JOIN `Dimensiones.NombresVendedores` AS ts ON ts.Vendedor = tnn.Asesor 

-- ;
)
)
SELECT 
        id_venta,
        id,
        Marca,
        Desarrollo,
        Privada,
        Etapa,
        Unidad,  
        folio_seguimiento AS Folio, 
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(Cliente, 'á', 'a'), 'é', 'e'), 'í', 'i'), 'ó', 'o'), 'ú', 'u'), 'ñ', 'n') AS Cliente,
        CONCAT('STP_', referencia_banco) AS STP,
        Estatus,
        id_ingreso,
        fecha_ingreso,
        fecha_creacion,
        Cantidad,
        forma_de_pago,
        concepto,
        nombre_banco AS Banco

FROM VENTASCOMPLETAS 
WHERE Cliente NOT LIKE '%Prueba%' 
AND Desarrollo NOT IN ('Demo','DEMO', 'Vista Esmeralda', 'Real Del Angel')
-- AND Desarrollo IN ('Hunucma', 'Punta Helena', 'Fundadores') -- 2360
AND Cliente NOT LIKE '%Oficina Dam%' 
AND Cliente NOT LIKE '%Manivela%' 
AND Cliente  NOT LIKE '%Demo%'
AND Cliente  NOT LIKE '%Direccion%'
-- AND nombre_banco  IN ('Banorte - SR del Mayab', 'Ban Bajío - SR del Mayab')
-- AND Marca = 'Terraviva'
-- AND nombre_banco  LIKE '%Ban%'
-- AND Asesor  NOT LIKE '%Rodrigo Ososrio%'
AND Asesor  NOT LIKE '%Asesor Pruebas%'
-- AND id_venta NOT IN (163, 162, 5203, 5593, 5594, 5596, 5642, 5788, 5906, 5927, 5927, 5936, 5940, 6315, 6420, 6441, 6441, 6447, 5101, 5102, 15038, 15039, 15086)
-- AND EXTRACT(MONTH FROM fecha_ingreso) IN (2,3,4)
AND EXTRACT(YEAR FROM fecha_ingreso) = 2025 
AND EXTRACT(MONTH FROM fecha_ingreso) = 12
-- AND Marca IN ('DAM', 'CO-IN')
-- AND Marca = 'Terraviva'
-- AND Marca = 'Almaviva'
-- AND Cliente LIKE '%Josethy%'
 GROUP BY id_venta, id,  Marca, Desarrollo, Privada, Etapa, Unidad,   Folio,  Cliente, STP, Estatus, id_ingreso, fecha_ingreso, fecha_creacion,Cantidad, forma_de_pago, concepto, Banco
ORDER BY fecha_creacion ASC