from prefect import flow, task, get_run_logger


# --------------------------------------------------
# CONFIGURACIÓN
# --------------------------------------------------

# True: simula el incidente con 15 % de rechazos.
# False: ejecuta el pipeline con datos correctos.
MODO_INCIDENTE = True

# Porcentaje máximo de registros rechazados.
UMBRAL_RECHAZO = 1.0


# --------------------------------------------------
# FUNCIÓN AUXILIAR
# --------------------------------------------------

def crear_transaccion(numero, monto, tipo, canal, sucursal):
    return {
        "id_transaccion": f"TX-{numero:03}",
        "monto": monto,
        "tipo": tipo,
        "canal": canal,
        "sucursal": sucursal,
        "iban": f"PE1200112345678901{numero:04}"
    }


# --------------------------------------------------
# 1. EXTRACCIÓN
# --------------------------------------------------

@task(retries=3, retry_delay_seconds=5)
def extraer_atm(modo_incidente):
    logger = get_run_logger()
    logger.info("Extrayendo transacciones de cajeros ATM")

    if modo_incidente:
        datos = [
            crear_transaccion(i, 100, "debito", "ATM", "Lima")
            for i in range(1, 9)
        ]
    else:
        datos = [
            crear_transaccion(1, 100, "debito", "ATM", "Lima"),
            crear_transaccion(2, 100, "credito", "ATM", "Lima")
        ]

    logger.info(f"ATM: {len(datos)} registros extraídos")
    return datos


@task(retries=3, retry_delay_seconds=5)
def extraer_api_bancaria(modo_incidente):
    logger = get_run_logger()
    logger.info("Extrayendo transacciones de banca móvil")

    if modo_incidente:
        datos = [
            crear_transaccion(i, 200, "credito", "BANCA_MOVIL", "Arequipa")
            for i in range(9, 15)
        ]
    else:
        datos = [
            crear_transaccion(
                3, 200, "debito", "BANCA_MOVIL", "Arequipa"
            ),
            crear_transaccion(
                4, 200, "credito", "BANCA_MOVIL", "Arequipa"
            )
        ]

    logger.info(f"API bancaria: {len(datos)} registros extraídos")
    return datos


@task(retries=3, retry_delay_seconds=5)
def extraer_ach(modo_incidente):
    logger = get_run_logger()
    logger.info("Extrayendo transacciones ACH")

    if modo_incidente:
        datos = [
            crear_transaccion(
                15, 50, "debito", "ACH", "Trujillo"
            ),
            crear_transaccion(
                16, 50, "credito", "ACH", "Trujillo"
            ),
            crear_transaccion(
                17, 50, "debito", "ACH", "Trujillo"
            ),

            # Estas tres transacciones serán rechazadas.
            crear_transaccion(
                18, -50, "credito", "ACH", "Trujillo"
            ),
            crear_transaccion(
                19, -100, "debito", "ACH", "Trujillo"
            ),
            crear_transaccion(
                20, -200, "credito", "ACH", "Trujillo"
            )
        ]
    else:
        datos = [
            crear_transaccion(
                5, 50, "debito", "ACH", "Trujillo"
            ),
            crear_transaccion(
                6, 50, "credito", "ACH", "Trujillo"
            )
        ]

    logger.info(f"ACH: {len(datos)} registros extraídos")
    return datos


# --------------------------------------------------
# 2. UNIFICACIÓN Y CDC
# --------------------------------------------------

@task
def unir_fuentes(datos_atm, datos_api, datos_ach):
    logger = get_run_logger()

    datos = datos_atm + datos_api + datos_ach

    logger.info(
        f"Total recibido desde todas las fuentes: {len(datos)}"
    )

    return datos


@task
def aplicar_cdc(datos):
    logger = get_run_logger()

    registros_unicos = {}
    duplicados = 0

    for registro in datos:
        identificador = registro["id_transaccion"]

        if identificador in registros_unicos:
            duplicados += 1

        registros_unicos[identificador] = registro

    resultado = list(registros_unicos.values())

    logger.info(f"Duplicados encontrados por CDC: {duplicados}")
    logger.info(f"Registros después del CDC: {len(resultado)}")

    return resultado


# --------------------------------------------------
# 3. DATA LAKE Y SUCURSALES
# --------------------------------------------------

@task
def guardar_en_data_lake(datos):
    logger = get_run_logger()

    # Esta parte simula el almacenamiento.
    logger.info(
        f"Guardando {len(datos)} registros originales en Data Lake Raw"
    )

    return datos


@task
def dividir_por_sucursal(datos):
    logger = get_run_logger()

    sucursales = {}

    for registro in datos:
        nombre = registro["sucursal"]

        if nombre not in sucursales:
            sucursales[nombre] = []

        sucursales[nombre].append(registro)

    lotes = list(sucursales.values())

    logger.info(
        f"Cantidad de sucursales encontradas: {len(lotes)}"
    )

    return lotes


# --------------------------------------------------
# 4. VALIDACIÓN
# --------------------------------------------------

@task
def validar_sucursal(datos_sucursal):
    logger = get_run_logger()

    validos = []
    rechazados = []
    identificadores = set()

    nombre_sucursal = datos_sucursal[0]["sucursal"]

    for registro in datos_sucursal:
        errores = []

        if not registro.get("id_transaccion"):
            errores.append("ID de transacción vacío")

        if registro.get("monto") is None:
            errores.append("Monto vacío")
        elif registro["monto"] < 0:
            errores.append("Monto negativo")

        if not registro.get("iban"):
            errores.append("IBAN vacío")

        if registro.get("id_transaccion") in identificadores:
            errores.append("ID de transacción duplicado")

        identificadores.add(registro.get("id_transaccion"))

        if errores:
            registro["errores"] = errores
            rechazados.append(registro)
        else:
            validos.append(registro)

    logger.info(
        f"{nombre_sucursal}: "
        f"{len(validos)} válidos y "
        f"{len(rechazados)} rechazados"
    )

    return {
        "validos": validos,
        "rechazados": rechazados
    }


@task
def consolidar_validaciones(resultados):
    validos = []
    rechazados = []

    for resultado in resultados:
        validos.extend(resultado["validos"])
        rechazados.extend(resultado["rechazados"])

    return {
        "validos": validos,
        "rechazados": rechazados
    }


# --------------------------------------------------
# 5. COMPUERTA DE CALIDAD
# --------------------------------------------------

@task
def evaluar_calidad(resultado):
    logger = get_run_logger()

    validos = resultado["validos"]
    rechazados = resultado["rechazados"]

    total = len(validos) + len(rechazados)

    if total == 0:
        raise ValueError("No se recibieron transacciones")

    porcentaje = len(rechazados) / total * 100

    logger.info(f"Total de registros: {total}")
    logger.info(f"Registros válidos: {len(validos)}")
    logger.info(f"Registros rechazados: {len(rechazados)}")
    logger.info(f"Porcentaje rechazado: {porcentaje:.2f} %")

    if rechazados:
        logger.warning(
            "Los registros rechazados fueron enviados a cuarentena"
        )

    if porcentaje > UMBRAL_RECHAZO:
        logger.error(
            "ALERTA: se superó el porcentaje máximo de rechazo"
        )

        raise ValueError(
            f"Pipeline detenido: {porcentaje:.2f} % de rechazo"
        )

    logger.info("Compuerta de calidad aprobada")

    return validos


# --------------------------------------------------
# 6. TRANSFORMACIÓN
# --------------------------------------------------

@task
def transformar(datos):
    logger = get_run_logger()

    for registro in datos:
        # Comisión del 1 %.
        registro["comision"] = registro["monto"] * 0.01

        # Score de riesgo simulado.
        if registro["monto"] >= 1000:
            registro["score_riesgo"] = "ALTO"
        elif registro["monto"] >= 500:
            registro["score_riesgo"] = "MEDIO"
        else:
            registro["score_riesgo"] = "BAJO"

        # Enmascaramiento del IBAN.
        iban = registro["iban"]
        registro["iban"] = iban[:4] + "**************" + iban[-4:]

    logger.info(
        f"Se transformaron {len(datos)} transacciones"
    )

    return datos


# --------------------------------------------------
# 7. CARGA
# --------------------------------------------------

@task(retries=3, retry_delay_seconds=5)
def cargar_data_warehouse(datos):
    logger = get_run_logger()

    # Esta parte simula una carga idempotente con MERGE.
    logger.info(
        f"Cargando {len(datos)} registros al Data Warehouse"
    )
    logger.info("Carga MERGE completada sin duplicados")

    return datos


# --------------------------------------------------
# 8. PRUEBAS POSTERIORES
# --------------------------------------------------

@task
def ejecutar_pruebas(datos):
    logger = get_run_logger()

    total_debitos = sum(
        registro["monto"]
        for registro in datos
        if registro["tipo"] == "debito"
    )

    total_creditos = sum(
        registro["monto"]
        for registro in datos
        if registro["tipo"] == "credito"
    )

    logger.info(f"Total débitos: {total_debitos}")
    logger.info(f"Total créditos: {total_creditos}")

    if len(datos) == 0:
        raise ValueError(
            "La carga no contiene registros"
        )

    if total_debitos != total_creditos:
        raise ValueError(
            "Prueba fallida: débito y crédito no coinciden"
        )

    logger.info("Pruebas posteriores aprobadas")

    return True


# --------------------------------------------------
# 9. PUBLICACIÓN
# --------------------------------------------------

@task
def publicar_reporte_sbs(pruebas_aprobadas):
    logger = get_run_logger()

    if pruebas_aprobadas:
        logger.info(
            "Reporte financiero enviado correctamente a la SBS"
        )


# --------------------------------------------------
# 10. FLUJO PRINCIPAL
# --------------------------------------------------

@flow(name="Pipeline financiero FinanData")
def pipeline_financiero():

    # Las tres extracciones se ejecutan en paralelo.
    futuro_atm = extraer_atm.submit(MODO_INCIDENTE)
    futuro_api = extraer_api_bancaria.submit(MODO_INCIDENTE)
    futuro_ach = extraer_ach.submit(MODO_INCIDENTE)

    datos = unir_fuentes(
        futuro_atm,
        futuro_api,
        futuro_ach
    )

    datos_cdc = aplicar_cdc(datos)
    datos_raw = guardar_en_data_lake(datos_cdc)

    lotes = dividir_por_sucursal(datos_raw)

    # Las sucursales se validan en paralelo.
    futuros_validacion = validar_sucursal.map(lotes)

    resultados = [
        futuro.result()
        for futuro in futuros_validacion
    ]

    resultado_general = consolidar_validaciones(resultados)

    datos_aprobados = evaluar_calidad(resultado_general)

    datos_transformados = transformar(datos_aprobados)

    datos_cargados = cargar_data_warehouse(
        datos_transformados
    )

    pruebas_aprobadas = ejecutar_pruebas(datos_cargados)

    publicar_reporte_sbs(pruebas_aprobadas)


if __name__ == "__main__":
    pipeline_financiero()