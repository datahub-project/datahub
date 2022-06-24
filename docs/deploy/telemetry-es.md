# Telemetría de DataHub

## Información general sobre la telemetría de DataHub

Para crear y mantener de manera efectiva el Proyecto DataHub, debemos comprender cómo trabajan los usuarios finales dentro de DataHub. A partir de la versión 0.8.35, DataHub recopila estadísticas de uso anónimas y errores para informar nuestras prioridades de hoja de ruta y permitirnos abordar los errores de manera proactiva.

A las implementaciones se les asigna un UUID que se envía junto con los detalles del evento, la versión de Java, el sistema operativo y la marca de tiempo; La colección de telemetría está habilitada de forma predeterminada y se puede deshabilitar configurando `DATAHUB_TELEMETRY_ENABLED=false` en la configuración de Docker Compose.

El código fuente está disponible [aquí.](../../metadata-service/factories/src/main/java/com/linkedin/gms/factory/telemetry/TelemetryUtils.java)
