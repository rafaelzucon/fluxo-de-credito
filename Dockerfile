# syntax=docker/dockerfile:1
# Build multi-stage: compila os 3 módulos uma única vez e gera uma imagem enxuta por serviço (targets).

FROM maven:3.10-eclipse-temurin-21 AS build
# Opcional: certificados de CA extras (*.crt) para redes com proxy corporativo/inspeção TLS.
COPY docker/certs/ /tmp/certs/
RUN for c in /tmp/certs/*.crt; do \
      [ -f "$c" ] && keytool -importcert -noprompt -cacerts -storepass changeit -alias "$(basename "$c")" -file "$c"; \
    done; true
WORKDIR /build
COPY pom.xml .
COPY solicitacao-service/pom.xml solicitacao-service/
COPY notificacao-service/pom.xml notificacao-service/
COPY motor-credito-simulador/pom.xml motor-credito-simulador/
RUN --mount=type=cache,target=/root/.m2 mvn -B -q dependency:go-offline -DexcludeReactor=true || true
COPY solicitacao-service/src solicitacao-service/src
COPY notificacao-service/src notificacao-service/src
COPY motor-credito-simulador/src motor-credito-simulador/src
ARG SKIP_TESTS=false
RUN --mount=type=cache,target=/root/.m2 mvn -B -q package -DskipTests=${SKIP_TESTS}

FROM eclipse-temurin:21-jre AS runtime-base
RUN groupadd --system app && useradd --system --gid app app
WORKDIR /app
USER app
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"
ENTRYPOINT ["java", "-jar", "/app/app.jar"]

FROM runtime-base AS solicitacao-service
COPY --from=build /build/solicitacao-service/target/solicitacao-service-*.jar /app/app.jar
EXPOSE 8080

FROM runtime-base AS notificacao-service
COPY --from=build /build/notificacao-service/target/notificacao-service-*.jar /app/app.jar
EXPOSE 8081

FROM runtime-base AS motor-credito-simulador
COPY --from=build /build/motor-credito-simulador/target/motor-credito-simulador-*.jar /app/app.jar
EXPOSE 8082
