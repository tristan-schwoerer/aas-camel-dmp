# The DMP image, built from this reactor: `docker build -t dmp:java .`
# The jar is always built from the sources in the context, so the image
# cannot ship an older DMP than the checkout.

# ---------------------------------------------------------------- build
FROM maven:3.9.16-eclipse-temurin-25 AS build
WORKDIR /build
COPY . .
# The Maven repository is a build cache: dependencies are downloaded once.
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -q -DskipTests -pl dmp.component -am package dependency:copy-dependencies \
        -DincludeScope=runtime -DoutputDirectory=target/lib \
 && cp "$(ls dmp.component/target/dmp.component-*.jar | grep -v -- '-sources.jar$')" /dmp.jar

# ---------------------------------------------------------------- runtime
FROM eclipse-temurin:25-jre

# Install wget to check health status
RUN apt update && apt install -y wget && apt clean

# The manifest's Class-Path names lib/<dependency>.jar next to the jar.
COPY --from=build /dmp.jar /usr/share/dmp.jar
COPY --from=build /build/dmp.component/target/lib /usr/share/lib

# This is a process-wide bound, shared by all mappings and Lua blobs.
# Override only after sizing the intended adoption scope.
ENV JAVA_OPTS="-Xmx512m"

# Shared Operations/health/status listener.
EXPOSE 8080

# Forward stop signals to the JVM for bounded managed shutdown. Compact object
# headers (JEP 519) take about 9% off a mapping's heap at no measured cost; they
# stay on when JAVA_OPTS is overridden, and -XX:-UseCompactObjectHeaders there
# turns them off.
CMD exec java -XX:+UseCompactObjectHeaders $JAVA_OPTS -jar "/usr/share/dmp.jar"
