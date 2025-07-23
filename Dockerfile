FROM clojure:temurin-21-lein AS builder

WORKDIR /app

COPY project.clj .

RUN lein deps

COPY src/ src/

RUN lein uberjar

FROM eclipse-temurin:21-jre-alpine

# To allow healthcheck to work
RUN apk add --no-cache curl

WORKDIR /app

COPY --from=builder /app/target/uberjar/rinha-clojure-0.1.0-SNAPSHOT-standalone.jar app.jar

CMD ["java", \
     "-server", \
     "-Xms128m", \
     "-Xmx128m", \
     "-XX:+UseG1GC", \
     "-XX:MaxGCPauseMillis=50", \
     "-XX:G1HeapRegionSize=16m", \
     "-XX:+UseStringDeduplication", \
     "-XX:+UseCompressedOops", \
     "-XX:+UseCompressedClassPointers", \
     "-XX:+TieredCompilation", \
     "-XX:TieredStopAtLevel=4", \
     "-XX:CompileThreshold=1000", \
     "-XX:+UseThreadPriorities", \
     "-XX:+OptimizeStringConcat", \
     "-Dclojure.compiler.direct-linking=true", \
     "-Dclojure.spec.skip-macros=true", \
     "-Djava.awt.headless=true", \
     "-Dfile.encoding=UTF-8", \
     "-jar", "app.jar"] 