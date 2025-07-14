FROM clojure:latest

WORKDIR /app

COPY deps.edn .

RUN clojure -P

COPY src/ src/

CMD ["clojure", "-M", "-m", "rinha.core"] 