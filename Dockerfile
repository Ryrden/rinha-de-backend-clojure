FROM clojure:latest

WORKDIR /app

COPY deps.edn .

RUN clojure -P

COPY src/ src/

EXPOSE 9999

CMD ["clojure", "-M", "-m", "rinha.core"] 