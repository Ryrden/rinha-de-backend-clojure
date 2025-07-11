(ns rinha.utils)

(defn valid-uuid?
  "Validates if string is a valid UUID"
  [uuid-string]
  (some? (parse-uuid uuid-string))) 