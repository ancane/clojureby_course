(ns task02.helpers)

(defn parse-int [int-str]
  (Integer/parseInt int-str))

(defn is-valid-int? [str]
  (try
    (Integer/parseInt str)
    true
    (catch Exception _ false)))

(defn valid-value? [str]
  (or (and (.startsWith str "'") (.endsWith str "'"))
      (try
        (Integer/parseInt str)
        true
        (catch Exception _ false))))

(defn to-valid-value [str]
  (try
    (Integer/parseInt str)
    (catch Exception _
      (if (and (.startsWith str "'") (.endsWith str "'"))
        (.substring str 1 (dec (.length str)))
        str
        ))))
