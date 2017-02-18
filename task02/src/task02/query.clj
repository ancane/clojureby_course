(ns task02.query
  (:require [clojure.core.match :refer [match]])
  (:use [task02 helpers db]))

;; Функция выполняющая парсинг запроса переданного пользователем
;;
;; Синтаксис запроса:
;; SELECT table_name [WHERE column comp-op value] [ORDER BY column] [LIMIT N] [JOIN other_table ON left_column = right_column]
;;
;; - Имена колонок указываются в запросе как обычные имена, а не в виде keywords. В
;;   результате необходимо преобразовать в keywords
;; - Поддерживаемые операторы WHERE: =, !=, <, >, <=, >=
;; - Имя таблицы в JOIN указывается в виде строки - оно будет передано функции get-table для получения объекта
;; - Значение value может быть либо числом, либо строкой в одинарных кавычках ('test').
;;   Необходимо вернуть данные соответствующего типа, удалив одинарные кавычки для строки.
;;
;; - Ключевые слова --> case-insensitive
;;
;; Функция должна вернуть последовательность со следующей структурой:
;;  - имя таблицы в виде строки
;;  - остальные параметры которые будут переданы select
;;
;; Если запрос нельзя распарсить, то вернуть nil

;; Примеры вызова:
;; > (parse-select "select student")
;; ("student")
;; > (parse-select "select student where id = 10")
;; ("student" :where #<function>)
;; > (parse-select "select student where id = 10 limit 2")
;; ("student" :where #<function> :limit 2)
;; > (parse-select "select student where id = 10 order by id limit 2")
;; ("student" :where #<function> :order-by :id :limit 2)
;; > (parse-select "select student where id = 10 order by id limit 2 join subject on id = sid")
;; ("student" :where #<function> :order-by :id :limit 2 :joins [[:id "subject" :sid]])
;; > (parse-select "werfwefw")
;; nil

(defn make-where-function [& args]
  (match (vec args)
         [col "=" vl]  #(= ((keyword col) %) vl)
         [col "!=" vl] #(not (= ((keyword col) %) vl))
         [col "<=" vl] #(<= ((keyword col) %) vl)
         [col ">=" vl] #(>= ((keyword col) %) vl)
         [col "<" vl]  #(< ((keyword col) %) vl)
         [col ">" vl]  #(> ((keyword col) %) vl)
         :else nil))


(defn- parse-joins [parsed unparsed]
  (loop [result parsed
         req unparsed]
    (match req
           [(_ :guard #(.equalsIgnoreCase "join" %))
            other_table
            (_ :guard #(.equalsIgnoreCase "on" %))
            left_column "=" right_column
            & rst
            ] (recur (conj result
                           :joins
                           [[(keyword left_column)
                             other_table
                             (keyword right_column)]]) rst)
           :else (reverse result)
           )))

(defn parse-select [^String sel-string]
  (loop [result ()
         req (vec (.split sel-string " "))]
    (match req
           [(_ :guard #(.equalsIgnoreCase "select" %))
            tbl & rst] (recur (conj result tbl) rst)
           [(_ :guard #(.equalsIgnoreCase "where" %))
            column
            (opr :guard #(contains? #{"=" "!=" "<" ">" "<=" ">="} %))
            (vl :guard #(valid-value? %))
            & rst] (recur (conj
                           result
                           :where
                           (make-where-function (keyword column)
                                                opr
                                                (to-valid-value vl)))
                          rst)
           [(_ :guard #(.equalsIgnoreCase "order" %))
            (_ :guard #(.equalsIgnoreCase "by" %))
            column & rst
            ] (recur (conj result :order-by (keyword column)) rst)
           [(_ :guard #(.equalsIgnoreCase "limit" %))
            (n :guard #(is-valid-int? %))
            & rst
            ] (recur (conj result :limit (parse-int n)) rst)
           :else (parse-joins result req)
           )))

;; Выполняет запрос переданный в строке.  Бросает исключение если не удалось распарсить запрос

;; Примеры вызова:
;; > (perform-query "select student")
;; ({:id 1, :year 1998, :surname "Ivanov"} {:id 2, :year 1997, :surname "Petrov"} {:id 3, :year 1996, :surname "Sidorov"})
;; > (perform-query "select student order by year")
;; ({:id 3, :year 1996, :surname "Sidorov"} {:id 2, :year 1997, :surname "Petrov"} {:id 1, :year 1998, :surname "Ivanov"})
;; > (perform-query "select student where id > 1")
;; ({:id 2, :year 1997, :surname "Petrov"} {:id 3, :year 1996, :surname "Sidorov"})
;; > (perform-query "not valid")
;; exception...
(defn perform-query [^String sel-string]
  (if-let [query (parse-select sel-string)]
    (apply select (get-table (first query)) (rest query))
    (throw (IllegalArgumentException. (str "Can't parse query: " sel-string)))))
