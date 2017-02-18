(ns task02.db
  "Namespace for database-related data & functions..."
  (:require [clojure.data.csv :as csv]
            [clojure.string :as str])
  (:use task02.helpers))

;; helper functions

(defn table-keys [tbl]
  (mapv keyword (first tbl)))

(defn key-value-pairs [tbl-keys tbl-record]
  (interleave tbl-keys tbl-record))

(def data-record zipmap)

(defn data-table [tbl]
  (map (partial data-record (table-keys tbl)) (next tbl)))

(defn str-field-to-int [field rec]
  (update-in rec [field] parse-int))

;; Место для хранения данных - используйте atom/ref/agent/...
(def student (agent ()))
(def subject (agent ()))
(def student-subject (agent ()))

;; функция должна вернуть мутабельный объект используя его имя
(defn get-table [^String tb-name]
  (let [lname (str/lower-case tb-name)]
    (cond
     (= lname "student") student
     (= lname "subject") subject
     (= lname "student-subject") student-subject
     )))

;;; Данная функция загружает начальные данные из файлов .csv
;;; и сохраняет их в изменяемых переменных student, subject, student-subject
(defn load-initial-data []
  ;;; :implement-me может быть необходимо добавить что-то еще
  (send student empty)
  (send student into (->> (data-table (csv/read-csv (slurp "student.csv")))
                          (map #(str-field-to-int :id %))
                          (map #(str-field-to-int :year %))))
  (send subject empty)
  (send subject into (->> (data-table (csv/read-csv (slurp "subject.csv")))
                          (map #(str-field-to-int :id %))))
  (send student-subject empty)
  (send student-subject into (->> (data-table (csv/read-csv (slurp "student_subject.csv")))
                                  (map #(str-field-to-int :subject_id %))
                                  (map #(str-field-to-int :student_id %)))))
;;(load-initial-data)

;; select-related functions...
(defn where* [data condition-func]
  (if condition-func
    (filter condition-func data)
    data))

(defn limit* [data lim]
  (if lim
    (take lim data)
    data))

(defn order-by* [data column]
  (if column
    (sort-by column data)
    data))

(defn join* [data1 column1 data2 column2]
  (for [left data1
        right data2
        :when (= (column1 left) (column2 right))]
    (merge right left)))

;; Внимание! в отличии от task01, в этом задании 2-я таблица передается в виде строки с именем таблицы!
(defn perform-joins [data joins]
  (if (empty? joins)
    data
    (let [[col1 data2 col2] (first joins)]
      (recur (join* data col1 @(get-table data2) col2)
             (next joins)))))

;; Данная функция аналогична функции в первом задании за исключением параметра :joins как описано выше.
;; примеры использования с измененым :joins:
;;   (select student-subject :joins [[:student_id "student" :id] [:subject_id "subject" :id]])
;;   (select student-subject :limit 2 :joins [[:student_id "student" :id] [:subject_id "subject" :id]])
(defn select [data & {:keys [where limit order-by joins]}]
  (as-> @data dt
    (if-not (nil? joins)
      (perform-joins dt joins)
      dt)
    (if-not (nil? where)
      (where* dt where)
      dt)
    (if-not (nil? order-by)
      (order-by* dt order-by)
      dt)
    (if-not (nil? limit)
      (limit* dt limit)
      dt)))

;; insert/update/delete...

;; Данная функция должна удалить все записи соответствующие указанному условию
;; :where. Если :where не указан, то удаляются все данные.
;;
;; Аргумент data передается как мутабельный объект (использование зависит от выбора ref/atom/agent/...)
;;
;; Примеры использования
;;   (delete student) -> []
;;   (delete student :where #(= (:id %) 1)) -> все кроме первой записи
(defn delete [data & {:keys [where]}]
  (if-not (nil? where)
    (send data #(remove where %))
    (send data empty)))

;; Данная функция должна обновить данные в строках соответствующих указанному предикату
;; (или во всей таблице).
;;
;; - Аргумент data передается как мутабельный объект (использование зависит от выбора ref/atom/agent/...)
;; - Новые данные передаются в виде map который будет объединен с данными в таблице.
;;
;; Примеры использования:
;;   (update student {:id 5})
;;   (update student {:id 6} :where #(= (:year %) 1996))
(defn update [data upd-map & {:keys [where]}]
  (if-not (nil? where)
    (send student #(map (fn[x] (if (where x) (into x upd-map) x)) %))
    (send student #(map (fn[x] (into x upd-map)) %))))

;; Вставляет новую строку в указанную таблицу
;;
;; - Аргумент data передается как мутабельный объект (использование зависит от выбора ref/atom/agent/...)
;; - Новые данные передаются в виде map
;;
;; Примеры использования:
;;   (insert student {:id 10 :year 2000 :surname "test"})
(defn insert [data new-entry]
  (send data #(conj % new-entry)))


(if (#(= (:year %) 1996) {:year 1996}) 1 2 ) 
