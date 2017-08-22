(ns movie-app.utility
  (:require [movie-app.camel-kebab :as ck]
            [clj-time.core :as time]
            [clj-time.format :as time-format]
            [clj-time.coerce :as time-coerce]
            [clojure.core.memoize :as memo]
            [clojure.string :as str]
            [clojure.set :as set]))

;;; Case conversion, memoized for performance.

(def memoized->kebab-case
  (memo/fifo ck/->kebab-case :fifo/threshold 4096))

(def memoized->camelCase
  (memo/fifo ck/->camelCase :fifo/threshold 4096))

(defn kebab->camel
  "Recursively transforms all kebab-case keys in coll to camelCase.
  Preserves :_id keys." [col]
  (ck/transform-keys memoized->camelCase col))

(defn camel->kebab
  "Recursively transforms all camelCase keys in coll to kebab-case.
  Preserves :_id keys." [col]
  (ck/transform-keys memoized->kebab-case col))

(defn camel-str->kebab
  "Given a string, converts it to kebab-case.  Excludes _id." [s]
  (if (= s "_id") s (memoized->kebab-case s)))

;;; Date conversion

(defn convert-date
  "Convert the ISO date string into a date object" [value]
  (try
    (time-format/parse (time-format/formatters :date-time) value)
    (catch Exception e value)))

(defn convert-dates
  "Recursively check alls maps for ISO date strings and converts them to date
  objects." [coll]
  (letfn [(transform [[k v]] [k (convert-date v)])]
    (clojure.walk/postwalk (fn [x] (if (map? x) (into {} (map transform x)) x)) coll)))
