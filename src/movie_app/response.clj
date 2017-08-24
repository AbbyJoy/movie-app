(ns movie-app.response
  (:import [org.bson.types ObjectId])
  (:require   [movie-app.db :as db]
              [movie-app.utility :as util]
              [cheshire.core :as cheshire]
              [cheshire.generate :as generate :refer [add-encoder encode-str]]))

(generate/add-encoder org.bson.types.ObjectId generate/encode-str)

(defn json-200 [to-render]
  {:status 200
   :headers {"Content-Type" "text/json; charset=utf-8"
             "Cache-Control" "no-cache, no-store, must-revalidate"
             "Pragma" "no-cache"
             "Expires" "0"}
   :body (cheshire/generate-string
          to-render {:key-fn #(util/memoized->camelCase (name %))})})

(defn json-404 []
  {:status 404
   :headers {"Content-Type" "text/json; charset=utf-8"
             "Cache-Control" "no-cache, no-store, must-revalidate"
             "Pragma" "no-cache"
             "Expires" "0"}
   :body (cheshire/generate-string "Page not found")})

(defn result-nil? [result]
  (if (nil? result)
    (json-404)
    (json-200 result)))

(defn gen-stars-per-movie [movie-id]
  (let [reviews (filter #(= (:movie-id %) movie-id)  (db/get-maps "reviews"))]
    (/ (reduce + (map :rating reviews)) (count reviews))))

(defn reviews? [movie-id]
  (let [reviews (filter #(= (:movie-id %) movie-id)  (db/get-maps "reviews"))]
    (count reviews)))

(defn convert-to-object-id [id]
  (if (= ObjectId (class id))
    id (ObjectId. id)))

(defn get-movie-reviews [movie-id]
  (db/get-maps "reviews" :conditions
               {:movie-id (convert-to-object-id movie-id)}))

(defn get-movie-by-id [id]
  (try
    (assoc (db/get-by-id "movies"  id)
           :stars (gen-stars-per-movie (convert-to-object-id id))
           :reviews (get-movie-reviews (convert-to-object-id id)))
    (catch IllegalArgumentException e
      (str "Caught exception: " (.getMessage e)))))

(defn get-rated-movies []
  (for [movie (db/get-maps "movies")]
    (assoc movie :stars (gen-stars-per-movie (:_id movie))
           :reviews (get-movie-reviews (:_id movie))
           )))

;;; Response endpoints that include movie and review information

(defn get-movies-list []
  (let [result (get-rated-movies)]
    (result-nil? result)))

(defn get-movie-entry [id-string]
  (let [result (get-movie-by-id id-string)]
    (result-nil? result)))
