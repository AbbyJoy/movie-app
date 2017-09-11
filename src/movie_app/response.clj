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

(defn json-404 [e]
  {:status 404
   :headers {"Content-Type" "text/json; charset=utf-8"
             "Cache-Control" "no-cache, no-store, must-revalidate"
             "Pragma" "no-cache"
             "Expires" "0"}
   :body (cheshire/generate-string e)})

(defn result-nil? [result]
  (if (nil? result)
    (json-404 "404 - Page not found")
    (json-200 result)))

(defn get-review-rating [review]
  (let [rating (:rating review)]
    (if (= String (class rating)) (double (read-string rating)) rating)))

(defn gen-stars-per-movie [movie-id]
  (let [reviews (filter #(= (:movie-id %) movie-id)  (db/get-maps "reviews"))]
    (/ (reduce + (map get-review-rating reviews)) (count reviews))))

(defn reviews? [movie-id]
  (let [reviews (filter #(= (:movie-id %) movie-id)  (db/get-maps "reviews"))]
    (count reviews)))

(defn convert-to-object-id [id]
  (if (= ObjectId (class id))
    id (ObjectId. id)))

(defn get-movie-reviews [movie-id]
  (db/get-maps "reviews" :conditions
               {:movie-id (convert-to-object-id movie-id)}))

(defn assoc-stars-reviews [movie movie-id]
  (let [id (convert-to-object-id movie-id)]
    (assoc (db/get-by-id "movies" id)
           :stars (format "%.1f" (gen-stars-per-movie id))
           :reviews (get-movie-reviews id))))

(defn get-movie-by-id [id]
  (assoc-stars-reviews (db/get-by-id "movies" (convert-to-object-id id)) id))

(defn get-rated-movies []
  (for [movie (db/get-maps "movies")]
    (assoc-stars-reviews movie (:_id movie))))

(defn get-review-by-id [id]
  (db/get-by-id "reviews" (convert-to-object-id id)))

(defn update-review-by-id [params]
  (let [id (:id params)
        document (dissoc params :id)]
    (db/update-by-id "reviews" document (convert-to-object-id id))))

;;; Response endpoints that include movie and review information

(defn get-movies-list []
  (try
    (let [result (get-rated-movies)]
      (result-nil? result))
    (catch Exception e
      (json-404 (.toString e)))))

(defn get-movie-entry [id-string]
  (try
    (let [result (get-movie-by-id id-string)]
      (result-nil? result))
    (catch IllegalArgumentException e
      (json-404 (.toString e)))))

(defn get-review-list []
  (let [result (db/get-maps "reviews")]
    (result-nil? result)))

(defn get-review-entry [id-string]
  (try
    (let [result (get-review-by-id id-string)]
      (result-nil? result))
    (catch IllegalArgumentException e
      (json-404 (.toString e)))))

(defn update-review [params]
  (try
    (result-nil?(update-review-by-id params))
    (catch IllegalArgumentException e
      (json-404 (str (.toString e) e)))
    (catch Exception e
      (json-404 (str (.toString e) (.getStackTrace e))))))
