(ns movie-app.handler
  (:import [org.bson.types ObjectId])
  (:require [movie-app.config :as config]
            [movie-app.db :as db]
            [movie-app.response :as response]
            [movie-app.utility :as util]
            [compojure.core :as cc]
            [compojure.handler :as handler]
            [compojure.route :as route]
            [cheshire.core :as cheshire]
            [cheshire.generate :as generate :refer [add-encoder encode-str]]
            [ring.middleware.defaults :as rmd]
            [ring.util.response :as resp]))

(defn init []
  (db/db-setup config/host config/port config/db config/staging-db
               config/temp-db config/archive-db config/user config/pwd))

(cc/defroutes app-routes
  (cc/routes (cc/GET "/" [] (resp/redirect "/index.html"))
             (cc/GET "/movie" [] (response/get-movies-list))
             (cc/GET "/movie/:id" [id] (response/get-movie-entry id))
             (cc/GET "/review" [] (response/get-review-list))
             (cc/GET "/review/:id" [id] (response/get-review-entry id))
             (cc/PUT "/review/:id" {params :params} (response/update-review params))
             (route/resources "/")
             (route/not-found "Not Found")))

(def app
  (rmd/wrap-defaults app-routes (assoc-in rmd/site-defaults [:security :anti-forgery] false)))
