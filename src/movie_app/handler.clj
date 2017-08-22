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
             (cc/GET "/movie" [] (response/get-col "movies"))
             (cc/GET "/movie/:id" [id] (response/get-map-by-id  "movies" id))
             (route/resources "/")
             (route/not-found "Not Found")))

(def app
  (rmd/wrap-defaults app-routes rmd/site-defaults))
