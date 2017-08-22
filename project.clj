(defproject movie-app "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://localhost:3000/"
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [compojure "1.5.1"]
                 [cheshire "5.6.1"]
                 [com.novemberain/monger "3.0.1"]
                 [org.clojure/core.memoize "0.5.8"]
                 [org.eclipse.jetty/jetty-servlet "7.6.1.v20120215"]
                 [ring/ring-jetty-adapter "1.3.2"]
                 [ring/ring-defaults "0.2.1"]]
  :plugins [[lein-ring "0.9.7"]]
  :ring {:init movie-app.handler/init
         :handler movie-app.handler/app}
  :jvm-opts ["-Xmx12288m" "-XX:+UseConcMarkSweepGC" "-XX:-OmitStackTraceInFastThrow"]
  :main a2t-clj.handler
  :repl-options {:init-ns user
                 :init (movie-app.handler/init)}
  :global-vars {*print-length* 100}
  :profiles {:dev {:resource-paths ["dev"]
                   :dependencies [[javax.servlet/servlet-api "2.5"]
                                  [org.clojure/tools.namespace "0.2.10"]
                                  [ring-mock "0.1.5"]]}
             :uberjar {:aot :all}})
