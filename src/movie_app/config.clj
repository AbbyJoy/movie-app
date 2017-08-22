(ns movie-app.config)

;; A flag specifying whether the application is in development or production
;; mode.  Turn this on to enable sending of email and to change unclass banners
;; to secret.

;; A flag specifying whether to communicate with MongoDB over SSL.
(def mongo-over-ssl false)

;; Local Datastore settings.
(def host "localhost")
(def port 27017)
(def db "movies")
(def staging-db "staging")
(def temp-db "temp")
(def cred-db "admin")
(def archive-db "archive")
(def user "ircop")
(def pwd "ircop")

;; Logging settings.
(def log-file "logs/aicig.log")
(def log-coll "log")

;; ;; Heroku Datastore settings.
;; (def host "ds033069.mongolab.com")
;; (def port 33069)
;; (def db "heroku_app22704327")
;; (def user "ircop")
;; (def pwd "ircop")

;; Crypto settings.


;; ;; Email settings.
;; (def smtp-ip "143.69.251.35")
;; (def smtp-host "bulksmtp.us.army.mil")
;; (def smtp-port "25")
;; ;; TODO: Change this to the real email address later.
;; (def master-email-address "josh.lents@us.army.mil")

(def smtp-ip "smtp.gmail.com")
(def smtp-host "smtp.gmail.com")
(def smtp-port "587")
(def master-email-address "ircop.mailer@gmail.com")

(def persisted-config (atom nil))

(def date-formats
  ;; Please maintain the order of these formats. Order matters for some them.
  ["yyyyMMdd"
   "ddMMyyyy"
   "yyyy/MM/dd"
   ;; Make sure that MM/dd/YY comes before MM/dd/yyyy, because MM/dd/yyyy will
   ;; match 12/31/03 as Dec 31, 0003, but we'd prefer Dec 31,2013, which is what
   ;; MM/dd/YY will return.
   "MM/dd/YY"
   "MM/dd/yyyy"
   "d-MMM-YY"
   "dd MMM YY"
   "yyyy-MM-dd"
   "yyyy-MM-dd HH:mm:ss"
   "yyyy-MM-dd'T'HH:mm:ss.SSSZ"])
