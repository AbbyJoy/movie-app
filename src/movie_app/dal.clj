(ns a2t-clj.dal
  (:import [org.bson.types ObjectId])
  (:require [a2t-clj.dal-relationships :as dr]
            [a2t-clj.db :as db]
            [a2t-clj.filter :as filter]
            [a2t-clj.historical :as h]
            [a2t-clj.log :as log]
            [a2t-clj.update :as update]
            [a2t-clj.utility :as util]
            [a2t-clj.etl.destination-schemas :as ds]
            [clojure.string :as str]
            [monger.operators :refer [$and $in]]))

(defn find-entity [entity query params] nil)

;; Global Var that holds the archivable entities.
(def dest-cols
  (set (map str (keys (ns-publics 'a2t-clj.etl.destination-schemas)))))
(def update-cols #{"unit" "taads-document" "taads-document-detail"
                   "probe-ape-def" "probe-mdep-def"})

(defn get-child-data
  "Map across child definitions. For each definition return new map containing
  child def (:def) and child data grouped by grouping function (:grouped-data)
  to make attachment to parent more efficient."
  [child-defs user]
  (let [gp       (fn [user c] (group-by
                               (:grouping-fn c)
                               (find-entity (:entity c) {:query (:query c)} nil user)))
        map-func (fn [c] {:def c :grouped-data ((partial gp user) c)})]
    (map map-func child-defs)))

(defn attach-child
  "For each parent entity, attach corresponding child data."
  [children parent]
  ;; For given child, create tuple to be inserted into this parent.
  (let [get-parent-data (fn [p c]
                          (let [mapping (:parent-mapping (:def c))
                                v (if (vector? mapping)
                                    (get-in p mapping) (mapping p))
                                items (get (:grouped-data c) v)]
                            {(keyword (:field (:def c))) (or items [])}))
        ;; Build new map with children data, to be merged with original parent.
        alter-map       (map (partial get-parent-data parent) children)]
    (apply merge (conj alter-map parent))))

(defn attach-children-to-parents
  "Map across entities, applying child data to each entry."
  [parents filters entity params user]
  (let [child-defs (filter :entity (dr/get-child-definitions entity parents))
        child-defs (filter/filter-on-eager-fetch child-defs params filters)
        children   (get-child-data child-defs user)
        map-func   (partial attach-child children)]
    (map map-func parents)))

(defn attach-relationships
  "Map across entities, applying relationship data to each entry."
  [parents filters entity params user]
  (let [relationships (filter/filter-on-eager-fetch (dr/get-relationships entity) params filters)
        get-rel-data (fn [rl]
                       (let [ids (map #(or (:_id %) %) (flatten (map (:field rl) parents)))]
                         (find-entity (:entity rl) {:query {:_id {$in ids}}
                                                    :whitelist (:projection rl)} {} user)))
        r-with-data (map #(assoc % :data (group-by :_id (get-rel-data %))) relationships)
        attach-rel (fn [p r] (as-> (map #(or (:_id %) %) (flatten [((:field r) p)])) $
                                   (flatten (vals (select-keys (:data r) $)))
                                   (if (true? (:single r)) (first $) $)
                                   (assoc p (:field r) $)))
        attach-data  (fn [parent] (reduce attach-rel parent r-with-data))
        new-data     (map attach-data parents)]
    new-data))

(defn find-entity-post-fetch
  "Given a list of entities, attach children and relationships."
  [list filters entity params user]
  (-> list
      (attach-children-to-parents filters entity params user)
      (attach-relationships filters entity params user)
      (dr/post-fetch-modifications entity find-entity params user)))

(defn get-filter-conditions [entity input user params]
  (filter/build-conditions entity input user params
                           (dr/get-child-definitions entity [])))

(defn get-data [entity filters params user dbname]
  (find-entity-post-fetch
   (if (and (contains? update-cols (name entity))
            (not (:skipUpdate filters false)))
     (update/get-updated-maps (name entity) filters)
     (db/get-maps (name entity)
                  :conditions (:query filters {})
                  :projection (:whitelist filters [])
                  :sort       (:sort filters {})
                  :page       (:page filters)
                  :per-page   (:per-page filters)
                  :dbname     dbname))
   filters entity params user))

(defn get-covered-pe-count
  "Gets the count of covered PEs taking into account both the production and the
  update dbs. Since all PEs in the production collection are covered, we have to
  inspect the update collection to see which records have uncovered PEs. We want
  to avoid all of the update/join logic, so this gives us a slightly quicker path."
  [entity filters dbname]
  (let [ids (db/get-count (name entity) :conditions (:query filters {})
                          :dbname dbname)
        ref-ids (db/get-count (str (name entity) "-update")
                              :conditions {:program-element-code {"$exists" true}
                                           :_id_ref {"$exists" true}})]
    (- ids ref-ids)))

(defn find-entity
  "Pull in entities from DB layer."
  ([entity input-filters params user etl?]
   (let [dbname  (if (true? etl?) :sdb :db)
         filters (get-filter-conditions entity input-filters user params)]
     (cond
       ;; Special case for counting covered PEs.
       (and (:count filters) (:uncovering filters))
          [{:count (get-covered-pe-count entity filters dbname)}]
       (:count filters)
          [{:count (db/get-count (name entity) :conditions (:query filters {}) :dbname dbname)}]
       :else (get-data entity filters params user dbname))))
  ([entity input-filters params user]
   (find-entity entity input-filters params user false)))

(defn get-user-by-akoid
  "Fetch user from database based on AKOID." [akoid]
  ;; Do these steps manually and not through find-entity because find-entity
  ;; depends on user data already being present.
  (let [user      (first (db/get-maps "user"
                                      :conditions {:username akoid}
                                      :projection {} :dbname :db))
        whitelist [:uic :long-name :parent-uic :readiness]
        whitelist (:projection (some #(when (= :unit (:entity %)) %)
                                     (dr/get-relationships :user)) whitelist)
        get-units (fn [k u]
                    (db/get-maps "unit"
                                 :conditions {:_id {$in (map :_id (k u))}}
                                 :projection whitelist :dbname :db))]
    (assoc user :following-units
           (get-units :following-units user) :units (get-units :units user))))

(defn std-list
  "Default list by collection." [entity user params]
  (find-entity entity {} params user))

(defn std-get-by-id
  "Default entity object retrieval, by _id field." [entity user params]
  (let [filters (filter/build-conditions entity {} user params
                                         (dr/get-child-definitions entity []))
        filters (assoc filters :query
                       {$and [{:_id (ObjectId. (:id params))}
                              (:query filters)]})
        results (find-entity entity filters params user)]
    (first results)))

(defn std-delete
  "Standard entity delete by _id field." [entity user params]
  (db/del-map (name entity) (:id params)))

(defn update-obj
  "Update a given entity object." [entity user params body]
  (let [body (dr/pre-persist-modifications entity body user)
        update-fn (if (contains? update-cols (name entity))
                    update/update-map db/update-by-id)]
    (update-fn (name entity) body (:id params))))

(defn bulk-update
  "Update group of items." [entity items user]
  ;; TODO: Return updated versions of items using IDs.
  (let [ids    (map #(ObjectId. (:_id %)) items)
        parse  (fn [entity user item] (update-obj entity user {:id (:_id item)} item))]
    (map (partial parse entity user) items)))

(defn create
  "Save an entity object to the datastore." [entity user params body]
  (let [id   (ObjectId.)
        body (assoc (dr/pre-persist-modifications entity body user) :_id id)]
    (db/put-map (name entity) body)
    (str id)))

;;; Custom metadata functions

(defn get-collection-names
  "Wrapper function for db/get-collection-names." []
  (db/get-collection-names))

(defn get-collection-field-type
  "Takes in a value and returns its type." [item]
  (case (-> item type str)
    "" "nil"
    "class clojure.lang.PersistentArrayMap" "Object"
    "class clojure.lang.PersistentHashMap" "Object"
    "class clojure.lang.PersistentVector" "Array"
    "class org.bson.types.ObjectId" "ObjectId"
    "class [B" "BinData"
    (last (str/split (last (str/split (str (type item)) #" ")) #"\."))))

(defn get-collection-field-types
  "A recursive function that determines the value type for each field in an
  object." [parent kv]
  (let [title      (if parent (str (name parent) "." (name (first kv))) (first kv))
        self       {title (get-collection-field-type (second kv))}]
    (case (get self title)
      "Object" (conj (map (partial get-collection-field-types title) (second kv)) self)
      "Array"  (conj (map #(get-collection-field-types parent [(first kv) %]) (second kv)) self)
      self)))

(defn get-array-type "Describe the type of data held in the array." [types]
  (if (some #(= % "Array") types)
    (str "Array::" (first (filter #(not (contains? #{"Array" "nil"} %)) types)))
    (first types)))

(defn get-collection-fields
  "A reducer that merges all the fields returned from get-type."
  [fields-memo item]
  (as-> (map (partial get-collection-field-types nil) item) $
    (group-by (comp first first) (flatten $))
    (into {} (for [[k v] $] [k (get-array-type (map (comp second first) v))]))
    (apply dissoc $ (map first (filter #(not (contains? #{"nil" "Array::"} (second %)))
                                       fields-memo)))
    (merge fields-memo $)))

(defn get-all-fields-for-collections
  "Determine all fields and field types for each collection." [collections dbname]
  (into {} (pmap #(vector % (reduce get-collection-fields {} (db/get-maps % :dbname dbname)))
                 (flatten [collections]))))

(defn diff-entity-with-database
  "Compares a passed in entity to what is in the database in the specified
  collection." [entity collection]
  (let [collection-in-db  (db/get-by-id collection (:_id entity))
        field-list    (keys (dissoc entity :_id))]
    (keys (into {} (clojure.set/difference (set entity) (set collection-in-db))))))

;;; Custom account-request functions

(defn update-user-based-on-request
  "Apply a user object update." [request user-updating-request]
  (let [units (map #(identity {:_id %}) (:units request))
        ;; TODO: Revisit if we figure out how to read email from CAC.
        new-user (assoc (select-keys request [:roles :email :name]) :units units)]
    (update-obj :user user-updating-request {:id (.toString (-> request :user :_id))} new-user)))

(defn get-alert-enabled-users
  "Return all users that have alerts." [type]
  (db/get-maps "user" :conditions {(str "alerts." type) true}))

;;; Custom unit functions

(defn reduce-rollup-units
  "Reducer for the list of units related to a 'FF' or 'AA' UIC. The result of
  the reducer will be returned to the front-end to be used on the unit dashboard."
  [merged-unit rollup-unit]
  (apply assoc merged-unit
         (apply concat (for [k [:gainloss :personnel :slots :equipment-slots]]
                         [k (concat (k merged-unit) (k rollup-unit))]))))

;;; Archive

(defn put-map
  "Given a collection name and a map, first archive and then insert the map as
  a document."
  [col m]
  (when (contains? dest-cols col)
    (h/archive-map col m))
  (db/del-map col (:_id m))
  (db/put-map col m))

(defn put-maps [col v]
  "Given a collection name and a vector of maps, first archive and then insert
  the maps as documents."
  (let [ids (mapv :_id (remove #(nil? (:_id %)) v))]
    (cond (empty? ids)
          (throw (Exception. (str "Empty MongoDB ObjectId(s) found in data:" ids)))
          (not (apply distinct? ids))
          (throw (Exception. (str "Duplicate MongoDB ObjectId(s) found in data:" ids)))
          :else (do (when (contains? dest-cols col)
                      (h/archive-maps col v))
                    (db/del-maps col {:_id {"$in" ids}})
                    (db/put-maps col v)))))

(defn get-by-id
  "Get a document, specified by _id (either as an ObjectId or
  string). Providing a start-date will retrieve the historical document."
  ([col id] (db/get-by-id col id))
  ([col id start-date]
   (if (contains? dest-cols col)
     (h/go-back-in-time-by-id col id start-date)
     (get-by-id col id))))

(defn get-map
  "Get a document from a collection, given some criteria. Providing a
  start-date will retrieve the historical document.
  e.g. (get-map \"person\" {:nameFirst \"JOHN\"})"
  ([col m] (db/get-map col m))
  ([col m start-date]
   (if (contains? dest-cols col)
     (h/go-back-in-time-by-query col m start-date)
     (get-map col m))))

(defn get-maps
  "Get multiple documents from a collection. Providing a start-date will
  retrieve the historical document."
  ([col conditions] (db/get-maps col :conditions conditions))
  ([col conditions projection] (db/get-maps col :conditions conditions :projection projection))
  ([col conditions projection start-date]
   (if (contains? dest-cols col)
     (h/go-back-in-time-by-coll-date col start-date projection conditions)
     (db/get-maps col :conditions conditions :projection projection))))

(defn update-by-id
  "Update entity based on the _id passed in. Will not delete missing fields
  from new data. Passing in a db is not supported because we only support
  deleting from the destination collections."
  ([col m id]
   (if (contains? dest-cols (str col))
     (h/archive-map col m))
   (db/update-by-id col m id)))

(defn del-map
  "Delete a single document from a collection, specified by _id (either as an
  ObjectId or string). Returns true if a record was found and deleted."
  ([col id] (h/del-map col id)))

(defn get-archive-data [collection params]
  (let [query (h/query-archive-by-field (seq (:fields params)))]
    (db/get-maps (str collection "-archive") :conditions query)))

;; Pagination

(defn partition-data
  "Takes the result of ingest-staging-collection and partitions the data
  into records that are small enough to be stored in MongoDB." [large-map]
  (let [partitioned-map (into {} (map #(assoc % 1 (partition-all 10000 (second %))) large-map))
        largest-pair    (reduce #(if (> (count (get partitioned-map %2)) (:count %1))
                                   {:key %2 :count (count (get partitioned-map %2))} %1)
                                {:key nil :count 0} (keys partitioned-map))]
    (map #(reduce (fn [acc key] (assoc acc key (nth (key partitioned-map) %1 [])))
                  {} (keys partitioned-map))
         (range (:count largest-pair)))))

(defn put-paged-data
  "Gets data in a paging method from a staging collection, applies a function to
  the data, then puts the data into another collection. The destination collection
  can be modified by using the dest? and dest-db optional parameters. The dest?
  parameter is for not attaching a date to the collection name, and the dest-db
  allows the db to be changed from :sdb."
  [edate get-coll put-coll put-fn & {:keys [dest? update? tdd?] ; tdd? is temporary.
                                     :or {dest? false update? false tdd? false}}]
  (let [get-coll-name (str edate "-" get-coll)
        put-coll-name (if (or dest? update?)
                        put-coll (str edate "-" put-coll))
        total (db/get-count get-coll-name :dbname :sdb)
        ;; Right now we don't have an efficient way to find how many records
        ;; from a current ingest have been put into an update or destination
        ;; collection, so for now we have to assume it's a fresh ingest.
        buffered-total (if (or dest? update?) 0
                           (db/get-count put-coll-name :dbname :sdb))
        per-page 10000
        pagination-data (util/get-pagination-data {:coll-size total
                                                   :page-size per-page
                                                   :buffer-size buffered-total})]
    (doseq [pagination-item pagination-data
            :let [[page-num per-page] pagination-item]]
      (do
        (as-> (db/get-maps get-coll-name :dbname :sdb :page page-num :per-page per-page) $
          (cond
            update? (bulk-update put-coll (put-fn $) {})
            dest? (put-maps put-coll-name (util/check-dup-ids (put-fn $)))
            ;; tdd? is a temporary solution, this should be handled better once
            ;; all view-fns are being paged.
            tdd? (as-> (str edate "-" (first (clojure.string/split get-coll #"_"))) $$
                       (partition-data (put-fn $$ :tdd $ :page-data (conj pagination-item total)))
                       (db/put-maps put-coll-name $$ :dbname :sdb))
            :else (db/put-maps put-coll-name (util/check-dup-ids (put-fn $)) :dbname :sdb))
          (log/log :info (str "Page " page-num " of "
                              (first (last pagination-data)) " pages.")))))))
