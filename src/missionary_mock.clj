(ns missionary-mock
  (:require [missionary.core :as m]))

(def max-page 10)

(defonce page-values (->> (range 0 (inc max-page))
                          (map (fn [p]
                                 (let [x (inc (* p 5))]
                                   [(inc p) {:max-page max-page
                                             :page     (range x (+ x 5))}])))
                          (into {})))

(defn mock-item-key [p]
  (str "i" p))

(defonce key-values (->> (vals page-values)
                         (map :page)
                         (apply concat)
                         (map mock-item-key)
                         (map (fn [k]
                                [k {:item-data {:code "123456789"}}]))
                         (into {})))

(defonce cached-key-values (into {} (map (fn [[k v]]
                                           [k (assoc v :from-cache? true)]) (random-sample 0.2 key-values))))

(def delays                                                 ;; Our backoff strategy :
  (->> 250                                                  ;; first retry is delayed by 1/4 second
       (iterate (partial * 2))                              ;; exponentially grow delay
       (take 5)))                                           ;; give up after 5 retries

(defn request [page-number]                                 ;; A mock request to exercise the strategy.
  (m/sp                                                     ;; Failure odds are made pretty high to
    (prn :attempt :page-number page-number)                 ;; simulate a terrible connectivity
    (if (zero? (rand-int 2))
      (let [{:keys [max-page, page]} (get page-values page-number)]
        (prn :success :page-number page-number)
        {:page page :all-pages (range 2 (inc max-page))})
      (throw (ex-info "failed." {:worth-retrying true})))))

(defn backoff [request delays]
  (if-some [[delay & delays] (seq delays)]
    (m/sp
      (try (m/? request)
           (catch Exception e
             (if (-> e ex-data :worth-retrying)
               (do (m/? (m/sleep delay))
                   (m/? (backoff request delays)))
               (throw e)))))
    request))

(defmacro parallel-blk-with-backoff [par f]
  `(fn [flow#]
     (m/ap
       (let [result# (m/?> ~par flow#)]
         (m/? (m/via m/blk (m/? (backoff (~f result#) delays))))))))


(defn request-item-details-cache [keys]                     ;; A mock request to exercise the strategy.
  (m/sp                                                     ;; Failure odds are made pretty high to
    (prn :request-item-cache-attempt :key-count (count keys) :keys keys)
    (if (zero? (rand-int 2))                                ;; simulate a terrible connectivity
      (let [result (m/? (m/sleep 100 (into [] (map (fn [k]
                                                     (let [cache-result (get cached-key-values k)]
                                                       (if cache-result
                                                         {:key    k
                                                          :result cache-result}
                                                         {:key k})))) keys)))]
        (prn :result result)
        result)
      (throw (ex-info "failed." {:worth-retrying true})))))

(def fetch-item-details-cache (parallel-blk-with-backoff 3 request-item-details-cache))


(defn request-item-details [keys]                           ;; A mock request to exercise the strategy.
  (m/sp                                                     ;; Failure odds are made pretty high to
    (prn :request-item-attempt :keys keys)                  ;; simulate a terrible connectivity
    (if (zero? (rand-int 2))
      (m/? (m/sleep 2000 (into [] (map (fn [k]
                                         {:key    k
                                          :result (get key-values k)}) keys))))
      (throw (ex-info "failed." {:worth-retrying true})))))

(def fetch-item-details (parallel-blk-with-backoff 5 request-item-details))


(defn store-to-cache [item-details]                         ;; A mock request to exercise the strategy.
  (m/sp                                                     ;; Failure odds are made pretty high to
    (prn :store-attempt :item-details item-details)         ;; simulate a terrible connectivity
    (if (zero? (rand-int 2))
      (m/? (m/sleep 100 (map #(assoc % :stored-to-cache true) item-details)))
      (throw (ex-info "failed." {:worth-retrying true})))))

(def store-item-details (parallel-blk-with-backoff 5 store-to-cache))


(defn fetch-missing-items [cache-response-flow]
  (m/ap (let [[hit? flow] (m/?> 2 (m/group-by #(contains? % :result) cache-response-flow))]
          (m/?> (if hit?
                  flow
                  (->> flow
                       (m/eduction (map :key) (partition-all 100))
                       fetch-item-details
                       (m/eduction cat (partition-all 25))
                       store-item-details
                       (m/eduction cat)))))))

(defn run []
  (let [page-numbers (range 1 (inc max-page))
        flow (m/seed (map #(m/via m/blk (m/? (backoff (request %) delays))) page-numbers))
        values (m/ap
                 (let [task (m/?> 5 flow)]                  ;; from here, fork on every task in **parallel**
                   (m/? task)))
        ;; drain the flow of values and count them
        all (m/?                                            ;; tasks are executed, and flow is consume here!
              (->> values
                   (m/eduction (mapcat :page) (map mock-item-key) (partition-all 25))
                   fetch-item-details-cache
                   (m/eduction cat)
                   fetch-missing-items
                   (m/reduce (fn [p v]
                               (prn :v v)
                               (conj p v))
                             ())))]
    (println :all-count (count all))
    all))
