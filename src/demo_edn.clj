(ns demo
  "A jackdaw demo app. Listens for commands to add/del items to a set."
  (:require [clojure.tools.logging :as log]
            [jackdaw.serdes.resolver :as resolver]
            [jackdaw.streams :as j])
  (:gen-class))

(def bootstrap-servers "localhost:9092")
(def app-config
  {"application.id"      "set-demo-app"
   "bootstrap.servers"   bootstrap-servers})

(def +topic-metadata+
  {:input
   {:topic-name "commands.edn"
    :partition-count 1
    :replication-factor 1
    :key-serde {:serde-keyword :jackdaw.serdes.edn/serde
                :key? true}
    :value-serde {:serde-keyword :jackdaw.serdes.edn/serde
                  :key? false}}
   :output
   {:topic-name "items.edn"
    :partition-count 1
    :replication-factor 1
    :key-serde {:serde-keyword :jackdaw.serdes.edn/serde
                :key? true}
    :value-serde {:serde-keyword :jackdaw.serdes.edn/serde
                  :key? false}}})

(def resolve-serde
  (resolver/serde-resolver))

(def topic-metadata
  (reduce-kv (fn [m k v]
               (assoc m k
                      (assoc v
                             :key-serde (resolve-serde (:key-serde v))
                             :value-serde (resolve-serde (:value-serde v)))))
             {}
             +topic-metadata+))

(def initialize (constantly {:items #{}}))

(defn aggregate
  [{:keys [items] :as agg} [k {:keys [op item] :as v}]]
  {:items (case op
            "add" (conj items item)
            "del" (disj items item)
            items)})

(defn topology-builder
  [topic-metadata]
  (fn [builder]
    (-> (j/kstream builder (:input topic-metadata))
        (j/peek (fn [[k v]] (log/info (str {:key k :value v}))))
        (j/group-by-key)
        (j/aggregate initialize
                     aggregate
                     (:output topic-metadata))
        (j/to-kstream)
        (j/to (:output topic-metadata)))
    builder))

(defn start-app
  "Starts the stream processing application."
  [topic-metadata app-config]
  (let [builder (j/streams-builder)
        topology ((topology-builder topic-metadata) builder)
        app (j/kafka-streams topology app-config)]
    (j/start app)
    (log/info "app is up")
    app))

(defn stop-app
  "Stops the stream processing application."
  [app]
  (j/close app)
  (log/info "app is down"))

(defn -main
  [& _]
  (start-app topic-metadata app-config))

;; REPL snippets
(comment
  ;; The following `require` is needed because the functons to reset
  ;; app state and produce and consume records are defined in the
  ;; `user` namespace but we want to evaluate them from this one.

  ;; Evaluate the form using `C-x C-e`:
  (require '[user :refer :all])

  ;; Start ZooKeeper and Kafka.
  ;; This requires the Confluent Platform CLI which may be obtained
  ;; from `https://www.confluent.io/download/`. If ZooKeeper and Kafka
  ;; are already running, skip this step.
  (confluent/start)

  ;; Evaluate the form using `C-c C-v C-f e`:
  (reset)

  (publish (:input topic-metadata) "key1" {:op "add" :item "squirrel"})

  (get-keyvals (:output topic-metadata))
)
