(ns demo
  "A jackdaw demo app. Listens for commands to add/del items to a set."
  (:require [clojure.tools.logging :as log]
            [jackdaw.serdes.resolver :as resolver]
            [jackdaw.streams :as j]
            [clojure.data.json :as json])
  (:gen-class))

(def bootstrap-servers "localhost:9092")
(def schema-registry-url "http://localhost:8081")
(def app-config
  {"application.id"      "set-demo-app"
   "bootstrap.servers"   bootstrap-servers})

(def command-key-schema (json/write-str "string"))

(def command-value-schema (json/write-str {:type "record"
                                           :name "Demo"
                                           :fields [{:name "op"
                                                     :type {:type "enum"
                                                            :name "Operator"
                                                            :symbols ["add" "del"]}}
                                                    {:name "item"
                                                     :type "string"}]}))

(def item-set-key-schema (json/write-str "string"))

(def item-set-value-schema (json/write-str {:type "record"
                                           :name "DemoAgg"
                                           :fields [{:name "items"
                                                     :type {:type "array"
                                                            :items "string"}}]}))

(def +topic-metadata+
  {:input
   {:topic-name "commands"
    :partition-count 1
    :replication-factor 1
    :key-serde {:serde-keyword :jackdaw.serdes.avro.confluent/serde
                :schema command-key-schema
                :key? true}
    :value-serde {:serde-keyword :jackdaw.serdes.avro.confluent/serde
                  :schema command-value-schema
                  :key? false}}
   :output
   {:topic-name "items"
    :partition-count 1
    :replication-factor 1
    :key-serde {:serde-keyword :jackdaw.serdes.avro.confluent/serde
                :schema item-set-key-schema
                :key? true}
    :value-serde {:serde-keyword :jackdaw.serdes.avro.confluent/serde
                  :schema item-set-value-schema
                  :key? false}}})

(def resolve-serde
  (resolver/serde-resolver :schema-registry-url schema-registry-url))

(def topic-metadata
  (reduce-kv (fn [m k v]
               (assoc m k (-> v
                              (update :key-serde resolve-serde)
                              (update :value-serde resolve-serde))))
             {}
             +topic-metadata+))

(def initialize (constantly {:items []}))

(defn aggregate
  [{:keys [items] :as agg} [k {:keys [op item] :as v}]]
  {:items (case op
            :add (vec (conj (set items) item))
            :del (vec (disj (set items) item))
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
  ;; The following `require` is needed because the functions to reset
  ;; app state and produce and consume records are defined in the
  ;; `user` namespace but we want to evaluate them from this one.

  ;; Evaluate the form using `C-x C-e`:
  (require '[user :refer :all])

  ;; Start ZooKeeper and Kafka.
  ;; This requires the Confluent Platform CLI which may be obtained
  ;; from `https://www.confluent.io/download/`. If ZooKeeper and Kafka
  ;; are already running, skip this step.
  ;; Be sure the Confluent Platform's `bin` dir is in your `PATH`.
  (require 'confluent)
  (confluent/start)

  ;; reset calls `stop` then `start`
  ;; Note that `stop` deletes topics and state stores
  ;; Note that `start` stashes a reference to the app in `system/system`
  (reset)
  (stop)
  (start)

  ;; Examples of publishing some input records
  (publish (:input topic-metadata) "fruit" {:op :add :item "apple"})
  (publish (:input topic-metadata) "fruit" {:op :add :item "banana"})
  (publish (:input topic-metadata) "fruit" {:op :add :item "cherry"})
  (publish (:input topic-metadata) "fruit" {:op :del :item "banana"})
  (publish (:input topic-metadata) "fruit" {:op :add :item "strawberry"})

  ;; Get all the data from a topic
  (get-keyvals (:output topic-metadata))
  (get-keyvals (:input topic-metadata))
  )
