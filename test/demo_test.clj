(ns demo-test
  (:require [demo :as sut]
            [jackdaw.streams.mock :as jsm]
            [jackdaw.serdes.resolver :as resolver]
            [jackdaw.serdes.avro.schema-registry :as registry]
            [clojure.test :refer :all]))

(def resolve-serde
  (resolver/serde-resolver :schema-registry-url "fake"
                           :schema-registry-client (registry/mock-client)))

(def topic-metadata
  (reduce-kv (fn [m k v]
               (assoc m k (-> v
                              (update :key-serde resolve-serde)
                              (update :value-serde resolve-serde))))
             {}
             sut/+topic-metadata+))

(deftest demo-test
  ;; Note that jsm/build-driver makes use of org.apache.kafka.streams.TopologyTestDriver so we don't need an actual Kafka broker running.
  (let [driver (jsm/build-driver (sut/topology-builder topic-metadata))
        publish (partial jsm/publish driver)
        get-keyvals (partial jsm/get-keyvals driver)]

    (publish (:input topic-metadata) "k1" {:op :add :item "foo"})
    (publish (:input topic-metadata) "k1" {:op :add :item "bar"})
    (publish (:input topic-metadata) "k2" {:op :add :item "hello"})
    (publish (:input topic-metadata) "k2" {:op :add :item "world"})
    (publish (:input topic-metadata) "k1" {:op :add :item "baz"})
    (publish (:input topic-metadata) "k1" {:op :del :item "bar"})

    (is (= [["k1" {:items ["foo"]}]
            ["k1" {:items ["foo" "bar"]}]
            ["k2" {:items ["hello"]}]
            ["k2" {:items ["hello" "world"]}]
            ["k1" {:items ["foo" "bar" "baz"]}]
            ["k1" {:items ["foo" "baz"]}]]
           (get-keyvals (:output topic-metadata))))))
