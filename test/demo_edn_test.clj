(ns demo-edn-test
  (:require [demo-edn :as sut]
            [jackdaw.streams.mock :as jsm]
            [clojure.test :refer :all]))

(deftest demo-edn-test
  ;; Note that jsm/build-driver makes use of org.apache.kafka.streams.TopologyTestDriver so we don't need an actual Kafka broker running.
  (let [driver (jsm/build-driver (sut/topology-builder sut/topic-metadata))
        publish (partial jsm/publish driver)
        get-keyvals (partial jsm/get-keyvals driver)]

    (publish (:input sut/topic-metadata) "k1" {:op :add :item "foo"})
    (publish (:input sut/topic-metadata) "k1" {:op :add :item "bar"})
    (publish (:input sut/topic-metadata) "k2" {:op :add :item "hello"})
    (publish (:input sut/topic-metadata) "k2" {:op :add :item "world"})
    (publish (:input sut/topic-metadata) "k1" {:op :add :item "baz"})
    (publish (:input sut/topic-metadata) "k1" {:op :del :item "bar"})

    (is (= [["k1" {:items #{"foo"}}]
            ["k1" {:items #{"foo" "bar"}}]
            ["k2" {:items #{"hello"}}]
            ["k2" {:items #{"hello" "world"}}]
            ["k1" {:items #{"foo" "bar" "baz"}}]
            ["k1" {:items #{"foo" "baz"}}]]
           (get-keyvals (:output sut/topic-metadata))))))
