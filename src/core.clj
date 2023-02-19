(ns core
  (:gen-class)
  (:require [missionary-mock :as r]))

(defn -main [& args]
  (println :result (r/run)))
