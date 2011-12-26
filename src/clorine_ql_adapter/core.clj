(ns clorine-ql-adapter.core
  ^{:doc "Clorine Adapter for ClojureQL"
    :authors "Paul Santa Clara"}
  (:require [clojureql.core                         :as ql]
            [clojure.java.jdbc                      :as sql])
  (:use [rn.clorine.core                            :only [get-connection
                                                           *curr-thread-connections*
                                                           *connection-registry*]]
        [clojure.java.jdbc.internal                 :only [*db*]])  
  (:import [org.apache.commons.dbcp  BasicDataSource]
           [rn.clorine RetriesExhaustedException]))

(defn with-connection* [conn-name func]
  (let [helper-fn
        #(let [[connection we-opened-it] (get-connection conn-name)]
           (binding [*db*
                     (merge
                      *db*
                      {:connection connection
                       :level      (get *db* :level 0)
                       :rollback   (get *db* :rollback (atom false))})]
             (try
               (func)
               (finally
                (if we-opened-it
                  (do
                    (swap! *curr-thread-connections* dissoc conn-name)
                    (.close connection)))))))]
    
    (if (nil? *curr-thread-connections*)
      (binding [*curr-thread-connections* (atom {})]
        (helper-fn))
      (helper-fn))))

(defmacro with-connection [conn-name & body]
  `(with-connection* ~conn-name (fn [] ~@body)))


(comment

  
  (rn.clorine.core/register-connection! :chicken
                                        {:driver-class-name  "org.postgresql.Driver"
                                         :db-port 5432
                                         :db-name "feedback_development"
                                         :user "some-user"
                                         :password "some-password"
                                         :url  "jdbc:postgresql://localhost:5432/feedback_development"
                                         })
  
  (use 'clojureql.predicates)
  (let [preds (partial and* (=* :id 4))
        preds (partial preds (<=* :created_at "20120101"))
        preds (preds (>* :updated_at "20111201") )]
    (ql/select (ql/table :users) preds))

  (with-connection :chicken
    (deref (ql/table :users)))

  (with-connection :chicken
    (sql/with-query-results rs ["select now();"]
      (doall rs)))

  (.close (get-in @*connection-registry* [ :chicken ]))


  )