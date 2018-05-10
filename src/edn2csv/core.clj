(ns edn2csv.core
  (require [clojure.core.reducers :as r]
           [clojure.edn :as edn]
           [clojure.java.io :as io]
           [clojure.pprint :as pp]
           [clojure.set :as set]
           [iota]
           [me.raynes.fs :as fs])
  (:gen-class))

(defn uuid [] (str (java.util.UUID/randomUUID)))

; The header line for the Individuals CSV file
(def individuals-header-line "UUID:ID(Individual),Generation:int,Location:int,:LABEL")
(def parent-edges-header ":START_ID(Individual),GeneticOperator,:END_ID(Individual),:TYPE")
(def semantics-header ":UUID:ID(Semantics),TotalError:int,:LABEL")
(def individual-semantics-header ":START_ID(Individual),:END_ID(Semantics),:TYPE")
(def error-header "UUID:ID(Error),ErrorValue:int,Position:int,:Label")
(def error-edge-header "START_ID(Semantics),:END_ID(Error),:TYPE")

; Ignores (i.e., returns nil) any EDN entries that don't have the
; 'clojure/individual tag.csv-
(defn individual-reader
    [t v]
    (when (= t 'clojush/individual) v))

; I got this from http://yellerapp.com/posts/2014-12-11-14-race-condition-in-clojure-println.html
; It prints in a way that avoids weird interleaving of lines and items.
; In several ways it would be better to use a CSV library like
; clojure.data.csv, but that won't (as written) avoid the interleaving
; problems, so I'm sticking with this approach for now.
(defn safe-println [output-stream & more]
  (.write output-stream (str (clojure.string/join "," more) "\n")))

; This prints out the relevant fields to the CSV filter
; and then returns 1 so we can count up how many individuals we processed.
; (The counting isn't strictly necessary, but it gives us something to
; fold together after we map this across the individuals; otherwise we'd
; just end up with a big list of nil's.)
(defn print-individual-to-csv
  [csv-file line]
  (as-> line $
    (map $ [:uuid :generation :location])
    (concat $ ["Individual"])
    (apply safe-println csv-file $))
  1)

(defn build-parent-edges-csv-filename
  [edn-filename]
  (str (fs/parent edn-filename)
  "/"
  (fs/base-name edn-filename ".edn")
  "_ParentOf_edges.csv"))

  (defn build-semantics-file-csv
    [edn-filename]
    (str (fs/parent edn-filename)
    "/"
    (fs/base-name edn-filename ".edn")
    "_Semantics.csv"))

  (defn build-semantics-edge-file
    [edn-filename]
    (str (fs/parent edn-filename)
    "/"
    (fs/base-name edn-filename ".edn")
    "_Individual_Semantics_edges.csv"))

    (defn build-errors-file
      [edn-filename]
      (str (fs/parent edn-filename)
      "/"
      (fs/base-name edn-filename ".edn")
      "_Errors.csv"))

      (defn build-error-edge-file
        [edn-filename]
        (str (fs/parent edn-filename)
        "/"
        (fs/base-name edn-filename ".edn")
        "Semantics_Error_edges.csv"))

(defn print-single-parent-edge [out-file child parent]
    (apply safe-println out-file (concat [parent] child)))

; prints all the parent edges for this individual to the out-file
(defn print-parent-edges [out-file edn-line]
    (as-> edn-line $
      (map $ [:genetic-operators :uuid])
      (concat $ ["PARENT_OF"])
      (doall (map (partial print-single-parent-edge out-file $)
                  (edn-line :parent-uuids)))))

(defn print-semantics-edge [out-file edn-line total-errors]
  (safe-println out-file
    (edn-line :uuid)
    (@total-errors [(edn-line :total-error) (edn-line :errors)])
    "HAS_SEMANTICS"))

(defn print-error-out [error-file error-edge-file semantic-uuid error pos]
  (let [error-uuid (uuid)]
    ;(prn "I was called!")
    (safe-println error-edge-file semantic-uuid error-uuid "HAS_ERROR")
    (safe-println error-file error-uuid error pos "Error")))

(defn print-error-vector [error-file error-edge-file error-vector semantic-uuid]
    (if (nil? error-vector)
      (prn "error vector was nil")
      (doall
        (pmap (partial print-error-out error-file error-edge-file semantic-uuid)
             error-vector (iterate inc 0)))))

(defn update-semantics [old-set new-error]
  (if (contains? old-set new-error)
     old-set
     (assoc old-set new-error (uuid))))

; Takes an atom, files to write to, and an edn object thing
; Writes the appropriate data to the appropriate files
; Returns 1
(defn read-and-write [total-errors out-file parent-edges-csv semantics-edge-file edn-line]
    (do
      (swap! total-errors update-semantics [(edn-line :total-error) (edn-line :errors)])
      (print-individual-to-csv out-file edn-line)
      (print-parent-edges parent-edges-csv edn-line)
      (print-semantics-edge semantics-edge-file edn-line total-errors)
      1))

(defn print-semantics [semantics-file semantic]
  (safe-println semantics-file
    (val semantic) (first (key semantic)) "Semantics"))

(defn print-semantics-and-error [semantics-file errors-file error-edge-file semantic]
  (do
    (print-semantics semantics-file semantic)
    (print-error-vector errors-file error-edge-file (second (key semantic)) (val semantic))))

(defn edn->csv-reducers [edn-file csv-file]
  (with-open [out-file (io/writer csv-file)
              parent-edges-file (io/writer (build-parent-edges-csv-filename edn-file))
              semantics-file (io/writer (build-semantics-file-csv edn-file))
              semantics-edge-file (io/writer (build-semantics-edge-file edn-file))
              errors-file (io/writer (build-errors-file edn-file))
              error-edge-file (io/writer (build-error-edge-file edn-file))]
    (safe-println out-file individuals-header-line)
    (safe-println parent-edges-file parent-edges-header)
    (safe-println semantics-file semantics-header)
    (safe-println semantics-edge-file individual-semantics-header)
    (safe-println errors-file error-header)
    (safe-println error-edge-file error-edge-header)

    (def total-errors (atom (hash-map)))

    (->>
      (iota/seq edn-file)
      (r/map (partial edn/read-string {:default individual-reader}))
      ; This eliminates empty (nil) lines, which result whenever
      ; a line isn't a 'clojush/individual. That only happens on
      ; the first line, which is a 'clojush/run, but we still need
      ; to catch it. We could do that with `r/drop`, but that
      ; totally kills the parallelism. :-(
      (r/filter identity)
        ;individuals-set
      (r/map (partial read-and-write total-errors out-file parent-edges-file semantics-edge-file))
      (r/fold +))

    (doall
        (pmap (partial print-semantics-and-error semantics-file errors-file error-edge-file) @total-errors))

      ;(prn @total-errors)

    ; reducer for ParentOf-edges.csv
    ;(->>(defn build-parent-edges-csv-filename
  ; [edn-filename]
  ; (str (fs/parent edn-filename)
  ; "/"
  ; (fs/base-name edn-filename ".edn")
  ; "_ParentOf_edges.csv"))
    ;    (iota/seq edn-file)
    ;    )
    ))

(defn build-individual-csv-filename
  [edn-filename strategy]
  (str (fs/parent edn-filename)
       "/"
       (fs/base-name edn-filename ".edn")
       (if strategy
         (str "_" strategy)
         "_reducers")
       "_Individuals.csv"))


(defn -main
  [edn-filename & [strategy]]
  (let [individual-csv-file (build-individual-csv-filename edn-filename strategy)]
    (time
      (condp = strategy
        (edn->csv-reducers edn-filename individual-csv-file))))
  ; Necessary to get threads spun up by `pmap` to shutdown so you get
  ; your prompt back right away when using `lein run`.
  (shutdown-agents))
