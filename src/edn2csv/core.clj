(ns edn2csv.core
  (require [clojure.core.reducers :as r]
           [clojure.edn :as edn]
           [clojure.java.io :as io]
           [clojure.pprint :as pp]
           [clojure.set :as set]
           [iota]
           [me.raynes.fs :as fs])
  (:gen-class))

; The header line for the Individuals CSV file
(def individuals-header-line "UUID:ID(Individual),Generation:int,Location:int,:LABEL")
(def parent-edges-header ":START_ID(Individual),GeneticOperator,:END_ID(Individual),:TYPE")

; Ignores (i.e., returns nil) any EDN entries that don't have the
; 'clojure/individual tag.
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

(defn print-single-parent-edge [out-file parent]
    (safe-println out-file parent)
    )

(defn print-parent-edges [out-file edn-line]
    (map (partial print-single-parent-edge out-file) (map edn-line [:parent-uuids])))

;Takes an atom and an edn line
;unions the line into the atom' value and returns the line as a string
(defn read-and-set [indiv edn-line]
    (do
        (as-> edn-line $
            (edn/read-string {:default individual-reader} $)
            (swap! indiv (set/union @indiv #{$})))
        (edn/read-string {:default individual-reader} edn-line)))

(defn edn->csv-reducers [edn-file csv-file]
  (with-open [out-file (io/writer csv-file)
              parent-edges-file (io/writer (build-parent-edges-csv-filename    edn-file))]
    (safe-println out-file individuals-header-line)
    (safe-println parent-edges-file parent-edges-header)

    (def indiv (atom {}))

    (->>
      (iota/seq edn-file)
      (r/map (partial read-and-set indiv))
      ; This eliminates empty (nil) lines, which result whenever
      ; a line isn't a 'clojush/individual. That only happens on
      ; the first line, which is a 'clojush/run, but we still need
      ; to catch it. We could do that with `r/drop`, but that
      ; totally kills the parallelism. :-(
      (r/filter identity)
        ;individuals-set
      (r/map (partial print-individual-to-csv out-file))
      (r/fold +))

    (r/map (partial print-parent-edges parent-edges-file) @indiv)

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
