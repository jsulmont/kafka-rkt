#lang racket

(require
 threading
 ffi/unsafe
 try-catch-finally
 "ffi.rkt")

(define argument-vec
  (if (vector-empty? (current-command-line-arguments))
      (vector "-q" "-A" "-X" "bootstrap.servers=localhost:9092"  "transactions:0")
      (current-command-line-arguments)))

(define consumer-group (make-parameter "rdkafka-consumer-example"))
(define brokers (make-parameter "localhost:9092"))
(define exit-eof (make-parameter #f))
(define verbose (make-parameter #t))
(define describe-group (make-parameter #f))
(define committed-offsets (make-parameter #f))
(define raw-output (make-parameter #f))
(define properties (make-parameter (list)))
(define debug-flags (make-parameter (list)))
(define dump-conf (make-parameter #f))

(define topic-list
  (command-line
   #:program "complex-consumer"
   #:argv argument-vec
   #:once-each
   ["-g" g "Consumer group. ((null))" (consumer-group g)]
   ["-q" "Be quiet." (verbose #f)]
   ["-b" b "Broker address. (localhost:9092)" (brokers b)]
   ["-e" "Exit consumer when last message ∈ partition has been received." (exit-eof #t)]
   ["-D" "Describe group." (describe-group #t)]
   ["-O" "Get committed offset(s)." (committed-offsets #t)]
   ["-A" "Raw payload output (consumer)." (raw-output #t)]
   #:multi
   ["-d" flag "set debug flag (all,generic,broker,topic...)."
         (debug-flags (cons flag (debug-flags)))]
   ["-X" property "Set arbitrary librdkafka configuration property (name=value)."
         (properties (cons property (properties)))]
   #:usage-help "For balanced consumer groups use the 'topic1 topic2..' format"
   #:usage-help "and for static assignment use 'topic1:part1 topic1:part2 topic2:part1..'"
   #:args  (topic . topics)
   (cons topic  topics)))

(define (bytes->string bytes)
  (string-trim (bytes->string/latin-1 bytes) "\u0000" #:repeat? #t))

(define (set-conf conf key value errstr errstr-len)
  (let ([res (rd-kafka-conf-set conf key value errstr errstr-len)])
    (unless (eq? res 'RD_KAFKA_CONF_OK)
      (rd-kafka-conf-destroy conf)
      (raise (bytes->string errstr)))))

(define (make-conf table errstr errstr-len)
  (let ([conf (rd-kafka-conf-new)])
    (when (ptr-equal? conf #f)
      (raise "Failed to allocate conf"))
    (for ([(key value) (in-hash table)])
      (set-conf conf key value errstr errstr-len))
    conf))

(when (verbose)
  (begin
      (displayln "%% Running arguments:")
      (displayln (format "%%\tconsumer-group = ~a" (consumer-group)))
      (displayln (format "%%\tbrokers = ~a" (brokers)))
      (displayln (format "%%\texit-eof = ~a" (exit-eof)))
      (displayln (format "%%\tverbose = ~a" (verbose)))
      (displayln (format "%%\tdescribe-group = ~a" (describe-group)))
      (displayln (format "%%\tcommitted-offsets = ~a" (committed-offsets)))
      (displayln (format "%%\traw-output = ~a" (raw-output)))
      (displayln (format "%%\tproperties = ~a" (properties)))
      (displayln (format "%%\tdebug-flags = ~a" (debug-flags)))
      (displayln (format "%%\ttopics = ~a" topic-list))))


(define (parse-properties prop-strs result)
  (foldl (λ (str r) (let ([l (~> str
                                 (string-normalize-spaces #px"\\s+" "")
                                 (string-trim)(string-split "="))])
                      (cond
                        [(equal? "dump" (car l)) (begin (dump-conf #t) r)]
                        [(= 2 (length l)) (dict-set r (car l) (cadr l))]
                        [else r])))
         result prop-strs))

(define (display-conf conf msg)
  (let ([properties (~> (rd-kafka-conf-dump conf)
                        (list->pairs))])
    (displayln msg)
    (for ([property properties])
      (displayln (format "\t~a=~a" (car property) (cdr property))))))

(define (list->pairs lst)
  (if (and (list? lst) (even? (length lst)))
      (let loop ([l lst] [result '()])
        (if (null? l)
            (reverse result)  ;; not using conj from data/collections
            (loop (cddr l) (cons (cons (car l) (cadr l)) result))))
      lst))

(try
 (let* ([errstr-len 256]
        [errstr (make-bytes errstr-len)]
        [table (~> #hash()
                   (dict-set "bootstrap.servers" (brokers))
                   (dict-set "group.id" (consumer-group)))]
        [table (if (null? (debug-flags)) table
                   (dict-set table "debug" (foldl string-append "" (add-between (debug-flags) ","))))]
        [table (parse-properties (properties) table)]
        [conf (make-conf table errstr errstr-len)]
        #;[topic-conf (rd-kafka-conf-get-default-topic-conf conf)])
   (displayln (ptr-equal? conf #f))
   (when (dump-conf)
     (display-conf conf "# Global properties")
     #;(unless (ptr-equal? topic-conf #f)
         (display-conf topic-conf "# Topic specific properties"))


     (exit)))
 (catch  (string? e)  (displayln (format "%% string ~a" e))))

(begin
  (define errstr (make-bytes 256))
  (define table (~> #hash()
                    (dict-set "bootstrap.servers" (brokers))
                    (dict-set "group.id" (consumer-group))))
  (define conf (make-conf table errstr 256)))
