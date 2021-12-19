#lang racket

(require threading
         racket/async-channel
         ffi/unsafe
         try-catch-finally
         unix-signals
         "ffi.rkt")

(void (capture-signal! 'SIGINT))

(define signal-channel (make-async-channel))

(define signal-thunk
  (λ ()
    (let loop ()
    (define signum (read-signal))
    (printf "Received signal ~v (name ~v)\n" signum (lookup-signal-name signum))
    (async-channel-put signal-channel signum)
    (loop))))

(define _signal-thread (thread signal-thunk))

(define (running?)
  (not (async-channel-try-get signal-channel)))

(define (shutdown!)
  (async-channel-put signal-channel 'BOOM))

(define argument-vec
  (if (vector-empty? (current-command-line-arguments))
      #;(vector "-A" "-q" "-X" "partition.assignment.strategy=cooperative-sticky" "-g" "xoxo13" "transactions")
      (vector)
      (current-command-line-arguments)))

(define group-id (make-parameter #f))
(define brokers (make-parameter "localhost:9092"))
(define error-rate (make-parameter 10))
(define verbose (make-parameter #f))
(define properties (make-parameter (list)))
(define debug-flags (make-parameter (list)))
(define dump-conf (make-parameter #f))
(define wait-eof 0)

(define (bytes->string bytes)
  (string-trim (bytes->string/latin-1 bytes) "\u0000" #:repeat? #t))

(define (set-conf conf key value errstr errstr-len)
  (let ([res (rd-kafka-conf-set conf key value errstr errstr-len)])
    (unless (eq? res 'RD_KAFKA_CONF_OK)
      (raise (bytes->string errstr)))))

(define (display-error-exit message err)
  (displayln (format "% ERROR: ~a: ~a"  message (rd-kafka-err2str err)))
  (exit 1))

(define (make-conf table errstr errstr-len)
  (let ([conf (rd-kafka-conf-new)])
    (when (ptr-equal? conf #f)
      (raise "Failed to allocate conf"))
    (for ([(key value) (in-hash table)])
      (set-conf conf key value errstr errstr-len))
    conf))

(define (make-consumer conf errstr errstr-len)
  (let ([consumer (rd-kafka-new 'RD_KAFKA_CONSUMER conf errstr errstr-len)])
    (when (ptr-equal? consumer #f)
      (rd-kafka-conf-destroy conf)
      (raise (bytes->string errstr)))
    (rd-kafka-poll-set-consumer consumer)
    consumer))

(define (parse-properties prop-strs result)
  (foldl
   (λ (str r)
     (let ([l (~> str (string-normalize-spaces #px"\\s+" "") (string-trim) (string-split "="))])
       (cond
         [(equal? "dump" (car l)) (begin (dump-conf #t) r)]
         [(= 2 (length l)) (dict-set r (car l) (cadr l))]
         [else r])))
   result prop-strs))

(define (display-conf conf msg)
  (let ([properties (~> (rd-kafka-conf-dump conf) (list->pairs))])
    (displayln msg)
    (for ([property properties])
      (displayln (format "\t~a=~a" (car property) (cdr property))))))

(define (list->pairs lst)
  (if (and (list? lst) (even? (length lst)))
      (let loop ([l lst] [result '()])
        (if (null? l)
            (reverse result) ;; not using conj from data/collections
            (loop (cddr l) (cons (cons (car l) (cadr l)) result))))
      lst))

(define (format-broker broker)
  (format "broker ~a (~a:~a)"
          (rd-kafka-metadata-broker-id broker)
          (rd-kafka-metadata-broker-host broker)
          (rd-kafka-metadata-broker-port broker)))

(define (format-topar-list partitions)
  (let ([partition-cnt (rd-kafka-topic-partition-list-cnt partitions)])
    (if (zero? partition-cnt) '()
        (let* ([partition-elms (rd-kafka-topic-partition-list-elems partitions)]
             [partition-lst (cblock->list partition-elms _rd-kafka-topic-partition partition-cnt)])
        (for/list ([p partition-lst])
          (format "~a [~a] offset ~a"
                  (rd-kafka-topic-partition-topic p)
                  (rd-kafka-topic-partition-partition p)
                  (rd-kafka-topic-partition-offset p)))))))

; Racket:
(define (partition/callback p? lst k)
  (match lst
    ['() (k '() '())]
    [(cons hd tl)
     (partition/callback p? tl (λ (ins outs)
                                 (if (p? hd)
                                     (k (cons hd ins) outs)
                                     (k ins (cons hd outs)))))]))

;; TODO rewrite
(define (rebalance-cb client err partitions _)
  (display "% Consumer group rebalanced: ")
  (let ([error #f]
        [ret-err 'RD_KAFKA_RESP_ERR_NO_ERROR])

    (match err
      ['RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS
       (printf "% Partition assigned (~a): ~s\n"
               (rd-kafka-rebalance-protocol client)
               (format-topar-list partitions))
       (if (string=? (rd-kafka-rebalance-protocol client) "COOPERATIVE")
           (set! error (rd-kafka-incremental-assign client partitions))
           (set! ret-err (rd-kafka-assign client partitions)))
       (set! wait-eof (+ wait-eof (rd-kafka-topic-partition-list-cnt partitions)))]

      ['RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS
       (printf "% Partition revoked  (~a): ~s\n"
               (rd-kafka-rebalance-protocol client)
               (format-topar-list partitions))
       (if (string=? (rd-kafka-rebalance-protocol client) "COOPERATIVE")
           (begin
             (set! error (rd-kafka-incremental-unassign client partitions))
             (set! wait-eof (- wait-eof (rd-kafka-topic-partition-list-cnt partitions))))
           (begin
             (set! ret-err (rd-kafka-assign client #f))
             (set! wait-eof 0)))]

      [_ (displayln "failed: ~a" (rd-kafka-err2str err))
         (rd-kafka-assign client #f)])

    (when error ;; error object
      (eprintf "% incremental assign failure: ~a\n" (rd-kafka-error-string error))
      (rd-kafka-error-destroy error))

    (unless (equal? ret-err 'RD_KAFKA_RESP_ERR_NO_ERROR) ;; versus enum
      (eprintf "% assign failure: ~a\n" (rd-kafka-err2str ret-err)))))

(define (make-partition-list lst)
  (let ([topics (rd-kafka-topic-partition-list-new (length lst))])
    (for ([topic lst])
      (let* ([seq (string-split topic ":")]
             [partition  (match (length seq)
                           [2 (string->number (cadr seq))]
                           [1 -1] [_ (raise (format "invalid topic list: " lst))])])
        (rd-kafka-topic-partition-list-add topics (car seq) partition)))
    topics))


(define (external-system msg)
  (unless (ptr-equal? msg #f)
    (let* ([duration (/ (random 1000) 1000.0)]
           [topic (rd-kafka-message-rkt msg)]
           [topic-name (rd-kafka-topic-name topic)]
           [offset (rd-kafka-message-offset msg)]
           [p (rd-kafka-message-partition msg)])
      (printf "Simulating a ~a ms call to an external system for message ~a[~a]/~a\n"
              duration topic-name p offset)
      (sleep duration)
      (when (< (random 100) (error-rate))
        (raise "💥💥 Call to external system failed!")))))

(define (message-consume msg)
  (let* ([key-len (rd-kafka-message-key-len msg)]
         [len (rd-kafka-message-len msg)])

    (when (positive? key-len)
      (display (format "~a  " (~> (rd-kafka-message-key msg)
                                  (bytes->string/latin-1 #f 0 key-len)))))
    (external-system msg)))

(define topic-list
  (command-line
   #:program "infinite-retries-consumer"
   #:argv argument-vec
   #:once-each ["-g" g "Consumer group. ((null))" (group-id g)]
   ["-v" "Be verbose." (verbose #t)]
   ["-b" b "Broker address. (localhost:9092)" (brokers b)]
   ["-e" e "Error rate %. (10%)" (error-rate e)]
   #:multi
   ["-d" flag
         "set debug flag (all,generic,broker,topic...)."
         (debug-flags (cons flag (debug-flags)))]
   ["-X" property
         "Set arbitrary librdkafka configuration property (name=value)."
         (properties (cons property (properties)))]
   #:usage-help "For balanced consumer groups use the 'topic1 topic2..' format"
   #:usage-help "and for static assignment use 'topic1:part1 topic1:part2 topic2:part1..'"
   #:args (topic)
   (list topic)))

(define (consume/batch client timeout [batch-size 10])
  (let pool ([batch '()] [left-timeout timeout])
    (let* ([t1 (current-milliseconds)]
           [msg (rd-kafka-consumer-poll client left-timeout)]
           [left-timeout* (- left-timeout (- (current-milliseconds) t1))])
      (if (not msg)
          batch
          (let ([err (rd-kafka-message-err msg)])
            (case err
              ['RD_KAFKA_RESP_ERR_NO_ERROR
               (let ([batch* (cons msg batch)])
                 (if (and (positive? left-timeout*)
                          (< (length batch*) batch-size))
                     (pool batch* left-timeout*)
                     batch*))]
              [else
               (begin
                 (rd-kafka-message-destroy msg)
                 (if (equal? err 'RD_KAFKA_RESP_ERR__PARTITION_EOF)
                   (if (positive? left-timeout)
                       (pool batch left-timeout)
                       batch)
                   (begin
                     (eprintf "% Consumer error: ~a\n" (rd-kafka-err2str err))
                     (shutdown!))))]))))))

(try
 (let* ([errstr-len 256]
        [errstr (make-bytes errstr-len)]
        [table  (~> #hash()
                    (dict-set "log.queue" "true")
                    (dict-set "enable.partition.eof" "true")
                    ;(dict-set "max.poll.interval.ms" "10000") ;; nope
                    (dict-set "bootstrap.servers" (brokers))
                    (dict-set "internal.termination.signal" (format "~a" (lookup-signal-number 'SIGIO)))
                    (dict-set "group.id" (or (group-id) "infinite-retries-consumer")))]
        [table (if (null? (debug-flags))
                   table
                   (dict-set table "debug" (foldl string-append "" (add-between (debug-flags) ","))))]
        [table (parse-properties (properties) table)]
        [conf (make-conf table errstr errstr-len)]
        [log-queue null] [log-thread null]
        [_ (rd-kafka-conf-set-rebalance-cb conf rebalance-cb)]
        [client (make-consumer conf errstr errstr-len)])

   (when (dump-conf)
     (display-conf conf "# Global properties")
     (exit))

   (unless (null? (debug-flags))
     (set! log-queue (rd-kafka-queue-new client))
     (let ([err (rd-kafka-set-log-queue client log-queue)])
       (unless (equal? err 'RD_KAFKA_RESP_ERR_NO_ERROR)
         (display-error-exit "Failed to set log queue" err)))
     (let* ([logger (λ (client level fac msg)
                      (displayln (format "~a RDKAFKA-~a-~a: ~a: ~a "
                                         (/ (current-inexact-milliseconds) 1000)
                                         level fac (rd-kafka-name client) msg)))]
            [logger-thunk (λ ()
                            (let loop ([evt (rd-kafka-queue-poll log-queue 200)])
                              (unless (equal? evt #f)
                                (let-values ([(rc fac str level) (rd-kafka-event-log evt)])
                                  (when (zero? rc) (logger client level fac str)))
                                (rd-kafka-event-destroy evt))
                              (when (running?)
                                (loop (rd-kafka-queue-poll log-queue 200)))))])
       (set! log-thread (thread logger-thunk))))

   (let* ([topics (make-partition-list topic-list)])
     (printf "% Subscribing to ~a topics"
             (rd-kafka-topic-partition-list-cnt topics))

     (let ([err (rd-kafka-subscribe client topics)])
       (unless (equal? err  'RD_KAFKA_RESP_ERR_NO_ERROR)
         (display-error-exit "Failed to start consuming topics" err)))

     (match/values (rd-kafka-subscription client)
       [('RD_KAFKA_RESP_ERR_NO_ERROR partitions)
        (printf "\nHAPPY ~a\n"  (format-topar-list partitions))]
       [(err _) (eprintf "ERROR ~a\n" (rd-kafka-err2str err))])

     
     (let loop ([msgs (consume/batch client 1000)])
       (unless (empty? msgs)
         (try
          (map message-consume (reverse msgs))
          (catch (string? e)
            (eprintf "~a\n" e)))
         (map rd-kafka-message-destroy msgs))
       (when (running?)
         (loop (consume/batch client 1000))))

     (displayln "% Shutting down")

     (rd-kafka-topic-partition-list-destroy topics)
     (displayln "% Partition list destroyed")

     (unless (null? (debug-flags))
       (kill-thread log-thread)
       (rd-kafka-queue-destroy log-queue)
       (displayln "% log-queue destroyed"))

     (match (rd-kafka-consumer-close client)
       ['RD_KAFKA_RESP_ERR_NO_ERROR  (displayln "% Consumer closed")]
       [err (displayln (format "% Failed to close consumer: ~a"
                               (rd-kafka-err2str err)))])

     (rd-kafka-destroy client)

     (let loop ([run 10]
                [rc (rd-kafka-wait-destroyed 1000)])
       (unless (or (zero? run) (zero? rc))
         (displayln "Waiting for librdkafka to decommission")
         (loop (sub1 run) (rd-kafka-wait-destroyed 1000))))))

 (catch
     (string? e) (displayln (format "% string ~a" e))))

(when (verbose)
  (begin
    (displayln "% Running arguments:")
    (displayln (format "%\tgroup.id = ~a" (group-id)))
    (displayln (format "%\terror rate = ~a" (error-rate)))
    (displayln (format "%\tbrokers = ~a" (brokers)))
    (displayln (format "%\tverbose = ~a" (verbose)))
    (displayln (format "%\tproperties = ~a" (properties)))
    (displayln (format "%\tdebug-flags = ~a" (debug-flags)))
    (displayln (format "%\ttopics = ~a" topic-list))))
