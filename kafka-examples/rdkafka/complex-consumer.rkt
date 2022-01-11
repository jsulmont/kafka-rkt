#lang racket

(require threading
         racket/async-channel
         ffi/unsafe
         try-catch-finally
         unix-signals
         kafka/rdkafka-ffi
         ;"ffi.rkt"
         )

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
(define verbose (make-parameter #t))
(define describe-group (make-parameter #f))
(define query-offsets (make-parameter #f))
(define raw-output (make-parameter #f))
(define properties (make-parameter (list)))
(define debug-flags (make-parameter (list)))
(define dump-conf (make-parameter #f))
(define subscription? (make-parameter #t))
(define exit-eof (make-parameter #f))
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

(define (describe-groups client group)
  (let-values ([(err g) (rd-kafka-list-groups client group 10000)])
    (unless (eq? err 'RD_KAFKA_RESP_ERR_NO_ERROR)
      (raise (format "% failed to acquire group list: ~a" (rd-kafka-err2str err))))
    (let* ([group-list* (ptr-ref g _rd-kafka-group-list)]
           [len (rd-kafka-group-list-group-cnt group-list*)]
           [group-list (cblock->list (rd-kafka-group-list-groups group-list*) _rd-kafka-group-info len)])
      (for ([group-info group-list])
        (let* ([broker (rd-kafka-group-info-broker group-info)]
               [group (rd-kafka-group-info-group group-info)]
               [err (rd-kafka-group-info-err group-info)]
               [state (rd-kafka-group-info-state group-info)]
               [proto-type (rd-kafka-group-info-proto-type group-info)]
               [protocol (rd-kafka-group-info-protocol group-info)]
               [members-cnt (rd-kafka-group-info-member-cnt group-info)])
          (displayln (format "\nGroup ~s in state ~a on broker ~a" group state (format-broker broker)))
          (when err (displayln (format " Error: ~a" (rd-kafka-err2str err))) )
          (displayln (format " Protocol type ~s, protocol ~s, with ~a member(s):"
                             proto-type protocol members-cnt))
          (when (positive? members-cnt)
            (let* ([members-ptr (rd-kafka-group-info-members group-info)]
                   [members (cblock->list members-ptr _rd-kafka-group-member-info members-cnt)])
              (for ([member members])
                (~> (format " ~s, client-id ~s on host /~a\n    metadata: ~a bytes\n    assignment: ~a bytes"
                        (rd-kafka-group-member-info-member-id member)
                        (rd-kafka-group-member-info-client-id member)
                        (rd-kafka-group-member-info-client-host member)
                        (rd-kafka-group-member-info-member-metadata-size member)
                        (rd-kafka-group-member-info-member-assignment-size member))
                (displayln))))))))
    (rd-kafka-group-list-destroy g)))

(define (format-topar-list partitions)
  (let ([partition-cnt (rd-kafka-topic-partition-list-cnt partitions)])
    (if (zero? partition-cnt) '()
        (let* ([partition-elms (rd-kafka-topic-partition-list-elems partitions)]
               [partition-lst (cblock->list partition-elms _rd-kafka-topic-partition partition-cnt)])
          (for/list ([p partition-lst])
            (match (rd-kafka-topic-partition-err p)
              ['RD_KAFKA_RESP_ERR_NO_ERROR
               (format "~a [~a] offset ~a"
                       (rd-kafka-topic-partition-topic p)
                       (rd-kafka-topic-partition-partition p)
                       (rd-kafka-topic-partition-offset p))]
              [err (format "~a [~a] error ~a"
                           (rd-kafka-topic-partition-topic p)
                           (rd-kafka-topic-partition-partition p)
                           (rd-kafka-err2str p))]))))))

#;(define format-date
    (match-lambda
      [(date* secs _ _ _ _ _ _ _ _ _ usec _)
       (format "~a.~a" secs usec)]))

;; TODO rewrite
(define (rebalance-cb client err partitions _)
  (display "% Consumer group rebalanced: ")
  (let ([error #f]
        [ret-err 'RD_KAFKA_RESP_ERR_NO_ERROR])

    (match err
      ['RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS
       (displayln (format "assigned (~a): ~s"
                          (rd-kafka-rebalance-protocol client)
                          (format-topar-list partitions)))
       (if (string=? (rd-kafka-rebalance-protocol client) "COOPERATIVE")
           (set! error (rd-kafka-incremental-assign client partitions))
           (set! ret-err (rd-kafka-assign client partitions)))
       (set! wait-eof (+ wait-eof (rd-kafka-topic-partition-list-cnt partitions)))]

      ['RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS
       (displayln (format "revoked  (~a): ~s"
                          (rd-kafka-rebalance-protocol client)
                          (format-topar-list partitions)))
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
      (displayln (format "% incremental assign failure: ~a" (rd-kafka-error-string error)))
      (rd-kafka-error-destroy error))

    (unless (equal? ret-err 'RD_KAFKA_RESP_ERR_NO_ERROR) ;; versus enum
      (displayln (format "% assign failure: ~a" (rd-kafka-err2str ret-err))))))

(define (make-partition-list lst)
  (let ([topics (rd-kafka-topic-partition-list-new (length lst))])
    (for ([topic lst])
      (let* ([seq (string-split topic ":")]
             [partition  (match (length seq)
                           [2 (begin (subscription? #f)
                                     (set! wait-eof (add1 wait-eof))
                                     (string->number (cadr seq)))]
                           [1 -1] [_ (raise (format "invalid topic list: " lst))])])
        (rd-kafka-topic-partition-list-add topics (car seq) partition)))
    topics))


(define (message-consume msg)
  (let* ([err (rd-kafka-message-err msg)]
         [topic (rd-kafka-message-rkt msg)])
    (match err
      [(or 'RD_KAFKA_RESP_ERR_NO_ERROR 'RD_KAFKA_RESP_ERR__PARTITION_EOF)
       (let ([topic-name (rd-kafka-topic-name topic)]
             [offset (rd-kafka-message-offset msg)]
             [partition (rd-kafka-message-partition msg)])
         (if (equal? err 'RD_KAFKA_RESP_ERR_NO_ERROR)
             (begin 
               (let ([key-len (rd-kafka-message-key-len msg)]
                     [len (rd-kafka-message-len msg)])
                 (when (verbose)
                   (displayln (format "% Message (topic ~s [~a] offset ~a, ~a bytes)"
                                      topic-name partition offset len)))
                 (when (positive? key-len)
                   (displayln (format "Key: ~a" ;; TODO -A (hexdump)
                                      (~> (rd-kafka-message-key msg)
                                          (bytes->string/latin-1 #f 0 key-len)))))
                 (displayln (format "~a"
                                    (~> (rd-kafka-message-payload msg)
                                        (bytes->string/latin-1 #f 0 len))))))
             (begin
               (displayln
                (format "% Consumer reached end of ~a [~a] message queue at offset ~a"
                        topic-name partition offset))
               #;(displayln (format "% ~a" (rd-kafka-message-errstr msg))) ;; gets you HWM
               (when (exit-eof)
                 (set! wait-eof (sub1 wait-eof))
                 (when (zero? wait-eof)
                   (displayln "% All partition(s) reached EOF: exiting")
                   (shutdown!))))))]
      [err
       (begin
         (if topic
             (displayln
              (format "% Consume error for topic ~s [~a] offset ~a: ~a"
                      (rd-kafka-topic-name topic)
                      (rd-kafka-message-partition msg)
                      (rd-kafka-message-offset msg)
                      (rd-kafka-message-errstr msg)))
             (displayln
              (format "% 3 Consumer error: ~a: ~a"
                      (rd-kafka-err2str err)
                      (rd-kafka-message-errstr msg)))))])))

(define topic-list
  (command-line
   #:program "complex-consumer"
   #:argv argument-vec
   #:once-each ["-g" g "Consumer group. ((null))" (group-id g)]
   ["-q" "Be quiet." (verbose #f)]
   ["-b" b "Broker address. (localhost:9092)" (brokers b)]
   ["-e" "Exit consumer when last message ∈ partition has been received." (exit-eof #t)]
   ["-D" "Describe group." (describe-group #t)]
   ["-O" "Get committed offset(s)." (query-offsets #t)]
   ["-A" "Raw payload output (consumer)." (raw-output #t)]
   #:multi
   ["-d" flag
         "set debug flag (all,generic,broker,topic...)."
         (debug-flags (cons flag (debug-flags)))]
   ["-X" property
         "Set arbitrary librdkafka configuration property (name=value)."
         (properties (cons property (properties)))]
   #:usage-help "For balanced consumer groups use the 'topic1 topic2..' format"
   #:usage-help "and for static assignment use 'topic1:part1 topic1:part2 topic2:part1..'"
   #:args (topic . topics)
   (cons topic topics)))

(try
 (let* ([errstr-len 256]
        [errstr (make-bytes errstr-len)]
        [table  (~> #hash()
                    (dict-set "log.queue" "true")
                    (dict-set "enable.partition.eof" "true")
                    (dict-set "bootstrap.servers" (brokers))
                    (dict-set "group.id" (or (group-id) "complex-consumer")))]
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

   (when (describe-group)
     (describe-groups client (group-id))
     (exit))

   (unless (null? (debug-flags))
     (set! log-queue (rd-kafka-queue-new client))
     (let ([err (rd-kafka-set-log-queue client log-queue)])
       (unless (equal? err 'RD_KAFKA_RESP_ERR_NO_ERROR)
         (display-error-exit "Failed to set log queue" err)))
     (let* ([logger (λ (client level fac msg)
                      (displayln (format "~a RDKAFKA-~a-~a: ~a: ~a "
                                         (/ (current-inexact-milliseconds) 1000)
                                         level fac (rd-kafka-memberid client) msg)))]
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
     (if (query-offsets)
         (match (rd-kafka-committed client topics 5000)
           ['RD_KAFKA_RESP_ERR_NO_ERROR
            (printf "committed: ~a\n" (format-topar-list topics)) ]
           [e (begin (eprintf "% Failed to fetch offsets: ~a\n" (rd-kafka-err2str e)) (exit 1))])

         (begin
           (if (subscription?)
               (begin
                 (displayln (format "% Subscribing to ~a topics"
                                    (rd-kafka-topic-partition-list-cnt topics)))
                 (let ([err (rd-kafka-subscribe client topics)])
                   (unless (equal? err  'RD_KAFKA_RESP_ERR_NO_ERROR)
                     (display-error-exit "Failed to start consuming topics" err))))
               (begin
                 (displayln (format "% Assigning ~a partitions"
                                    (rd-kafka-topic-partition-list-cnt topics)))
                 (let ([err (rd-kafka-assign client topics)])
                   (unless (equal? err  'RD_KAFKA_RESP_ERR_NO_ERROR)
                     (display-error-exit "Failed to start consuming topics" err)))))

           (let loop ([msg (rd-kafka-consumer-poll client 500)])
             (unless (ptr-equal? msg #f)
               (message-consume msg)
               (rd-kafka-message-destroy msg))
             (when (running?)
               (loop (rd-kafka-consumer-poll client 500))))))

     (displayln "% Shutting down")

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
    (displayln (format "%\tbrokers = ~a" (brokers)))
    (displayln (format "%\texit-eof = ~a" (exit-eof)))
    (displayln (format "%\tverbose = ~a" (verbose)))
    (displayln (format "%\tcommitted-offsets = ~a" (query-offsets)))
    (displayln (format "%\traw-output = ~a" (raw-output)))
    (displayln (format "%\tproperties = ~a" (properties)))
    (displayln (format "%\tdebug-flags = ~a" (debug-flags)))
    (displayln (format "%\ttopics = ~a" topic-list))))
