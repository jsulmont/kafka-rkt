#lang racket

(require threading
         racket/async-channel
         ffi/unsafe
         try-catch-finally
         unix-signals
         kafka/rdkafka-ffi
         ; "ffi.rkt"
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

(define paused #f)
(define (paused?) paused)

(define argument-vec
  (if (vector-empty? (current-command-line-arguments))
      (vector)
      (current-command-line-arguments)))

(define group-id (make-parameter #f))
(define brokers (make-parameter "localhost:9092"))
(define error-rate (make-parameter 10))
(define verbose (make-parameter #f))
(define properties (make-parameter (list)))
(define debug-flags (make-parameter (list)))
(define dump-conf (make-parameter #f))

(define (bytes->string bytes)
  (string-trim (bytes->string/latin-1 bytes) "\u0000" #:repeat? #t))

(define (set-conf conf key value errstr errstr-len)
  (let ([res (rd-kafka-conf-set conf key value errstr errstr-len)])
    (unless (eq? res 'RD_KAFKA_CONF_OK)
      (raise (bytes->string errstr)))))

(define (display-error-exit message err)
  (eprintf "% ERROR: ~a: ~a\n"  message (rd-kafka-err2str err))
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
     (let ([l (~> str
                  (string-normalize-spaces #px"\\s+" "")
                  (string-trim)
                  (string-split "="))])
       (cond
         [(equal? "dump" (car l)) (begin (dump-conf #t) r)]
         [(= 2 (length l)) (dict-set r (car l) (cadr l))]
         [else r])))
   result prop-strs))

(define (list->pairs lst)
  (if (and (list? lst) (even? (length lst)))
      (let loop ([l lst] [result '()])
        (if (null? l)
            (reverse result) ;; not using conj from data/collections
            (loop (cddr l) (cons (cons (car l) (cadr l)) result))))
      lst))

(define (format-topar-list partitions)
  (let ([partition-cnt (rd-kafka-topic-partition-list-cnt partitions)])
    (if (zero? partition-cnt) '()
        (let* ([partition-elms
                (rd-kafka-topic-partition-list-elems partitions)]
               [partition-lst
                (cblock->list partition-elms _rd-kafka-topic-partition partition-cnt)])
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

(define (rebalance/cb client err partitions _)
  (let ([error #f]
        [ret-err 'RD_KAFKA_RESP_ERR_NO_ERROR])

    (match err
      ['RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS
       (printf "Partition assigned (~a): ~s\n"
               (rd-kafka-rebalance-protocol client)
               (format-topar-list partitions))
       (if (string=? (rd-kafka-rebalance-protocol client) "COOPERATIVE")
           (set! error (rd-kafka-incremental-assign client partitions))
           (set! ret-err (rd-kafka-assign client partitions)))]

      ['RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS
       (printf "Partition revoked  (~a): ~s\n"
               (rd-kafka-rebalance-protocol client)
               (format-topar-list partitions))
       (if (string=? (rd-kafka-rebalance-protocol client) "COOPERATIVE")
           (set! error (rd-kafka-incremental-unassign client partitions))
           (set! ret-err (rd-kafka-assign client #f)))]

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
                           [2
                            (string->number (cadr seq))]
                           [1 -1]
                           [_ (raise (format "invalid topic list: " lst))])])
        (rd-kafka-topic-partition-list-add topics (car seq) partition)))
    topics))

(define (message->topar-offset msg)
  (unless (ptr-equal? msg #f)
    (let* ([topic (rd-kafka-message-rkt msg)]
           [topic-name (rd-kafka-topic-name topic)]
           [offset (rd-kafka-message-offset msg)]
           [p (rd-kafka-message-partition msg)])
      (values topic-name p offset))))

(define (external-system msg)
  (unless (ptr-equal? msg #f)
    (let-values ([(duration) (/ (random 1000) 1000.0)]
                 [(topic-name p offset) (message->topar-offset msg)])
      (printf
       "Simulating a ~a ms call to an external system for message ~a[~a]/~a\n"
       duration topic-name p offset)
      (sleep duration)
      (when (< (random 100) (error-rate))
        (raise "💥 Call to external system failed! 💀")))))

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
   ["-e" e "Error rate %. (10%)" (error-rate (string->number e))]
   #:multi
   ["-X" property
         "Set arbitrary librdkafka configuration property (name=value)."
         (properties (cons property (properties)))]
   #:usage-help
   "For balanced consumer groups use the 'topic1 topic2..' format"
   #:usage-help
   "and for static assignment use 'topic1:part1 topic1:part2 topic2:part1..'"
   #:args (topic)
   (list topic)))

(define (consume/batch client timeout [batch-size 10])
  ; (printf "XXX batch 1 to=~a\n" timeout)
  (let pool ([batch '()] [left-timeout timeout])
    (let* ([t1 (current-milliseconds)]
           [msg (rd-kafka-consumer-poll client left-timeout)]
           [left-timeout* (- left-timeout (- (current-milliseconds) t1))])
      ;     (printf "XXX batch 2 msg=~a\n" msg)
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
                     (shutdown!)
                     null)))]))))))



(define auto.offset.reset
  (let ([topic-conf (rd-kafka-topic-conf-new)])
    (let-values ([(res val)
                  (rd-kafka-topic-conf-get topic-conf "auto.offset.reset")])
      (rd-kafka-topic-conf-destroy topic-conf)
      val)))

(define (consumer/pause client partitions)
  (unless (paused?)
    (displayln "----->>> XXX PAUSING")
    (rd-kafka-pause-partitions client partitions)
    (set! paused #t)))

(define (consumer/resume client partitions)
  (when (paused?)
    (displayln "---->>> XXX RESUMING")
    (rd-kafka-resume-partitions client partitions)
    (set! paused #f)))


(define (consumer/rewind client partitions)
  (printf "---> REWIND BEFORE ~a\n" (format-topar-list partitions))
  (let ([partition-cnt (rd-kafka-topic-partition-list-cnt partitions)])
    (unless (zero? partition-cnt)
      (rd-kafka-committed client partitions 5000) ;; see what's committed
      (let* ([partition-elms (rd-kafka-topic-partition-list-elems partitions)]
             [partition-lst (cblock->list partition-elms _rd-kafka-topic-partition
                                          partition-cnt)])

        (for ([p partition-lst])
          (when (equal? (rd-kafka-topic-partition-offset p) RD_KAFKA_OFFSET_INVALID )
            (if (string=? auto.offset.reset "largest")
                (set-rd-kafka-topic-partition-offset! p RD_KAFKA_OFFSET_END)
                (set-rd-kafka-topic-partition-offset! p RD_KAFKA_OFFSET_BEGINNING)))))))
  (printf "---> REWIND AFTER ~a\n" (format-topar-list partitions))
  (let ([err (rd-kafka-seek-partitions client partitions 5000)])
    (when err
      (let ([errstr (rd-kafka-error-string err)])
        (rd-kafka-error-destroy err)
        (raise errstr)))))

(define (table->topar-list table)
  (let* ([topar-list (rd-kafka-topic-partition-list-new (hash-count table))])
    (for ([entry (hash->list table)])
      (match entry
        [(cons (cons topic part) offset)
         (begin
           (rd-kafka-topic-partition-list-add topar-list topic part)
           (rd-kafka-topic-partition-list-set-offset topar-list topic part offset))]))
    #;(printf "---> CREATED COMMIT LIST=~a\n" (format-topar-list topar-list))
    topar-list))

(try
 (let* ([errstr-len 256]
        [errstr (make-bytes errstr-len)]
        [table  (~> #hash()
                    (dict-set "log.queue" "true")
                    (dict-set "enable.partition.eof" "true")
                    (dict-set "enable.auto.commit" "false")
                    ;(dict-set "max.poll.interval.ms" "10000") ;; nope
                    (dict-set "bootstrap.servers" (brokers))
                    (dict-set "internal.termination.signal"
                              (format "~a" (lookup-signal-number 'SIGIO)))
                    (dict-set "group.id" (or (group-id) "infinite-retries-consumer")))]
        [table (if (null? (debug-flags))
                   table
                   (dict-set table "debug"
                             (foldl string-append "" (add-between (debug-flags) ","))))]
        [table (parse-properties (properties) table)]
        [conf (make-conf table errstr errstr-len)]
        [log-queue null] [log-thread null]
        [_ (rd-kafka-conf-set-rebalance-cb conf rebalance/cb)]
        [client (make-consumer conf errstr errstr-len)])

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

       (let-values ([(err partitions) (rd-kafka-assignment client)]
                    [(now) (current-inexact-milliseconds)])


         (unless (equal? err 'RD_KAFKA_RESP_ERR_NO_ERROR)
           (raise (format "couldn't fetch assignments: ~a" err)))

         (consumer/resume client partitions) ;; always call

         (try

          (map message-consume (reverse msgs))

          ;; we're good.
          (unless (empty? msgs)
            ;; let's figure offsets to be committed.
            (let* ([new-commits (make-hash)]
                   [f (λ (msg)
                        (let-values ([(topic p offset) (message->topar-offset msg)])
                          (let ([ref (hash-ref new-commits (cons topic p)
                                               (λ () RD_KAFKA_OFFSET_END))])
                            (when (>= offset ref)
                              (hash-set! new-commits (cons topic p) (add1 offset))))))]
                   [_ (map f msgs)]
                   [commit-list (table->topar-list new-commits)])
              ;; ... and commit.
              (let ([err (rd-kafka-commit client commit-list 0)])
                (unless (equal? err 'RD_KAFKA_RESP_ERR_NO_ERROR)
                  (raise (format  "% failed to commit: ~a" (rd-kafka-err2str err)))))
              (rd-kafka-topic-partition-list-destroy commit-list)))
          (catch (string? e)
            (eprintf "~a\n" e)
            (consumer/pause client partitions)
            (consumer/rewind client partitions))
          (finally (rd-kafka-topic-partition-list-destroy partitions))))
       (map rd-kafka-message-destroy msgs)
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
