;;; SPDX-License-Identifier: LGPL-3.0-or-later
;;; SPDX-FileCopyrightText: Copyright © 2010-2021 Tony Garnock-Jones <tonyg@leastfixedpoint.com>

#lang info
(define collection "kafka-examples")
(define deps '("base"  "kafka"))
(define build-deps '("rackunit-lib" "racket-doc" "scribble-libs"))
(define version "0.1")
(define pkg-authors '(jans))
(define homepage "https://github.com/jsulmont/kafka-rkt")
(define license '(Apache-2.0 OR MIT))
