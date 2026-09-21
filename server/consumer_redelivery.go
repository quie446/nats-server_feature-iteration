// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"errors"
	"fmt"
)

// redeliveryReason identifies why a message is being delivered again.
type redeliveryReason uint8

const (
	// redeliveryReasonTimeout is a redelivery triggered by ack wait expiry.
	redeliveryReasonTimeout redeliveryReason = iota
	// redeliveryReasonNak is a redelivery triggered by an explicit NAK.
	redeliveryReasonNak
)

func (r redeliveryReason) String() string {
	switch r {
	case redeliveryReasonNak:
		return "nak"
	case redeliveryReasonTimeout:
		return "timeout"
	}
	return "unknown"
}

// RedeliveryAccounting is the observable snapshot of a consumer's redelivery
// ledger. First deliveries are accounted separately from redeliveries, and
// redeliveries are split by trigger (NAK vs ack timeout).
type RedeliveryAccounting struct {
	FirstDeliveries     uint64 `json:"first_deliveries"`
	NakRedeliveries     uint64 `json:"nak_redeliveries"`
	TimeoutRedeliveries uint64 `json:"timeout_redeliveries"`
	TrackedSequences    int    `json:"tracked_sequences"`
	MaxDeliver          int    `json:"max_deliver"`
	Recovered           bool   `json:"recovered"`
	LastError           string `json:"last_error,omitempty"`
}

// redeliveryLedgerEntry tracks per-stream-sequence delivery accounting.
type redeliveryLedgerEntry struct {
	first   uint64
	nak     uint64
	timeout uint64
}

func (e *redeliveryLedgerEntry) total() uint64 {
	return e.first + e.nak + e.timeout
}

// redeliveryLedger is the consumer's authoritative redelivery accounting.
// It is validated against the configured MaxDeliver limit and any mismatch
// is recorded (and surfaced) rather than silently dropped.
type redeliveryLedger struct {
	maxDeliver int
	entries    map[uint64]*redeliveryLedgerEntry
	first      uint64
	nak        uint64
	timeout    uint64
	recovered  bool
	lastErr    error
}

func newRedeliveryLedger(maxDeliver int) *redeliveryLedger {
	if maxDeliver < 0 {
		// Negative means unlimited in consumer config semantics.
		maxDeliver = 0
	}
	return &redeliveryLedger{
		maxDeliver: maxDeliver,
		entries:    make(map[uint64]*redeliveryLedgerEntry),
	}
}

// recordFirst accounts the first delivery of a stream sequence.
// A duplicate first delivery after redeliveries have been recorded is a
// mismatch and is reported, identifying the offending sequence.
func (l *redeliveryLedger) recordFirst(sseq uint64) error {
	e := l.entries[sseq]
	if e == nil {
		e = &redeliveryLedgerEntry{}
		l.entries[sseq] = e
	}
	if e.first > 0 {
		if e.nak == 0 && e.timeout == 0 {
			// Benign re-attempt of a first delivery that never completed.
			return nil
		}
		err := fmt.Errorf("redelivery accounting mismatch: duplicate first delivery for stream sequence %d after %d recorded redeliveries", sseq, e.nak+e.timeout)
		l.lastErr = err
		return err
	}
	e.first++
	l.first++
	return nil
}

// recordRedelivery accounts a redelivery of a stream sequence. It fails if
// no first delivery was recorded or if the configured MaxDeliver limit would
// be exceeded; the error identifies the exact sequence at fault.
func (l *redeliveryLedger) recordRedelivery(sseq uint64, reason redeliveryReason) error {
	e := l.entries[sseq]
	if e == nil || e.first == 0 {
		err := fmt.Errorf("redelivery accounting mismatch: %s redelivery for stream sequence %d without a recorded first delivery", reason, sseq)
		l.lastErr = err
		return err
	}
	if l.maxDeliver > 0 && e.total()+1 > uint64(l.maxDeliver) {
		err := fmt.Errorf("redelivery accounting mismatch: stream sequence %d would exceed max_deliver (%d)", sseq, l.maxDeliver)
		l.lastErr = err
		return err
	}
	switch reason {
	case redeliveryReasonNak:
		e.nak++
		l.nak++
	default:
		e.timeout++
		l.timeout++
	}
	return nil
}

// verify recomputes the totals from the per-sequence entries and reports any
// drift between the detailed accounting and the aggregate counters.
func (l *redeliveryLedger) verify() error {
	var first, nak, timeout uint64
	for sseq, e := range l.entries {
		if e.first == 0 && (e.nak > 0 || e.timeout > 0) {
			return fmt.Errorf("redelivery accounting drift: stream sequence %d has redeliveries but no first delivery", sseq)
		}
		if l.maxDeliver > 0 && e.total() > uint64(l.maxDeliver) {
			return fmt.Errorf("redelivery accounting drift: stream sequence %d exceeds max_deliver (%d)", sseq, l.maxDeliver)
		}
		first += e.first
		nak += e.nak
		timeout += e.timeout
	}
	if first != l.first || nak != l.nak || timeout != l.timeout {
		return fmt.Errorf("redelivery accounting drift: entries total (first=%d nak=%d timeout=%d) != counters (first=%d nak=%d timeout=%d)",
			first, nak, timeout, l.first, l.nak, l.timeout)
	}
	return nil
}

// accounting returns the observable snapshot of the ledger.
func (l *redeliveryLedger) accounting() *RedeliveryAccounting {
	a := &RedeliveryAccounting{
		FirstDeliveries:     l.first,
		NakRedeliveries:     l.nak,
		TimeoutRedeliveries: l.timeout,
		TrackedSequences:    len(l.entries),
		MaxDeliver:          l.maxDeliver,
		Recovered:           l.recovered,
	}
	if l.lastErr != nil {
		a.LastError = l.lastErr.Error()
	}
	return a
}

// verifyAgainst compares this ledger to an observed accounting snapshot
// (e.g. one surfaced through monitoring) and reports any drift.
func (l *redeliveryLedger) verifyAgainst(obs *RedeliveryAccounting) error {
	if obs == nil {
		return errors.New("redelivery observability drift: observed accounting is empty")
	}
	a := l.accounting()
	if a.FirstDeliveries != obs.FirstDeliveries ||
		a.NakRedeliveries != obs.NakRedeliveries ||
		a.TimeoutRedeliveries != obs.TimeoutRedeliveries ||
		a.TrackedSequences != obs.TrackedSequences {
		return fmt.Errorf("redelivery observability drift: ledger (first=%d nak=%d timeout=%d tracked=%d) != observed (first=%d nak=%d timeout=%d tracked=%d)",
			a.FirstDeliveries, a.NakRedeliveries, a.TimeoutRedeliveries, a.TrackedSequences,
			obs.FirstDeliveries, obs.NakRedeliveries, obs.TimeoutRedeliveries, obs.TrackedSequences)
	}
	return nil
}

// checkRedeliveryAccountingConfig validates that the redelivery-related
// consumer config fields line up with the existing consumer semantics. Any
// misalignment halts consumer creation rather than silently diverging.
func checkRedeliveryAccountingConfig(config *ConsumerConfig) *ApiError {
	if config.MaxDeliver < -1 {
		return NewJSStreamInvalidConfigError(fmt.Errorf("consumer max_deliver (%d) is invalid, must be -1 (unlimited) or >= 1", config.MaxDeliver))
	}
	// Ack flow control has its own dedicated validation for these fields.
	if len(config.BackOff) > 0 && config.MaxDeliver == 1 && config.AckPolicy != AckFlowControl {
		return NewJSStreamInvalidConfigError(errors.New("consumer backoff policy can never apply with max_deliver of 1"))
	}
	if config.AckPolicy != AckNone && config.AckPolicy != AckFlowControl && config.MaxDeliver != 1 && config.AckWait <= 0 && len(config.BackOff) == 0 {
		return NewJSStreamInvalidConfigError(errors.New("consumer redelivery accounting requires a positive ack_wait or an explicit backoff policy"))
	}
	return nil
}

// markRedeliveryReason records why a stream sequence is being queued for
// redelivery so the delivery path can account it against the right bucket.
// Lock should be held.
func (o *consumer) markRedeliveryReason(sseq uint64, reason redeliveryReason) {
	if o.rdReason == nil {
		o.rdReason = make(map[uint64]redeliveryReason)
	}
	o.rdReason[sseq] = reason
}

// recordDeliveryAccounting hooks the ledger into the existing delivery path.
// dc is the delivery count for this attempt (1 == first delivery). Accounting
// errors are recorded and surfaced through ConsumerInfo and the server log;
// deliveries that pass accounting are never held back.
// Lock should be held.
func (o *consumer) recordDeliveryAccounting(sseq, dc uint64) {
	if o.ledger == nil {
		return
	}
	var err error
	if dc <= 1 {
		err = o.ledger.recordFirst(sseq)
		delete(o.rdReason, sseq)
	} else {
		reason, ok := o.rdReason[sseq]
		if !ok {
			reason = redeliveryReasonTimeout
		}
		err = o.ledger.recordRedelivery(sseq, reason)
		delete(o.rdReason, sseq)
	}
	if err != nil && o.srv != nil {
		o.srv.Errorf("JetStream consumer '%s > %s > %s' %v", o.acc.Name, o.stream, o.name, err)
	}
}

// checkRedeliveryConfigAlignment verifies the ledger's configured limit still
// matches the consumer's effective config. Behavior that drifted from the
// recorded configuration is reported, not silently accepted.
// Lock should be held.
func (o *consumer) checkRedeliveryConfigAlignment() error {
	if o.ledger == nil {
		return errors.New("redelivery ledger not initialized")
	}
	effective := o.cfg.MaxDeliver
	if effective < 0 {
		effective = 0
	}
	if o.ledger.maxDeliver != effective {
		err := fmt.Errorf("redelivery config alignment mismatch: ledger max_deliver (%d) != consumer config max_deliver (%d)", o.ledger.maxDeliver, effective)
		o.ledger.lastErr = err
		return err
	}
	return nil
}

// recoverRedeliveryLedger rebuilds the redelivery accounting after a leader
// change or restart, using the restored consumer state. Messages that were
// pending or already redelivered must still be accounted for; redelivery
// after recovery is allowed but bounded by the same MaxDeliver limit. Any
// inconsistency (lost messages, over-limit entries, empty queue pretending
// to be recovered) is reported with its cause.
// Lock should be held.
func (o *consumer) recoverRedeliveryLedger() error {
	if o.ledger == nil {
		o.ledger = newRedeliveryLedger(o.cfg.MaxDeliver)
	}
	l := o.ledger
	if err := o.checkRedeliveryConfigAlignment(); err != nil {
		return err
	}
	if o.mset == nil {
		err := errors.New("redelivery recovery failed: stream not available")
		l.lastErr = err
		return err
	}

	// Rebuild accounting for messages still awaiting ack or redelivery.
	var recovered int
	for sseq := range o.pending {
		e := l.entries[sseq]
		if e == nil {
			e = &redeliveryLedgerEntry{}
			l.entries[sseq] = e
		}
		if e.first == 0 {
			e.first = 1
			l.first++
		}
		recovered++
	}
	for sseq, dc := range o.rdc {
		e := l.entries[sseq]
		if e == nil {
			e = &redeliveryLedgerEntry{first: 1}
			l.entries[sseq] = e
			l.first++
		}
		// Redeliveries recorded before the outage are attributed to the
		// timeout bucket; the original trigger is not persisted.
		for i := uint64(0); i < dc; i++ {
			if l.maxDeliver > 0 && e.total()+1 > uint64(l.maxDeliver) {
				err := fmt.Errorf("redelivery recovery failed: stream sequence %d exceeds max_deliver (%d)", sseq, l.maxDeliver)
				l.lastErr = err
				return err
			}
			e.timeout++
			l.timeout++
		}
		recovered++
	}

	// Detect lost messages: the stream shows deliveries beyond the ack floor
	// but nothing is pending or queued for redelivery anymore.
	if recovered == 0 && o.sseq > 1 && o.sseq-1 > o.asflr {
		err := fmt.Errorf("redelivery recovery failed: %d delivered messages (up to stream sequence %d) are neither pending, redeliverable nor acked", o.sseq-1-o.asflr, o.sseq-1)
		l.lastErr = err
		return err
	}
	// An empty queue is reported as not-recovered rather than claimed.
	l.recovered = recovered > 0
	return l.verify()
}

// redeliveryAccounting returns the consumer's current accounting snapshot,
// or nil if the ledger is not available.
// Lock should be held.
func (o *consumer) redeliveryAccounting() *RedeliveryAccounting {
	if o.ledger == nil {
		return nil
	}
	return o.ledger.accounting()
}

// verifyRedeliveryObservability checks that what monitoring exposes matches
// the authoritative ledger. Empty observability or drift is a failure.
// Lock should be held.
func (o *consumer) verifyRedeliveryObservability() error {
	if o.ledger == nil {
		return errors.New("redelivery observability failed: ledger not initialized")
	}
	if err := o.ledger.verify(); err != nil {
		return err
	}
	return o.ledger.verifyAgainst(o.redeliveryAccounting())
}
