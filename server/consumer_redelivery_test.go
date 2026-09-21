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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestConsumerRedeliveryLedgerAccounting(t *testing.T) {
	l := newRedeliveryLedger(3)

	// First deliveries and redeliveries are accounted separately.
	require_NoError(t, l.recordFirst(1))
	require_NoError(t, l.recordFirst(2))
	require_NoError(t, l.recordRedelivery(1, redeliveryReasonNak))
	require_NoError(t, l.recordRedelivery(1, redeliveryReasonTimeout))

	a := l.accounting()
	if a.FirstDeliveries != 2 || a.NakRedeliveries != 1 || a.TimeoutRedeliveries != 1 {
		t.Fatalf("unexpected accounting: %+v", a)
	}
	require_NoError(t, l.verify())

	// Redelivery without a recorded first delivery must fail and name the sequence.
	err := l.recordRedelivery(22, redeliveryReasonNak)
	require_Error(t, err)
	if !strings.Contains(err.Error(), "22") {
		t.Fatalf("error should identify the stream sequence, got: %v", err)
	}

	// Exceeding max_deliver must fail and name the sequence and the limit.
	err = l.recordRedelivery(1, redeliveryReasonTimeout)
	require_Error(t, err)
	if !strings.Contains(err.Error(), "max_deliver") || !strings.Contains(err.Error(), "1") {
		t.Fatalf("error should identify sequence and limit, got: %v", err)
	}

	// Duplicate first delivery after redeliveries is a mismatch.
	require_Error(t, l.recordFirst(1))

	// The last error must be visible in the accounting snapshot.
	if l.accounting().LastError == _EMPTY_ {
		t.Fatalf("expected last error to be surfaced in accounting")
	}

	// Drift between counters and entries must be reported.
	l.first++
	require_Error(t, l.verify())
}

func TestConsumerRedeliveryConfigAlignment(t *testing.T) {
	// Invalid max_deliver halts.
	if err := checkRedeliveryAccountingConfig(&ConsumerConfig{MaxDeliver: -2}); err == nil {
		t.Fatalf("expected max_deliver -2 to be rejected")
	}
	// Backoff that can never apply halts.
	if err := checkRedeliveryAccountingConfig(&ConsumerConfig{MaxDeliver: 1, BackOff: []time.Duration{time.Second}}); err == nil {
		t.Fatalf("expected backoff with max_deliver 1 to be rejected")
	}
	// Redelivery configured but no ack wait or backoff halts.
	if err := checkRedeliveryAccountingConfig(&ConsumerConfig{AckPolicy: AckExplicit, MaxDeliver: 5}); err == nil {
		t.Fatalf("expected missing ack_wait to be rejected")
	}
	// Aligned configs pass.
	if err := checkRedeliveryAccountingConfig(&ConsumerConfig{AckPolicy: AckExplicit, MaxDeliver: 5, AckWait: time.Second}); err != nil {
		t.Fatalf("expected aligned config to pass, got %v", err)
	}
	if err := checkRedeliveryAccountingConfig(&ConsumerConfig{AckPolicy: AckExplicit, AckWait: time.Second}); err != nil {
		t.Fatalf("expected default config to pass, got %v", err)
	}
}

func TestConsumerRedeliveryRecoveryUnit(t *testing.T) {
	// No stream available: recovery must fail with the reason.
	o := &consumer{cfg: ConsumerConfig{MaxDeliver: 3}, ledger: newRedeliveryLedger(3)}
	require_Error(t, o.recoverRedeliveryLedger())

	// Empty queue must not pretend to be recovered.
	o = &consumer{
		cfg:     ConsumerConfig{MaxDeliver: 3},
		ledger:  newRedeliveryLedger(3),
		mset:    &stream{},
		pending: map[uint64]*Pending{},
		sseq:    1,
	}
	require_NoError(t, o.recoverRedeliveryLedger())
	if o.ledger.recovered {
		t.Fatalf("empty queue must not be reported as recovered")
	}

	// Delivered but vanished messages must be reported, not silently skipped.
	o = &consumer{
		cfg:    ConsumerConfig{MaxDeliver: 3},
		ledger: newRedeliveryLedger(3),
		mset:   &stream{},
		sseq:   6,
		asflr:  2,
	}
	err := o.recoverRedeliveryLedger()
	require_Error(t, err)
	if !strings.Contains(err.Error(), "5") {
		t.Fatalf("error should identify the missing stream sequence, got: %v", err)
	}

	// Config drift between ledger and consumer config must halt recovery.
	o = &consumer{
		cfg:    ConsumerConfig{MaxDeliver: 7},
		ledger: newRedeliveryLedger(3),
		mset:   &stream{},
		sseq:   1,
	}
	require_Error(t, o.recoverRedeliveryLedger())

	// Over-limit redelivery state must be reported during recovery.
	o = &consumer{
		cfg:    ConsumerConfig{MaxDeliver: 2},
		ledger: newRedeliveryLedger(2),
		mset:   &stream{},
		sseq:   3,
		asflr:  0,
		rdc:    map[uint64]uint64{2: 5},
	}
	err = o.recoverRedeliveryLedger()
	require_Error(t, err)
	if !strings.Contains(err.Error(), "2") {
		t.Fatalf("error should identify the over-limit stream sequence, got: %v", err)
	}

	// Pending and redelivered messages are recovered and stay within limits.
	o = &consumer{
		cfg:    ConsumerConfig{MaxDeliver: 5},
		ledger: newRedeliveryLedger(5),
		mset:   &stream{},
		sseq:   4,
		asflr:  0,
		pending: map[uint64]*Pending{
			1: {Sequence: 1},
			2: {Sequence: 2},
			3: {Sequence: 3},
		},
		rdc: map[uint64]uint64{2: 1},
	}
	require_NoError(t, o.recoverRedeliveryLedger())
	a := o.ledger.accounting()
	if !a.Recovered || a.FirstDeliveries != 3 || a.TimeoutRedeliveries != 1 {
		t.Fatalf("unexpected recovered accounting: %+v", a)
	}
}

func TestConsumerRedeliveryAccountingDeliveryPath(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "T", Subjects: []string{"t"}})
	require_NoError(t, err)

	_, err = js.AddConsumer("T", &nats.ConsumerConfig{
		Durable:    "d",
		AckPolicy:  nats.AckExplicitPolicy,
		AckWait:    500 * time.Millisecond,
		MaxDeliver: 10,
	})
	require_NoError(t, err)

	for i := 0; i < 5; i++ {
		_, err := js.Publish("t", []byte(fmt.Sprintf("msg-%d", i)))
		require_NoError(t, err)
	}

	sub, err := js.PullSubscribe("t", "d")
	require_NoError(t, err)

	msgs, err := sub.Fetch(5, nats.MaxWait(2*time.Second))
	require_NoError(t, err)
	if len(msgs) != 5 {
		t.Fatalf("expected 5 messages, got %d", len(msgs))
	}
	// Two explicit NAKs, two acks, one left to expire via ack wait.
	require_NoError(t, msgs[0].Nak())
	require_NoError(t, msgs[1].Nak())
	require_NoError(t, msgs[2].AckSync())
	require_NoError(t, msgs[3].AckSync())

	// Pull mode only redelivers against a waiting request: fetch the two
	// NAKed messages, then the one that expires via ack wait.
	msgs, err = sub.Fetch(2, nats.MaxWait(3*time.Second))
	require_NoError(t, err)
	if len(msgs) != 2 {
		t.Fatalf("expected 2 nak redeliveries, got %d", len(msgs))
	}
	for _, m := range msgs {
		require_NoError(t, m.AckSync())
	}
	msgs, err = sub.Fetch(1, nats.MaxWait(3*time.Second))
	require_NoError(t, err)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 timeout redelivery, got %d", len(msgs))
	}
	require_NoError(t, msgs[0].AckSync())

	// Wait for the timeout redelivery of the unacked message and the
	// NAK redeliveries to be accounted.
	acc := s.GlobalAccount()
	mset, err := acc.lookupStream("T")
	require_NoError(t, err)
	o := mset.lookupConsumer("d")
	if o == nil {
		t.Fatalf("consumer not found")
	}

	checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
		o.mu.RLock()
		defer o.mu.RUnlock()
		a := o.ledger.accounting()
		if a.FirstDeliveries != 5 {
			return fmt.Errorf("expected 5 first deliveries, got %d", a.FirstDeliveries)
		}
		if a.NakRedeliveries != 2 {
			return fmt.Errorf("expected 2 nak redeliveries, got %d", a.NakRedeliveries)
		}
		if a.TimeoutRedeliveries < 1 {
			return fmt.Errorf("expected at least 1 timeout redelivery, got %d", a.TimeoutRedeliveries)
		}
		return o.verifyRedeliveryObservability()
	})

	// The accounting must be visible through the API/monitoring path.
	resp, err := nc.Request(fmt.Sprintf(JSApiConsumerInfoT, "T", "d"), nil, 2*time.Second)
	require_NoError(t, err)
	var info ConsumerInfo
	require_NoError(t, json.Unmarshal(resp.Data, &info))
	if info.Redelivery == nil {
		t.Fatalf("expected redelivery accounting in consumer info")
	}
	if info.Redelivery.FirstDeliveries != 5 || info.Redelivery.NakRedeliveries != 2 {
		t.Fatalf("unexpected observed accounting: %+v", info.Redelivery)
	}

	// Observability drift must be detected.
	o.mu.RLock()
	drifted := *info.Redelivery
	drifted.NakRedeliveries++
	err = o.ledger.verifyAgainst(&drifted)
	o.mu.RUnlock()
	require_Error(t, err)
	require_Error(t, o.ledger.verifyAgainst(nil))

	// Previously fine deliveries still flow.
	_, err = js.Publish("t", []byte("msg-5"))
	require_NoError(t, err)
	msgs, err = sub.Fetch(1, nats.MaxWait(2*time.Second))
	require_NoError(t, err)
	require_NoError(t, msgs[0].AckSync())
}

func TestConsumerRedeliveryRecoveryAfterOutage(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "T", Subjects: []string{"t"}})
	require_NoError(t, err)
	_, err = js.AddConsumer("T", &nats.ConsumerConfig{
		Durable:    "d",
		AckPolicy:  nats.AckExplicitPolicy,
		AckWait:    500 * time.Millisecond,
		MaxDeliver: 5,
	})
	require_NoError(t, err)

	for i := 0; i < 3; i++ {
		_, err := js.Publish("t", []byte(fmt.Sprintf("msg-%d", i)))
		require_NoError(t, err)
	}

	sub, err := js.PullSubscribe("t", "d")
	require_NoError(t, err)
	msgs, err := sub.Fetch(3, nats.MaxWait(2*time.Second))
	require_NoError(t, err)
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}
	// Ack one, leave two unacked so they must survive the outage.
	require_NoError(t, msgs[0].AckSync())
	nc.Close()

	// Simulate a full consumer outage.
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	// The two unacked messages must not be lost: they are redelivered after
	// recovery, bounded by the same max_deliver.
	sub, err = js.PullSubscribe("t", "d")
	require_NoError(t, err)
	msgs, err = sub.Fetch(2, nats.MaxWait(5*time.Second))
	require_NoError(t, err)
	if len(msgs) != 2 {
		t.Fatalf("expected 2 redelivered messages after recovery, got %d", len(msgs))
	}
	for _, m := range msgs {
		md, err := m.Metadata()
		require_NoError(t, err)
		if md.NumDelivered > 5 {
			t.Fatalf("delivery count %d exceeds max_deliver after recovery", md.NumDelivered)
		}
		require_NoError(t, m.AckSync())
	}

	// Recovery must be visible in the accounting and consistent with the ledger.
	acc := s.GlobalAccount()
	mset, err := acc.lookupStream("T")
	require_NoError(t, err)
	o := mset.lookupConsumer("d")
	if o == nil {
		t.Fatalf("consumer not found after restart")
	}
	o.mu.RLock()
	a := o.ledger.accounting()
	verr := o.verifyRedeliveryObservability()
	o.mu.RUnlock()
	if !a.Recovered {
		t.Fatalf("expected ledger to be marked recovered after outage, got %+v", a)
	}
	require_NoError(t, verr)
	// The two unacked messages must be recovered; the acked one is done.
	if a.FirstDeliveries < 2 {
		t.Fatalf("expected recovered first deliveries to cover the 2 unacked messages, got %+v", a)
	}
	if a.LastError != _EMPTY_ {
		t.Fatalf("unexpected recovery error: %v", a.LastError)
	}
}

func TestConsumerRedeliveryConfigAlignmentAPI(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "T", Subjects: []string{"t"}})
	require_NoError(t, err)

	// Misaligned config must halt consumer creation.
	_, err = js.AddConsumer("T", &nats.ConsumerConfig{
		Durable:    "bad",
		AckPolicy:  nats.AckExplicitPolicy,
		MaxDeliver: 1,
		BackOff:    []time.Duration{time.Second},
	})
	require_Error(t, err)

	// Aligned config is accepted.
	_, err = js.AddConsumer("T", &nats.ConsumerConfig{
		Durable:    "good",
		AckPolicy:  nats.AckExplicitPolicy,
		AckWait:    time.Second,
		MaxDeliver: 3,
	})
	require_NoError(t, err)

	// Behavior that drifts from the recorded config is reported.
	acc := s.GlobalAccount()
	mset, err := acc.lookupStream("T")
	require_NoError(t, err)
	o := mset.lookupConsumer("good")
	if o == nil {
		t.Fatalf("consumer not found")
	}
	o.mu.Lock()
	require_NoError(t, o.checkRedeliveryConfigAlignment())
	o.ledger.maxDeliver = 9
	err = o.checkRedeliveryConfigAlignment()
	o.mu.Unlock()
	require_Error(t, err)
	if !errors.Is(err, o.ledger.lastErr) {
		t.Fatalf("alignment failure must be recorded on the ledger")
	}
}
