package app

import (
	"testing"
	"time"

	pb "github.com/mreastmelon-cyber/right-capital-project/api/notification/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func validEvent() *pb.Event {
	return &pb.Event{EventId: "event-1", Source: "billing", EventType: "subscription.paid", SchemaVersion: 1, OccurredAt: timestamppb.New(time.Date(2026, 10, 12, 0, 0, 0, 123, time.UTC)), Data: &pb.Event_SubscriptionPayment{SubscriptionPayment: &pb.SubscriptionPayment{CustomerId: "c1", SubscriptionId: "s1", Status: "paid"}}}
}
func TestEventContract(t *testing.T) {
	e := validEvent()
	if ValidateEvent(e, "billing") != nil {
		t.Fatal("valid event rejected")
	}
	for _, mutate := range []func(*pb.Event){func(e *pb.Event) { e.Source = "other" }, func(e *pb.Event) { e.EventId = " bad" }, func(e *pb.Event) { e.SchemaVersion = 2 }, func(e *pb.Event) { e.OccurredAt = nil }, func(e *pb.Event) {
		e.Data = &pb.Event_Purchase{Purchase: &pb.Purchase{OrderId: "o1", ProductId: "p1", Quantity: 1}}
	}, func(e *pb.Event) { e.GetSubscriptionPayment().Status = "unpaid" }} {
		v := proto.Clone(e).(*pb.Event)
		mutate(v)
		if ValidateEvent(v, "billing") == nil {
			t.Fatal("invalid contract accepted")
		}
	}
}
func TestCanonicalHash(t *testing.T) {
	a := validEvent()
	_, hash, e := Canonical(a)
	if e != nil {
		t.Fatal(e)
	}
	b := proto.Clone(a).(*pb.Event)
	_, same, e := Canonical(b)
	if e != nil || same != hash {
		t.Fatal("equal event hashes differ")
	}
	b.GetSubscriptionPayment().CustomerId = "c2"
	_, different, e := Canonical(b)
	if e != nil || different == hash {
		t.Fatal("content difference ignored")
	}
}
