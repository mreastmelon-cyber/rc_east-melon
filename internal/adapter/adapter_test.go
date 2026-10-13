package adapter

import (
	"encoding/json"
	"net/url"
	"testing"

	pb "github.com/mreastmelon-cyber/right-capital-project/api/notification/v1"
	"github.com/mreastmelon-cyber/right-capital-project/internal/domain"
)

func TestVendorEncoding(t *testing.T) {
	e := &pb.Event{EventId: "evt-1", Data: &pb.Event_Registration{Registration: &pb.Registration{UserId: "u1", Channel: "ad-a"}}}
	r, err := Prepare(e, domain.Destination{Adapter: "ad.registration.v1", URL: "https://example.com"})
	if err != nil {
		t.Fatal(err)
	}
	form, err := url.ParseQuery(string(r.Body))
	if err != nil || form.Get("user_id") != "u1" || form.Get("event_id") != "evt-1" || r.ContentType != "application/x-www-form-urlencoded" {
		t.Fatal("incorrect registration encoding")
	}
	e.Data = &pb.Event_SubscriptionPayment{SubscriptionPayment: &pb.SubscriptionPayment{CustomerId: "c1", SubscriptionId: "s1", Status: "paid"}}
	r, err = Prepare(e, domain.Destination{Adapter: "crm.contact.v1"})
	if err != nil {
		t.Fatal(err)
	}
	var data map[string]string
	if json.Unmarshal(r.Body, &data) != nil || data["customer_id"] != "c1" || data["status"] != "paid" {
		t.Fatal("incorrect CRM encoding")
	}
	e.Data = &pb.Event_Purchase{Purchase: &pb.Purchase{OrderId: "o1", ProductId: "p1", Quantity: 3}}
	r, err = Prepare(e, domain.Destination{Adapter: "inventory.purchase.v1"})
	if err != nil {
		t.Fatal(err)
	}
	var purchase struct {
		Quantity uint32 `json:"quantity"`
	}
	if json.Unmarshal(r.Body, &purchase) != nil || purchase.Quantity != 3 {
		t.Fatal("incorrect quantity")
	}
	if _, err = Prepare(e, domain.Destination{Adapter: "crm.contact.v1"}); err == nil {
		t.Fatal("mismatched contract accepted")
	}
}
