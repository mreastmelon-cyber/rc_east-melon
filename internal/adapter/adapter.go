package adapter

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"

	pb "github.com/mreastmelon-cyber/right-capital-project/api/notification/v1"
	"github.com/mreastmelon-cyber/right-capital-project/internal/domain"
)

func Prepare(e *pb.Event, d domain.Destination) (domain.Prepared, error) {
	r := domain.Prepared{Method: "POST", URL: d.URL, Headers: map[string]string{}, ContentType: "application/json"}
	for k, v := range d.Headers {
		r.Headers[k] = v
	}
	var data any
	switch d.Adapter {
	case "ad.registration.v1":
		v := e.GetRegistration()
		if v == nil {
			return r, fmt.Errorf("%w: registration required", domain.ErrInvalid)
		}
		r.ContentType = "application/x-www-form-urlencoded"
		r.Body = []byte(url.Values{"user_id": {v.UserId}, "channel": {v.Channel}, "event_id": {e.EventId}}.Encode())
		return r, nil
	case "crm.contact.v1":
		v := e.GetSubscriptionPayment()
		if v == nil {
			return r, fmt.Errorf("%w: payment required", domain.ErrInvalid)
		}
		data = map[string]string{"customer_id": v.CustomerId, "subscription_id": v.SubscriptionId, "status": v.Status}
	case "inventory.purchase.v1":
		v := e.GetPurchase()
		if v == nil {
			return r, fmt.Errorf("%w: purchase required", domain.ErrInvalid)
		}
		data = struct {
			OrderID   string `json:"order_id"`
			ProductID string `json:"product_id"`
			Quantity  uint32 `json:"quantity"`
		}{v.OrderId, v.ProductId, v.Quantity}
	default:
		return r, fmt.Errorf("%w: unknown adapter", domain.ErrInvalid)
	}
	var err error
	r.Body, err = json.Marshal(data)
	if len(r.Body) > 1<<20 {
		return r, domain.ErrInvalid
	}
	return r, err
}
func FieldValue(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case json.Number:
		return string(x)
	case bool:
		return strconv.FormatBool(x)
	default:
		return ""
	}
}
