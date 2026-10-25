// Package crypto provides payload encryption hooks for PII at rest (design §6.4).
// GDPR product path is tombstone-only (Q5) — this package does NOT implement crypto-shredding.
package crypto

import (
	"context"
	"encoding/json"
)

// PayloadProtector encrypts selected JSON fields before event/command insert
// and decrypts on load. Prefer envelope encryption (DEK/KEK) with an external KMS.
type PayloadProtector interface {
	Protect(ctx context.Context, eventType string, payload json.RawMessage) (json.RawMessage, error)
	Unprotect(ctx context.Context, eventType string, payload json.RawMessage) (json.RawMessage, error)
}

// Noop leaves payloads unchanged.
type Noop struct{}

func (Noop) Protect(_ context.Context, _ string, payload json.RawMessage) (json.RawMessage, error) {
	return payload, nil
}
func (Noop) Unprotect(_ context.Context, _ string, payload json.RawMessage) (json.RawMessage, error) {
	return payload, nil
}

// FieldEncryptor is a helper shape for apps that encrypt named JSON object fields.
// The framework does not ship a KMS implementation — wire your own.
type FieldEncryptor interface {
	EncryptField(ctx context.Context, plaintext []byte) (ciphertext []byte, err error)
	DecryptField(ctx context.Context, ciphertext []byte) (plaintext []byte, err error)
}

// WrapFields builds a PayloadProtector that encrypts the listed object keys in-place
// (values become {"__enc":"<base64>"}). Requires a FieldEncryptor.
type WrapFields struct {
	Encryptor FieldEncryptor
	Fields    map[string][]string // eventType → field names
}

// Note: WrapFields methods are left as documentation stubs — apps should copy
// or extend with their encoding scheme. Use Noop until a KMS is wired.
