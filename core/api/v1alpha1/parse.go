package v1alpha1

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// ParseKind reads just the TypeMeta.Kind from a YAML document so callers
// (e.g. the control plane's apply endpoint) can dispatch to the right typed
// decoder without knowing the kind up front.
func ParseKind(raw []byte) (string, error) {
	var meta TypeMeta
	if err := yaml.Unmarshal(raw, &meta); err != nil {
		return "", fmt.Errorf("parsing kind: %w", err)
	}
	if meta.Kind == "" {
		return "", fmt.Errorf("missing required field: kind")
	}
	return meta.Kind, nil
}

// DecodeConnection parses and validates a Connection document.
func DecodeConnection(raw []byte) (*Connection, error) {
	var c Connection
	if err := yaml.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("decoding Connection: %w", err)
	}
	if err := ValidateConnection(&c); err != nil {
		return nil, err
	}
	return &c, nil
}

// DecodeSync parses and validates a Sync document.
func DecodeSync(raw []byte) (*Sync, error) {
	var s Sync
	if err := yaml.Unmarshal(raw, &s); err != nil {
		return nil, fmt.Errorf("decoding Sync: %w", err)
	}
	if err := ValidateSync(&s); err != nil {
		return nil, err
	}
	return &s, nil
}

// DecodeProjection parses and validates a Projection document.
func DecodeProjection(raw []byte) (*Projection, error) {
	var p Projection
	if err := yaml.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("decoding Projection: %w", err)
	}
	if err := ValidateProjection(&p); err != nil {
		return nil, err
	}
	return &p, nil
}
