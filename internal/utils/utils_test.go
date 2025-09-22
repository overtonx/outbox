package utils

import (
	"database/sql"
	"testing"
)

func TestNewDefaultLogger(t *testing.T) {
	logger, err := NewDefaultLogger()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if logger == nil {
		t.Fatal("Expected logger to be non-nil")
	}
}

func TestPlaceholders(t *testing.T) {
	tests := []struct {
		name     string
		count    int
		expected string
	}{
		{"zero count", 0, ""},
		{"single placeholder", 1, "?"},
		{"multiple Placeholders", 3, "?,?,?"},
		{"many Placeholders", 5, "?,?,?,?,?"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Placeholders(tt.count)
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestNullString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected sql.NullString
	}{
		{"empty string", "", sql.NullString{String: "", Valid: false}},
		{"non-empty string", "test", sql.NullString{String: "test", Valid: true}},
		{"whitespace string", " ", sql.NullString{String: " ", Valid: true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NullString(tt.input)
			if result.String != tt.expected.String {
				t.Errorf("Expected String %q, got %q", tt.expected.String, result.String)
			}
			if result.Valid != tt.expected.Valid {
				t.Errorf("Expected Valid %v, got %v", tt.expected.Valid, result.Valid)
			}
		})
	}
}
