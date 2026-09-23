package v1alpha1

import (
	"testing"
	"time"
)

func TestParseAge(t *testing.T) {
	cases := []struct {
		in      string
		want    time.Duration
		wantErr bool
	}{
		{in: "7d", want: 7 * 24 * time.Hour},
		{in: "1.5d", want: 36 * time.Hour},
		{in: "168h", want: 168 * time.Hour},
		{in: "30m", want: 30 * time.Minute},
		{in: "7dx", wantErr: true},
		{in: "not-a-duration", wantErr: true},
	}
	for _, c := range cases {
		got, err := ParseAge(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("ParseAge(%q): expected error, got %v", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseAge(%q): unexpected error: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("ParseAge(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}
