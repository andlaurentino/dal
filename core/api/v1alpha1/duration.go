package v1alpha1

import (
	"fmt"
	"regexp"
	"strconv"
	"time"
)

var dayDurationPattern = regexp.MustCompile(`^(\d+(?:\.\d+)?)d$`)

// ParseAge parses a SourceRouting minAge/maxAge value: either a standard Go
// duration string (e.g. "168h", "30m") or a plain day count suffixed with
// "d" (e.g. "7d", "1.5d"), shorthand for that many 24h days. Go's own
// time.ParseDuration has no "d" unit, but retention/routing windows are
// most naturally expressed in days.
func ParseAge(s string) (time.Duration, error) {
	if m := dayDurationPattern.FindStringSubmatch(s); m != nil {
		days, err := strconv.ParseFloat(m[1], 64)
		if err != nil {
			return 0, fmt.Errorf("invalid day duration %q: %w", s, err)
		}
		return time.Duration(days * float64(24*time.Hour)), nil
	}
	return time.ParseDuration(s)
}
