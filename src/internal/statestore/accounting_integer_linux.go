package statestore

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"ai-control-plane/internal/numeric"
)

func checkedAdd(a, b int64) (int64, error) {
	if b > 0 && a > math.MaxInt64-b || b < 0 && a < math.MinInt64-b {
		return 0, numeric.Invalid("integer", "overflow", "addition is unrepresentable")
	}
	return a + b, nil
}

func checkedSubtract(a, b int64) (int64, error) {
	if b > 0 && a < math.MinInt64+b || b < 0 && a > math.MaxInt64+b {
		return 0, numeric.Invalid("integer", "overflow", "subtraction is unrepresentable")
	}
	return a - b, nil
}

func checkedNext(value int64, domain string) (int64, error) {
	if value < 0 || value == math.MaxInt64 {
		return 0, numeric.Invalid(domain, "exhausted", "successor is unrepresentable")
	}
	return value + 1, nil
}

// scaledDecimal converts a JSON decimal exactly, including exponent notation.
// No floating point or unbounded powers of ten participate in conversion.
func scaledDecimal(value string, scale int64) (int64, error) {
	mantissa, exponent := value, int64(0)
	if at := strings.IndexAny(value, "eE"); at >= 0 {
		mantissa = value[:at]
		var err error
		exponent, err = strconv.ParseInt(value[at+1:], 10, 64)
		if err != nil {
			return 0, numeric.Invalid("decimal", "invalid_input", "exponent is unrepresentable")
		}
	}
	fraction := int64(0)
	if at := strings.IndexByte(mantissa, '.'); at >= 0 {
		fraction = int64(len(mantissa) - at - 1)
		mantissa = mantissa[:at] + mantissa[at+1:]
	}
	if mantissa == "" {
		return 0, fmt.Errorf("empty resource amount")
	}
	for _, digit := range mantissa {
		if digit < '0' || digit > '9' {
			return 0, fmt.Errorf("invalid nonnegative decimal")
		}
	}
	mantissa = strings.TrimLeft(mantissa, "0")
	if mantissa == "" {
		return 0, nil
	}
	shift, err := checkedSubtract(exponent, fraction)
	if err != nil {
		return 0, err
	}
	shift, err = checkedAdd(shift, scale)
	if err != nil {
		return 0, err
	}
	if shift < 0 {
		if shift <= -int64(len(mantissa)) {
			return 0, fmt.Errorf("resource amount has fractional units")
		}
		cut := len(mantissa) + int(shift)
		if strings.Trim(mantissa[cut:], "0") != "" {
			return 0, fmt.Errorf("resource amount has fractional units")
		}
		mantissa = mantissa[:cut]
	} else {
		if shift > 19 || int64(len(mantissa)) > 19-shift {
			return 0, numeric.Invalid("resource_amount", "invalid_input", "amount is unrepresentable")
		}
		mantissa += strings.Repeat("0", int(shift))
	}
	n, err := strconv.ParseInt(mantissa, 10, 64)
	if err != nil {
		return 0, numeric.Invalid("resource_amount", "invalid_input", "amount is unrepresentable")
	}
	return n, nil
}
