package schemas

import (
	"encoding/json"
	"strconv"
	"strings"

	"marc-runtime/internal/numeric"
)

// The pinned validator's rational parser refuses decimal scales outside +/-1e6
// but its validator does not handle that refusal. Check syntax without allocating
// powers of ten. This is a downstream representation guard, not a resource limit.
func validateNumber(n json.Number) error {
	text := n.String()
	mantissa := text
	var exponent int64
	if at := strings.IndexAny(text, "eE"); at >= 0 {
		mantissa = text[:at]
		var err error
		exponent, err = strconv.ParseInt(text[at+1:], 10, 64)
		if err != nil {
			return numeric.Invalid("json_number", "invalid_input", "decimal exponent is unrepresentable")
		}
	}
	var fraction int64
	if at := strings.IndexByte(mantissa, '.'); at >= 0 {
		fraction = int64(len(mantissa) - at - 1)
	}
	// Comparing before subtraction also excludes signed intermediate overflow.
	if exponent > 1_000_000 || exponent < -1_000_000 || fraction > 1_000_000 || exponent-fraction < -1_000_000 {
		return numeric.Invalid("json_number", "invalid_input", "decimal scale is unsupported by schema validation")
	}
	return nil
}
