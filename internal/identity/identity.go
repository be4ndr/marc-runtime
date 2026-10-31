// Package identity contains the shared v0.1 string and identifier policy.
package identity

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"
)

// MaxStringCodePoints retains the structural string and identifier limit.
const MaxStringCodePoints = 256

// MaxDescriptionCodePoints bounds human-readable descriptions in Unicode code points.
const MaxDescriptionCodePoints = 1024

// StringCodePointLimit selects the descriptive limit only for named text fields.
// Arrays inherit their field's limit; object members select their own. Unknown
// fields, object keys, identifiers and structural strings retain the 256 limit.
// Both wire names and persisted Go field names are recognized.
func StringCodePointLimit(field string) int {
	switch field {
	case "goal", "Goal", "summary", "Summary", "blocker", "Blocker",
		"escalation_reason", "EscalationReason", "evidence", "Evidence",
		"remaining_work", "RemainingWork", "acceptance_criteria", "AcceptanceCriteria",
		"intent", "Intent", "justification", "Justification",
		"prompt", "Prompt", "response", "Response", "approval_response", "ApprovalResponse",
		"reason", "Reason", "contingency_reason", "ContingencyReason":
		return MaxDescriptionCodePoints
	default:
		return MaxStringCodePoints
	}
}

// ValidDescription accepts valid UTF-8 with at most 1024 Unicode code points.
func ValidDescription(value string) bool {
	return utf8.ValidString(value) && utf8.RuneCountInString(value) <= MaxDescriptionCodePoints
}

func CheckDescription(field, value string) error {
	if !ValidDescription(value) {
		return fmt.Errorf("%s must be valid UTF-8 and at most %d Unicode code points", field, MaxDescriptionCodePoints)
	}
	return nil
}

// ValidID implements ^[a-z0-9]+(?:-[a-z0-9]+)*$.
func ValidID(value string) bool {
	if len(value) == 0 || len(value) > 256 {
		return false
	}
	groupHasChar := false
	for i := 0; i < len(value); i++ {
		c := value[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
			groupHasChar = true
		case c == '-' && groupHasChar && i+1 < len(value):
			groupHasChar = false
		default:
			return false
		}
	}
	return groupHasChar
}

// ValidText accepts valid UTF-8 containing at most 256 Unicode code points.
func ValidText(value string) bool {
	return utf8.ValidString(value) && utf8.RuneCountInString(value) <= MaxStringCodePoints
}

func CheckID(field, value string) error {
	if !ValidID(value) {
		return fmt.Errorf("%s must match the v0.1 identifier format", field)
	}
	return nil
}

func CheckText(field, value string) error {
	if !ValidText(value) {
		return fmt.Errorf("%s must be valid UTF-8 and at most 256 Unicode code points", field)
	}
	return nil
}

// Ingress limits bound decoding work, not semantic authority or token accounting.
const MaxJSONBytes = 1 << 20
const MaxJSONDepth = 32
const MaxJSONTokens = 65536

func CheckJSONEnvelope(raw []byte) error { return checkJSON(raw, true) }

// CheckJSONStrings also rejects lossy escapes and duplicate names. Trusted
// exports retain unlimited width; untrusted ingress uses CheckJSONEnvelope.
func CheckJSONStrings(raw []byte) error {
	return checkJSON(raw, false)
}

// JSONStringLimitError locates an oversized value without reflecting its text.
// Pointer uses JSON Pointer escaping and includes array indexes.
type JSONStringLimitError struct {
	Pointer string
	Limit   int
}

func (e *JSONStringLimitError) Error() string {
	return fmt.Sprintf("JSON string at %q must be valid UTF-8 and at most %d Unicode code points", e.Pointer, e.Limit)
}

func checkJSON(raw []byte, bounded bool) error {
	if bounded && len(raw) > MaxJSONBytes {
		return fmt.Errorf("JSON exceeds %d-byte ingress limit", MaxJSONBytes)
	}
	if !utf8.Valid(raw) {
		return fmt.Errorf("JSON is not valid UTF-8")
	}
	// Check raw escapes before encoding/json can replace unpaired surrogates.
	for i := 0; i < len(raw); i++ {
		if raw[i] != '"' {
			continue
		}
		i++
		for ; i < len(raw) && raw[i] != '"'; i++ {
			if raw[i] != '\\' {
				continue
			}
			i++
			if i >= len(raw) {
				return fmt.Errorf("incomplete JSON escape")
			}
			if raw[i] != 'u' {
				continue
			}
			if i+4 >= len(raw) {
				return fmt.Errorf("incomplete Unicode escape")
			}
			v, err := strconv.ParseUint(string(raw[i+1:i+5]), 16, 16)
			if err != nil {
				return err
			}
			i += 4
			if v >= 0xdc00 && v <= 0xdfff {
				return fmt.Errorf("unpaired low surrogate")
			}
			if v >= 0xd800 && v <= 0xdbff {
				if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
					return fmt.Errorf("unpaired high surrogate")
				}
				low, err := strconv.ParseUint(string(raw[i+3:i+7]), 16, 16)
				if err != nil || low < 0xdc00 || low > 0xdfff {
					return fmt.Errorf("unpaired high surrogate")
				}
				i += 6
			}
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	tokens := 0
	readToken := func() (json.Token, error) {
		tokens++
		if bounded && tokens > MaxJSONTokens {
			return nil, fmt.Errorf("JSON exceeds %d-token ingress limit", MaxJSONTokens)
		}
		return decoder.Token()
	}
	var lengthError *JSONStringLimitError
	var read func(int, int, string) error
	read = func(depth, stringLimit int, pointer string) error {
		tok, err := readToken()
		if err != nil {
			return err
		}
		if text, ok := tok.(string); ok {
			if !utf8.ValidString(text) || utf8.RuneCountInString(text) > stringLimit {
				if lengthError == nil {
					lengthError = &JSONStringLimitError{Pointer: pointer, Limit: stringLimit}
				}
			}
			return nil
		}
		delim, ok := tok.(json.Delim)
		if !ok {
			return nil
		}
		if depth >= MaxJSONDepth {
			return fmt.Errorf("JSON exceeds %d-container depth limit", MaxJSONDepth)
		}
		switch delim {
		case '{':
			keys := map[string]bool{}
			for decoder.More() {
				tok, err := readToken()
				if err != nil {
					return err
				}
				key, ok := tok.(string)
				if !ok {
					return fmt.Errorf("invalid JSON object key")
				}
				if err := CheckText("JSON object key", key); err != nil {
					return err
				}
				if keys[key] {
					return fmt.Errorf("duplicate JSON object key")
				}
				keys[key] = true
				escaped := strings.ReplaceAll(strings.ReplaceAll(key, "~", "~0"), "/", "~1")
				if err := read(depth+1, StringCodePointLimit(key), pointer+"/"+escaped); err != nil {
					return err
				}
			}
		case '[':
			index := 0
			for decoder.More() {
				if err := read(depth+1, stringLimit, pointer+"/"+strconv.Itoa(index)); err != nil {
					return err
				}
				index++
			}
		default:
			return fmt.Errorf("unexpected JSON delimiter")
		}
		end, err := readToken()
		if err != nil || (delim == '{' && end != json.Delim('}')) || (delim == '[' && end != json.Delim(']')) {
			return fmt.Errorf("unterminated JSON container")
		}
		return nil
	}
	if err := read(0, MaxStringCodePoints, ""); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return fmt.Errorf("trailing JSON content")
	}
	// Finish bounded structural validation first. Duplicate keys or malformed
	// JSON must not be presented as an unambiguous, correctable length failure.
	if lengthError != nil {
		return lengthError
	}
	return nil
}
