// Package schemas validates protocol/export JSON. It never grants persistence
// or workflow authority. Schemas and metaschemas are compiled without network IO.
package schemas

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"unicode/utf8"

	"ai-control-plane/internal/identity"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

//go:embed *.schema.json
var Files embed.FS

var once sync.Once
var compiler *jsonschema.Compiler
var compileErr error
var compiled map[string]*jsonschema.Schema

const base = "https://ai-control-plane.invalid/schemas/"

func initialize() {
	compiler = jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.AssertFormat()
	compiler.UseLoader(embeddedOnlyLoader{})
	entries, err := Files.ReadDir(".")
	if err != nil {
		compileErr = err
		return
	}
	for _, entry := range entries {
		raw, err := Files.ReadFile(entry.Name())
		if err != nil {
			compileErr = err
			return
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
		if err != nil {
			compileErr = err
			return
		}
		if err := compiler.AddResource(base+entry.Name(), doc); err != nil {
			compileErr = err
			return
		}
	}
	compiled = map[string]*jsonschema.Schema{}
	for _, entry := range entries {
		sch, err := compiler.Compile(base + entry.Name())
		if err != nil {
			compileErr = err
			return
		}
		compiled[entry.Name()] = sch
	}
	for _, def := range []string{"identifier", "resultID", "findingID", "finding", "findingOccurrence", "evidenceRef", "location", "qaCheck"} {
		name := "role-result-common.schema.json#/$defs/" + def
		sch, err := compiler.Compile(base + name)
		if err != nil {
			compileErr = err
			return
		}
		compiled[name] = sch
	}
}

// Validate checks Draft 2020-12 shape and asserted formats. Reference resolution,
// derived identifiers, historical invariants and policy remain separate checks.
func Validate(name string, raw []byte) error {
	if name == "usage-receipt.schema.json" && len(raw) > MaxUsageReceiptBytes {
		return fmt.Errorf("usage receipt exceeds %d-byte wire limit", MaxUsageReceiptBytes)
	}
	check := identity.CheckJSONEnvelope
	if name == "metrics.schema.json" || name == "run-state.schema.json" {
		check = identity.CheckJSONStrings // observational exports are not Worker ingress
	}
	if err := check(raw); err != nil {
		return err
	}
	if name == "usage-receipt.schema.json" {
		if err := validateUsageReceiptWire(raw); err != nil {
			return err
		}
	}
	once.Do(initialize)
	if compileErr != nil {
		return compileErr
	}
	sch, ok := compiled[name]
	if !ok {
		return fmt.Errorf("unknown schema %q", name)
	}
	value, err := decodeUnique(raw)
	if err != nil {
		return err
	}
	return sch.Validate(value)
}

type embeddedOnlyLoader struct{}

func (embeddedOnlyLoader) Load(url string) (any, error) {
	return nil, fmt.Errorf("schema resource is not embedded: %s", url)
}

// Duplicate object keys would make authoritative structured fields ambiguous.
// Validate checks field-specific string limits before this decoder runs.
func decodeUnique(raw []byte) (any, error) {
	if !utf8.Valid(raw) {
		return nil, fmt.Errorf("JSON input is not valid UTF-8")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var read func() (any, error)
	read = func() (any, error) {
		tok, err := d.Token()
		if err != nil {
			return nil, err
		}
		delim, ok := tok.(json.Delim)
		if !ok {
			if n, ok := tok.(json.Number); ok {
				if err := validateNumber(n); err != nil {
					return nil, err
				}
			}
			return tok, nil
		}
		switch delim {
		case '{':
			object := map[string]any{}
			for d.More() {
				key, err := d.Token()
				if err != nil {
					return nil, err
				}
				name, ok := key.(string)
				if !ok {
					return nil, fmt.Errorf("invalid object key")
				}
				if !identity.ValidText(name) {
					return nil, fmt.Errorf("object key must be at most %d Unicode code points", identity.MaxStringCodePoints)
				}
				if _, ok := object[name]; ok {
					return nil, fmt.Errorf("duplicate object key %s", name)
				}
				value, err := read()
				if err != nil {
					return nil, err
				}
				object[name] = value
			}
			end, err := d.Token()
			if err != nil || end != json.Delim('}') {
				return nil, fmt.Errorf("unterminated object")
			}
			return object, nil
		case '[':
			values := []any{}
			for d.More() {
				v, err := read()
				if err != nil {
					return nil, err
				}
				values = append(values, v)
			}
			end, err := d.Token()
			if err != nil || end != json.Delim(']') {
				return nil, fmt.Errorf("unterminated array")
			}
			return values, nil
		default:
			return nil, fmt.Errorf("unexpected JSON delimiter")
		}
	}
	value, err := read()
	if err != nil {
		return nil, err
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, fmt.Errorf("trailing JSON content")
	}
	return value, nil
}
