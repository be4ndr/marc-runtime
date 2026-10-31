package schemas

import (
	"strings"
	"testing"
)

func TestSchemaFieldSpecificTextLimits(t *testing.T) {
	once.Do(initialize)
	if compileErr != nil {
		t.Fatal(compileErr)
	}
	for _, tc := range []struct {
		path  string
		limit int
	}{
		{"lead-state.schema.json#/properties/goal", 1024},
		{"lead-state.schema.json#/properties/acceptance_criteria/items", 1024},
		{"lead-state.schema.json#/$defs/finding/properties/summary", 1024},
		{"lead-state.schema.json#/$defs/resolvedFinding/properties/summary", 1024},
		{"next-action.schema.json#/properties/intent", 1024},
		{"resource-request.schema.json#/properties/justification", 1024},
		{"developer-result.schema.json#/properties/remaining_work/items", 1024},
		{"developer-result.schema.json#/properties/blocker/properties/summary", 1024},
		{"role-result-common.schema.json#/$defs/envelope/properties/summary", 1024},
		{"role-result-common.schema.json#/$defs/finding/properties/summary", 1024},
		{"role-result-common.schema.json#/$defs/qaCheck/properties/summary", 1024},
		{"role-result-common.schema.json#/$defs/location/properties/path", 256},
		{"role-result-common.schema.json#/$defs/identifier", 256},
	} {
		t.Run(tc.path, func(t *testing.T) {
			sch, err := compiler.Compile(base + tc.path)
			if err != nil {
				t.Fatal(err)
			}
			for _, n := range []int{256, 257, 1024, 1025} {
				chars := []string{"x", "😀"}
				if strings.HasSuffix(tc.path, "identifier") {
					chars = []string{"x"}
				}
				for _, char := range chars {
					if (sch.Validate(strings.Repeat(char, n)) == nil) != (n <= tc.limit) {
						t.Fatalf("limit mismatch at %d", n)
					}
				}
			}
		})
	}
}
