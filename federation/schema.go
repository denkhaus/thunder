package federation

import (
	"errors"
	"fmt"
	"sort"

	"github.com/samsarahq/thunder/graphql"
)

// MergeMode controls how to combine two different schemas. Union is used for
// two independent services, Intersection for two different versions of the same
// service.
type MergeMode string

const (
	// Union computes a schema that is supported by the two services combined.
	//
	// A Union is used to to combine the schema of two independent services.
	// The proxy will split a GraphQL query to ask each service the fields
	// it knows about.
	//
	// Two schemas must be compatible: Any overlapping types (eg. a field that
	// is implemented by both services, or two input types) must be compatible.
	// In practice, this means types must be identical except for non-nil
	// modifiers.
	//
	// XXX: take intersection on ENUM values to not confuse a service with a
	// type it doesn't support?
	Union MergeMode = "union"

	// Intersection computes a schema that is supported by both services.
	//
	// An Intersection is used to combine two schemas of different versions
	// of the same service. During a deploy, only of two versions might be
	// available, and so queries must be compatible with both schemas.
	//
	// Intersection computes a schema that can be executed by both services.
	// It only includes types and fields (etc.) exported by both services.
	// Overlapping types must be compatible as in a Union merge.
	//
	// One surprise might be that newly added ENUM values or UNION types might
	// be returned by the merged schema.
	Intersection MergeMode = "intersection"
)

// introspectionTypeRef is a type reference from the GraphQL introspection
// query.
type introspectionTypeRef struct {
	Kind   string                `json:"kind"`
	Name   string                `json:"name"`
	OfType *introspectionTypeRef `json:"ofType"`
}

func (t *introspectionTypeRef) String() string {
	if t == nil {
		return "<nil>"
	}
	switch t.Kind {
	case "SCALAR", "ENUM", "UNION", "OBJECT", "INPUT_OBJECT":
		return t.Name
	case "NON_NULL":
		return t.OfType.String() + "!"
	case "LIST":
		return "[" + t.OfType.String() + "]"
	default:
		return fmt.Sprintf("<kind=%s name=%s ofType%s>", t.Kind, t.Name, t.OfType)
	}
}

type introspectionInputField struct {
	Name string                `json:"name"`
	Type *introspectionTypeRef `json:"type"`
}

type introspectionField struct {
	Name string                    `json:"name"`
	Type *introspectionTypeRef     `json:"type"`
	Args []introspectionInputField `json:"args"`
}

type introspectionEnumValue struct {
	Name string `json:"name"`
}

type introspectionType struct {
	Name          string                    `json:"name"`
	Kind          string                    `json:"kind"`
	Fields        []introspectionField      `json:"fields"`
	InputFields   []introspectionInputField `json:"inputFields"`
	PossibleTypes []*introspectionTypeRef   `json:"possibleTypes"`
	EnumValues    []introspectionEnumValue  `json:"enumValues"`
}

type introspectionSchema struct {
	Types []introspectionType `json:"types"`
}

type introspectionQueryResult struct {
	Schema introspectionSchema `json:"__schema"`
}

// mergeTypeRefs takes two types from two different services,
// makes sure they are compatible, and computes a merged type.
//
// Two types are compatible if they are the same besides non-nullable modifiers.
//
// The merged type gets non-nullable modifiers depending on how the type is used.
// For input types, the merged type should be accepted by both services, so it's nullable only if both services accept a nullable type.
// For output types, the merged type should work for either service, so it's nullable if either service might return null.
func mergeTypeRefs(a, b *introspectionTypeRef, isInput bool) (*introspectionTypeRef, error) {
	// If either a or b is non-nil, unwrap it, recurse, and maybe mark the
	// resulting type as non-nil.
	aNonNil := false
	if a.Kind == "NON_NULL" {
		aNonNil = true
		a = a.OfType
	}
	bNonNil := false
	if b.Kind == "NON_NULL" {
		bNonNil = true
		b = b.OfType
	}
	if aNonNil || bNonNil {
		merged, err := mergeTypeRefs(a, b, isInput)
		if err != nil {
			return nil, err
		}

		// Input types are non-nil if either type is non-nil, as one service
		// will always want an input. Output types are non-nil if both
		// types are non-nil, as we can only guarantee non-nil values if both
		// services play along.
		resultNonNil := isInput || (aNonNil && bNonNil)

		if resultNonNil {
			return &introspectionTypeRef{Kind: "NON_NULL", OfType: merged}, nil
		}
		return merged, nil
	}

	// Otherwise, recursively assert that the input types are compatible.
	if a.Kind != b.Kind {
		return nil, fmt.Errorf("kinds %s and %s differ", a.Name, b.Kind)
	}
	switch a.Kind {
	// Basic types must be identical.
	case "SCALAR", "ENUM", "INPUT_OBJECT", "UNION", "OBJECT":
		if a.Name != b.Name {
			return nil, errors.New("types must be identical")
		}
		return &introspectionTypeRef{
			Kind: a.Kind,
			Name: a.Name,
		}, nil

	// Recursive must be compatible but don't have to be identical.
	case "LIST":
		inner, err := mergeTypeRefs(a.OfType, b.OfType, isInput)
		if err != nil {
			return nil, err
		}
		return &introspectionTypeRef{Kind: "LIST", OfType: inner}, nil

	default:
		return nil, errors.New("unknown type kind")
	}
}
// mergeInputFields combines two sets of input fields from two schemas.
//
// It checks the types are compabible and takes the union or intersection of the fields depending on the Mergemode
func mergeInputFields(a, b []introspectionInputField, mode MergeMode) ([]introspectionInputField, error) {
	types := make(map[string][]introspectionInputField)
	for _, a := range a {
		types[a.Name] = append(types[a.Name], a)
	}
	for _, b := range b {
		types[b.Name] = append(types[b.Name], b)
	}
	names := make([]string, 0, len(types))
	for name := range types {
		names = append(names, name)
	}
	sort.Strings(names)

	merged := make([]introspectionInputField, 0, len(names))
	for _, name := range names {
		p := types[name]
		if len(p) == 1 {
			if p[0].Type.Kind == "NON_NULL" {
				return nil, fmt.Errorf("new field %s is non-null: %v", name, p[0].Type)
			}
			if mode == Union {
				merged = append(merged, p[0])
			}
			continue
		}
		m, err := mergeTypeRefs(p[0].Type, p[1].Type, true)
		if err != nil {
			return nil, fmt.Errorf("field %s has incompatible types %s and %s: %v", name, p[0].Type, p[1].Type, err)
		}
		merged = append(merged, introspectionInputField{
			Name: name,
			Type: m,
		})
	}

	return merged, nil
}

func mergeFields(a, b []introspectionField, mode MergeMode) ([]introspectionField, error) {
	types := make(map[string][]introspectionField)
	for _, a := range a {
		types[a.Name] = append(types[a.Name], a)
	}
	for _, b := range b {
		types[b.Name] = append(types[b.Name], b)
	}
	names := make([]string, 0, len(types))
	for name := range types {
		names = append(names, name)
	}
	sort.Strings(names)

	merged := make([]introspectionField, 0, len(names))
	for _, name := range names {
		p := types[name]
		if len(p) == 1 {
			if mode == Union {
				merged = append(merged, p[0])
			}
			continue
		}

		typ, err := mergeTypeRefs(p[0].Type, p[1].Type, false)
		if err != nil {
			return nil, fmt.Errorf("field %s has incompatible types %v and %v: %v", name, p[0], p[1], err)
		}
		args, err := mergeInputFields(p[0].Args, p[1].Args, mode)
		if err != nil {
			return nil, fmt.Errorf("field %s has incompatible arguments: %v", name, err)
		}

		merged = append(merged, introspectionField{
			Name: name,
			Type: typ,
			Args: args,
		})
	}

	return merged, nil
}

func mergePossibleTypes(a, b []*introspectionTypeRef, mode MergeMode) ([]*introspectionTypeRef, error) {
	types := make(map[string][]*introspectionTypeRef)
	for _, a := range a {
		types[a.Name] = append(types[a.Name], a)
	}
	for _, b := range b {
		types[b.Name] = append(types[b.Name], b)
	}
	names := make([]string, 0, len(types))
	for name := range types {
		names = append(names, name)
	}
	sort.Strings(names)

	merged := make([]*introspectionTypeRef, 0, len(names))
	for _, name := range names {
		p := types[name]
		if len(p) == 1 {
			if mode == Union {
				merged = append(merged, p[0])
			}
			continue
		}

		merged = append(merged, p[0])
	}

	return merged, nil
}

func mergeEnumValues(a, b []introspectionEnumValue, mode MergeMode) ([]introspectionEnumValue, error) {
	types := make(map[string][]introspectionEnumValue)
	for _, a := range a {
		types[a.Name] = append(types[a.Name], a)
	}
	for _, b := range b {
		types[b.Name] = append(types[b.Name], b)
	}
	names := make([]string, 0, len(types))
	for name := range types {
		names = append(names, name)
	}
	sort.Strings(names)

	merged := make([]introspectionEnumValue, 0, len(names))
	for _, name := range names {
		p := types[name]
		if len(p) == 1 {
			if mode == Union {
				merged = append(merged, p[0])
			}
			continue
		}

		merged = append(merged, p[0])
	}

	return merged, nil
}

func mergeTypes(a, b introspectionType, mode MergeMode) (*introspectionType, error) {
	if a.Kind != b.Kind {
		return nil, fmt.Errorf("conflicting kinds %s and %s", a.Kind, b.Kind)
	}

	merged := introspectionType{
		Name:          a.Name,
		Kind:          a.Kind,
		Fields:        []introspectionField{},
		InputFields:   []introspectionInputField{},
		PossibleTypes: []*introspectionTypeRef{},
		EnumValues:    []introspectionEnumValue{},
	}

	switch a.Kind {
	case "INPUT_OBJECT":
		inputFields, err := mergeInputFields(a.InputFields, b.InputFields, mode)
		if err != nil {
			return nil, fmt.Errorf("merging input fields: %v", err)
		}
		merged.InputFields = inputFields

	case "OBJECT":
		fields, err := mergeFields(a.Fields, b.Fields, mode)
		if err != nil {
			return nil, fmt.Errorf("merging fields: %v", err)
		}
		merged.Fields = fields

	case "UNION":
		possibleTypes, err := mergePossibleTypes(a.PossibleTypes, b.PossibleTypes, mode)
		if err != nil {
			return nil, fmt.Errorf("merging possible types: %v", err)
		}
		merged.PossibleTypes = possibleTypes

	case "ENUM":
		enumValues, err := mergeEnumValues(a.EnumValues, b.EnumValues, mode)
		if err != nil {
			return nil, fmt.Errorf("merging enum values: %v", err)
		}
		merged.EnumValues = enumValues

	case "SCALAR":

	default:
		return nil, fmt.Errorf("unknown kind %s", a.Kind)
	}

	return &merged, nil
}

func mergeSchemas(a, b *introspectionQueryResult, mode MergeMode) (*introspectionQueryResult, error) {
	types := make(map[string][]introspectionType)
	for _, a := range a.Schema.Types {
		types[a.Name] = append(types[a.Name], a)
	}
	for _, b := range b.Schema.Types {
		types[b.Name] = append(types[b.Name], b)
	}
	names := make([]string, 0, len(types))
	for name := range types {
		names = append(names, name)
	}
	sort.Strings(names)

	merged := make([]introspectionType, 0, len(names))
	for _, name := range names {
		p := types[name]
		if len(p) == 1 {
			// When interesction only include types that appear in both schemas
			if mode == Union {
				merged = append(merged, p[0])
			}
			continue
		}
		m, err := mergeTypes(p[0], p[1], mode)
		if err != nil {
			return nil, fmt.Errorf("can't merge type %s: %v", name, err)
		}
		merged = append(merged, *m)
	}

	return &introspectionQueryResult{
		Schema: introspectionSchema{
			Types: merged,
		},
	}, nil
}

func mergeSchemaSlice(schemas []*introspectionQueryResult, mode MergeMode) (*introspectionQueryResult, error) {
	if len(schemas) == 0 {
		return nil, errors.New("no schemas")
	}
	merged := schemas[0]
	for _, schema := range schemas[1:] {
		var err error
		merged, err = mergeSchemas(merged, schema, mode)
		if err != nil {
			return nil, err
		}
	}
	return merged, nil
}

// The code below converts the result of an introspection query in a graphql.Schema
// that is used for type-checking and computing an execution plan for a federated query.

func lookupTypeRef(t *introspectionTypeRef, all map[string]graphql.Type) (graphql.Type, error) {
	if t == nil {
		return nil, errors.New("malformed typeref")
	}

	switch t.Kind {
	case "SCALAR", "OBJECT", "UNION", "INPUT_OBJECT", "ENUM":
		// TODO: enforce type?
		typ, ok := all[t.Name]
		if !ok {
			return nil, fmt.Errorf("type %s not found among top-level types", t.Name)
		}
		return typ, nil

	case "LIST":
		inner, err := lookupTypeRef(t.OfType, all)
		if err != nil {
			return nil, err
		}
		return &graphql.List{
			Type: inner,
		}, nil

	case "NON_NULL":
		inner, err := lookupTypeRef(t.OfType, all)
		if err != nil {
			return nil, err
		}
		return &graphql.NonNull{
			Type: inner,
		}, nil

	default:
		return nil, fmt.Errorf("unknown type kind %s", t.Kind)
	}
}

func parseInputFields(source []introspectionInputField, all map[string]graphql.Type) (map[string]graphql.Type, error) {
	fields := make(map[string]graphql.Type)

	for _, field := range source {
		here, err := lookupTypeRef(field.Type, all)
		if err != nil {
			return nil, fmt.Errorf("field %s has bad typ: %v",
				field.Name, err)
		}
		// TODO: check this is an input type object isnt an input type
		fields[field.Name] = here
	}

	return fields, nil
}

func parseSchema(schema *introspectionQueryResult) (map[string]graphql.Type, error) {
	all := make(map[string]graphql.Type)

	for _, typ := range schema.Schema.Types {
		if _, ok := all[typ.Name]; ok {
			return nil, fmt.Errorf("duplicate type %s", typ.Name)
		}

		switch typ.Kind {
		case "OBJECT":
			all[typ.Name] = &graphql.Object{
				Name: typ.Name,
			}

		case "INPUT_OBJECT":
			all[typ.Name] = &graphql.InputObject{
				Name: typ.Name,
			}

		case "SCALAR":
			all[typ.Name] = &graphql.Scalar{
				Type: typ.Name,
			}

		case "UNION":
			all[typ.Name] = &graphql.Union{
				Name: typ.Name,
			}

		case "ENUM":
			all[typ.Name] = &graphql.Enum{
				Type: typ.Name,
			}

		default:
			return nil, fmt.Errorf("unknown type kind %s", typ.Kind)
		}
	}

	// Initialize barebone types
	for _, typ := range schema.Schema.Types {
		switch typ.Kind {
		case "OBJECT":
			fields := make(map[string]*graphql.Field)
			for _, field := range typ.Fields {
				fieldTyp, err := lookupTypeRef(field.Type, all)
				if err != nil {
					return nil, fmt.Errorf("typ %s field %s has bad typ: %v",
						typ.Name, field.Name, err)
				}

				parsed, err := parseInputFields(field.Args, all)
				if err != nil {
					return nil, fmt.Errorf("field %s input: %v", field.Name, err)
				}

				fields[field.Name] = &graphql.Field{
					Args: parsed,
					Type: fieldTyp,
				}
			}

			all[typ.Name].(*graphql.Object).Fields = fields

		case "INPUT_OBJECT":
			parsed, err := parseInputFields(typ.InputFields, all)
			if err != nil {
				return nil, fmt.Errorf("typ %s: %v", typ.Name, err)
			}

			all[typ.Name].(*graphql.InputObject).InputFields = parsed

		case "UNION":
			types := make(map[string]*graphql.Object)
			for _, other := range typ.PossibleTypes {
				if other.Kind != "OBJECT" {
					return nil, fmt.Errorf("typ %s has possible typ not OBJECT: %v", typ.Name, other)
				}
				typ, ok := all[other.Name].(*graphql.Object)
				if !ok {
					return nil, fmt.Errorf("typ %s possible typ %s does not refer to obj", typ.Name, other.Name)
				}
				types[typ.Name] = typ
			}

			all[typ.Name].(*graphql.Union).Types = types



		case "ENUM":
			// XXX: introspection relies on the EnumValues map.
			reverseMap := make(map[interface{}]string)
			values := make([]string, 0, len(typ.EnumValues))
			for _, value := range typ.EnumValues {
				values = append(values, value.Name)
				reverseMap[value.Name] = value.Name
			}

			enum := all[typ.Name].(*graphql.Enum)
			enum.Values = values
			enum.ReverseMap = reverseMap

		case "SCALAR":
			// pass

		default:
			return nil, fmt.Errorf("unknown type kind %s", typ.Kind)
		}
	}

	return all, nil
}

// serviceSchemas holds all schemas for all of versions of all executors services. It is a map from service name and version to schema.
type serviceSchemas map[string]map[string]*introspectionQueryResult

// FieldInfo holds federation-specific information for graphql.Fields used to plan and execute queries.
type FieldInfo struct {
	// Service is an arbitrary service that can resolve this field. TODO: Delete in favor of services?
	Service  string
	// Services is the set of all services that can resolve this field. If a service has multiple versions, all versions must
	// be able to resolve the field.
	Services map[string]bool
}

// SchemaWithFederationInfo holds a graphql.Schema along with federtion-specific annotations per field.
type SchemaWithFederationInfo struct {
	Schema *graphql.Schema
	Fields map[*graphql.Field]*FieldInfo
}

func convertVersionedSchemas(schemas serviceSchemas) (*SchemaWithFederationInfo, error) {
	serviceNames := make([]string, 0, len(schemas))
	for service := range schemas {
		serviceNames = append(serviceNames, service)
	}
	sort.Strings(serviceNames)

	serviceSchemasByName := make(map[string]*introspectionQueryResult)

	var serviceSchemas []*introspectionQueryResult
	for _, service := range serviceNames {
		versions := schemas[service]

		versionNames := make([]string, 0, len(versions))
		for version := range versions {
			versionNames = append(versionNames, version)
		}
		sort.Strings(versionNames)

		var versionSchemas []*introspectionQueryResult
		for _, version := range versionNames {
			versionSchemas = append(versionSchemas, versions[version])
		}

		serviceSchema, err := mergeSchemaSlice(versionSchemas, Intersection)
		if err != nil {
			return nil, err
		}

		serviceSchemasByName[service] = serviceSchema

		serviceSchemas = append(serviceSchemas, serviceSchema)
	}

	merged, err := mergeSchemaSlice(serviceSchemas, Union)
	if err != nil {
		return nil, err
	}

	types, err := parseSchema(merged)
	if err != nil {
		return nil, err
	}

	fieldInfos := make(map[*graphql.Field]*FieldInfo)
	for _, service := range serviceNames {
		for _, typ := range serviceSchemasByName[service].Schema.Types {
			if typ.Kind == "OBJECT" {
				obj := types[typ.Name].(*graphql.Object)

				for _, field := range typ.Fields {
					f := obj.Fields[field.Name]

					info, ok := fieldInfos[f]
					if !ok {
						info = &FieldInfo{
							Service:  service,
							Services: map[string]bool{},
						}
						fieldInfos[f] = info
					}
					info.Services[service] = true
				}
			}
		}
	}

	return &SchemaWithFederationInfo{
		Schema: &graphql.Schema{
			Query:    types["Query"],
			Mutation: types["Mutation"],
		},
		Fields: fieldInfos,
	}, nil
}

func convertSchema(schemas map[string]*introspectionQueryResult) (*SchemaWithFederationInfo, error) {
	versionedSchemas := make(serviceSchemas)
	for service, schema := range schemas {
		versionedSchemas[service] = map[string]*introspectionQueryResult{
			"": schema,
		}
	}
	return convertVersionedSchemas(versionedSchemas)
}




// XXX: for types missing __federation, take intersection?

// XXX: for (merged) unions, make sure we only send possible types
// to each service

// TODO: support descriptions in merging
