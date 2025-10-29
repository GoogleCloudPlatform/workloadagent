/*
Copyright 2025 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package openshiftmetrics

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	structpb "github.com/golang/protobuf/ptypes/struct"
)

const (
	cannotParseErrMsg = "unparseable"
)

// jsonStringToStruct unmarshals a JSON string into a Struct proto.
func jsonStringToStruct(j string) (*structpb.Struct, error) {
	var i any
	if err := json.Unmarshal([]byte(j), &i); err != nil {
		return nil, err
	}

	m, ok := i.(map[string]any)
	if !ok {
		return nil, errors.New("JSON did not Unmarshal as an object")
	}

	s, err := jsonToStruct(m)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// jsonToStruct converts a JSON struct as output by json.Unmarshal into a Struct proto.
func jsonToStruct(m map[string]any) (*structpb.Struct, error) {
	s := structpb.Struct{Fields: map[string]*structpb.Value{}}
	for k, v := range m {
		var sv *structpb.Value
		var err error
		// Special case if the field name is "fieldMask" and the type *is* string.
		// Need this special case for backwards compatibility between jsonpb and protojson marshalling.
		if s, ok := v.(string); k == "fieldMask" && ok {
			split := strings.Split(s, ",")
			l := make([]any, len(split))
			for i, ss := range split {
				l[i] = ss
			}
			val := map[string]any{"paths": l}
			sv, err = jsonToValue(val)
		} else {
			sv, err = jsonToValue(v)
		}
		if err != nil {
			return nil, err
		}
		s.Fields[k] = sv
	}
	return &s, nil
}

func isMapType(j map[string]any) bool {
	_, kExists := j["key"]
	_, vExists := j["value"]
	return len(j) == 2 && kExists && vExists
}

func convertKeyToString(k any) string {
	// According to the proto spec, map keys can only be ints, strings, or bool.
	switch v := k.(type) {
	case int32:
		return strconv.Itoa(int(v))
	case uint32:
		return strconv.Itoa(int(v))
	case int64:
		return strconv.Itoa(int(v))
	case string:
		return v
	case bool:
		return strconv.FormatBool(v)
	default:
		// Should be impossible to reach here
		return ""
	}
}

// jsonToStruct converts a JSON value as output by json.Unmarshal into a Value proto.
func jsonToValue(j any) (*structpb.Value, error) {
	switch v := j.(type) {
	case nil:
		return &structpb.Value{Kind: &structpb.Value_NullValue{}}, nil
	case float32:
		return &structpb.Value{Kind: &structpb.Value_NumberValue{NumberValue: float64(v)}}, nil
	case float64:
		return &structpb.Value{Kind: &structpb.Value_NumberValue{NumberValue: v}}, nil
	case int32:
		return &structpb.Value{Kind: &structpb.Value_NumberValue{NumberValue: float64(v)}}, nil
	case uint32:
		return &structpb.Value{Kind: &structpb.Value_NumberValue{NumberValue: float64(v)}}, nil
	case int64:
		return &structpb.Value{Kind: &structpb.Value_NumberValue{NumberValue: float64(v)}}, nil
	case string:
		if utf8.ValidString(v) {
			return &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: v}}, nil
		}
		return &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: cannotParseErrMsg}}, nil
	case bool:
		return &structpb.Value{Kind: &structpb.Value_BoolValue{BoolValue: v}}, nil
	case []any:
		lv := &structpb.ListValue{Values: make([]*structpb.Value, len(v))}
		sv := structpb.Struct{Fields: map[string]*structpb.Value{}}
		for i, u := range v {
			v2, ok := u.(map[string]any)
			// Map type is converted to a map w/ 2 fields ("key", "value") under the hood. The value of
			// "key" is the actual key, and the value of "value" is the actual value.
			if ok && isMapType(v2) {
				key := convertKeyToString(v2["key"])
				value := v2["value"]
				vv, err := jsonToValue(value)
				if err != nil {
					return nil, err
				}
				sv.Fields[key] = vv
			} else {
				vv, err := jsonToValue(u)
				if err != nil {
					return nil, err
				}
				lv.Values[i] = vv
			}
		}
		if len(sv.Fields) > 0 {
			return &structpb.Value{Kind: &structpb.Value_StructValue{StructValue: &sv}}, nil
		}
		return &structpb.Value{Kind: &structpb.Value_ListValue{ListValue: lv}}, nil
	case []uint8:
		// Check if the []uint8 is a valid UTF-8 string.
		if utf8.Valid(v) {
			return &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: string(v[:])}}, nil
		}
		return &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: cannotParseErrMsg}}, nil
	case map[string]any:
		s, err := jsonToStruct(v)
		if err != nil {
			return nil, err
		}
		return &structpb.Value{Kind: &structpb.Value_StructValue{StructValue: s}}, nil
	default:
		return &structpb.Value{}, fmt.Errorf("unknown JSON type: %T", v)
	}
}
