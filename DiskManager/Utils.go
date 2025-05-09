package DiskManager

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
)

func SerializeHeader(h *RecordHeader) ([]byte, error) {

	bufHeader := new(bytes.Buffer)

	if err := binary.Write(bufHeader, BINARY_ORDER, h.Addr); err != nil {
		return nil, err
	}

	if err := binary.Write(bufHeader, BINARY_ORDER, h.Size); err != nil {
		return nil, err
	}

	if err := binary.Write(bufHeader, BINARY_ORDER, h.Type); err != nil {
		return nil, err
	}

	if bufHeader.Len() != HEADER_SIZE {
		return nil, fmt.Errorf("serialized header size mismatch: expected %d, got %d", HEADER_SIZE, bufHeader.Len())
	}
	return bufHeader.Bytes(), nil
}

func SerializeData(data interface{}) ([]byte, error) {

	val := reflect.ValueOf(data)
	switch val.Kind() {
	case reflect.String:
		return []byte(val.String()), nil
	case reflect.Slice:
		if val.Type().Elem().Kind() == reflect.Uint8 {
			if val.IsNil() {
				return nil, nil
			}
			result := make([]byte, val.Len())
			reflect.Copy(reflect.ValueOf(result), val)
			return result, nil
		}
		return nil, fmt.Errorf("slice of type %v not supported", val.Type())
	case reflect.Array:
		if val.Type().Elem().Kind() == reflect.Uint8 {
			length := val.Len()
			result := make([]byte, length)
			reflect.Copy(reflect.ValueOf(result), val)
			return result, nil
		}
		return nil, fmt.Errorf("array of type %v not supported", val.Type())
	case reflect.Int8:
		buf := new(bytes.Buffer)
		if err := binary.Write(buf, BINARY_ORDER, int8(val.Int())); err != nil {
			return nil, fmt.Errorf("binary.Write failed: %v", err)
		}
		return buf.Bytes(), nil
	case reflect.Int16:
		buf := new(bytes.Buffer)
		if err := binary.Write(buf, BINARY_ORDER, int16(val.Int())); err != nil {
			return nil, fmt.Errorf("binary.Write failed: %v", err)
		}
		return buf.Bytes(), nil
	case reflect.Int32:
		buf := new(bytes.Buffer)
		if err := binary.Write(buf, BINARY_ORDER, int32(val.Int())); err != nil {
			return nil, fmt.Errorf("binary.Write failed: %v", err)
		}
		return buf.Bytes(), nil
	case reflect.Int64:
		buf := new(bytes.Buffer)
		if err := binary.Write(buf, BINARY_ORDER, int64(val.Int())); err != nil {
			return nil, fmt.Errorf("binary.Write failed: %v", err)
		}
		return buf.Bytes(), nil
	default:
		return nil, fmt.Errorf("type %v not supported", val.Kind())
	}
}

func DeserializeHeader(headerBytes []byte) (*RecordHeader, error) {

	if len(headerBytes) != HEADER_SIZE {
		return nil, fmt.Errorf("invalid header size: expected %d, got %d", HEADER_SIZE, len(headerBytes))
	}

	buf := bytes.NewReader(headerBytes)
	var header RecordHeader

	if err := binary.Read(buf, BINARY_ORDER, &header.Addr); err != nil {
		return nil, fmt.Errorf("binary read failed: %v", err)
	}

	if err := binary.Read(buf, BINARY_ORDER, &header.Size); err != nil {
		return nil, fmt.Errorf("binary read failed: %v", err)
	}

	if err := binary.Read(buf, BINARY_ORDER, &header.Type); err != nil {
		return nil, fmt.Errorf("binary read failed: %v", err)
	}

	return &header, nil
}

func DeserializeData(dataBytes []byte, DataType int8) (interface{}, error) {

	switch DataType {
	case DT_STRING:
		return string(dataBytes), nil

	case DT_BYTES:
		result := make([]byte, len(dataBytes))
		copy(result, dataBytes)
		return result, nil

	case DT_INT64:
		if len(dataBytes) != 8 {
			return nil, fmt.Errorf("invalid data size for int64: expected 8, got %d", len(dataBytes))
		}
		var tmp int64
		buf := bytes.NewReader(dataBytes)
		if err := binary.Read(buf, BINARY_ORDER, &tmp); err != nil {
			return nil, fmt.Errorf("binary read failed: %v", err)
		}
		return tmp, nil

	case DT_INT32:
		if len(dataBytes) != 4 {
			return nil, fmt.Errorf("invalid data size for int32: expected 4, got %d", len(dataBytes))
		}
		var tmp int32
		buf := bytes.NewReader(dataBytes)
		if err := binary.Read(buf, BINARY_ORDER, &tmp); err != nil {
			return nil, fmt.Errorf("binary read failed: %v", err)
		}
		return tmp, nil

	case DT_INT16:
		if len(dataBytes) != 2 {
			return nil, fmt.Errorf("invalid data size for int16: expected 2, got %d", len(dataBytes))
		}
		var tmp int16
		buf := bytes.NewReader(dataBytes)
		if err := binary.Read(buf, BINARY_ORDER, &tmp); err != nil {
			return nil, fmt.Errorf("binary read failed: %v", err)
		}
		return tmp, nil
	case DT_INT8:
		if len(dataBytes) != 1 {
			return nil, fmt.Errorf("invalid data size for int8: expected 1, got %d", len(dataBytes))
		}
		var tmp int8
		buf := bytes.NewReader(dataBytes)
		if err := binary.Read(buf, BINARY_ORDER, &tmp); err != nil {
			return nil, fmt.Errorf("binary read failed: %v", err)
		}
		return tmp, nil

	default:
		return nil, fmt.Errorf("following datatype not supported")

	}

}
