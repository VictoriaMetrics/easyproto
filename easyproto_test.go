package easyproto

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
)

func TestZigZagInt32(t *testing.T) {
	f := func(i32 int32) {
		t.Helper()

		u32 := encodeZigZagInt32(i32)
		n := decodeZigZagInt32(u32)
		if n != i32 {
			t.Fatalf("unexpected value after zig-zag coding; got %d; want %d", n, i32)
		}
	}

	f(0)
	f(1)
	f(-1)
	f(2)
	f(-2)
	f(1 << 7)
	f(-1 << 7)
	f(1 << 8)
	f(-1 << 8)
	f(1<<31 - 1)
	f(-1 << 31)
	f(-1<<31 + 1)
}

func TestZigZagInt64(t *testing.T) {
	f := func(i64 int64) {
		t.Helper()

		u64 := encodeZigZagInt64(i64)
		n := decodeZigZagInt64(u64)
		if n != i64 {
			t.Fatalf("unexpected value after zig-zag coding; got %d; want %d", n, i64)
		}
	}

	f(0)
	f(1)
	f(-1)
	f(2)
	f(-2)
	f(1 << 7)
	f(-1 << 7)
	f(1 << 8)
	f(-1 << 8)
	f(1<<63 - 1)
	f(-1 << 63)
	f(-1<<63 + 1)
}

func TestMarshaler(t *testing.T) {
	m := mp.Get()
	mm1 := m.MessageMarshaler()
	mm2 := m.MessageMarshaler()
	if mm1 != mm2 {
		t.Fatalf("unexpected mm2=%p; must equal to mm1=%p", mm2, mm1)
	}
	mp.Put(m)
}

func TestMarshalUnmarshalMessageInt32(t *testing.T) {
	for _, fieldNum := range []uint32{0, 1, 10, 1000, 1<<32 - 1} {
		for _, value := range []int32{0, -1, 1, 1<<31 - 1, -1 << 31, -1<<31 + 1} {
			// Marshal int32
			m := mp.Get()
			mm := m.MessageMarshaler()
			mm.AppendInt32(fieldNum, value)
			data := m.Marshal(nil)
			mp.Put(m)

			// Unmarshal int32
			var fc FieldContext
			tail, err := fc.NextField(data)
			if err != nil {
				t.Fatalf("unexpected error in NextField(): %s", err)
			}
			if len(tail) > 0 {
				t.Fatalf("unexpected non-empty tail left with %d bytes", len(tail))
			}
			if fc.FieldNum != fieldNum {
				t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
			}
			v, ok := fc.Int32()
			if !ok {
				t.Fatalf("unexpected error in Int32()")
			}
			if v != value {
				t.Fatalf("unexpected value; got %d; want %d", v, value)
			}
		}
	}
}

func TestMarshalUnmarshalMessageInt64(t *testing.T) {
	for _, fieldNum := range []uint32{0, 1, 10, 1000, 1<<32 - 1} {
		for _, value := range []int64{0, -1, 1, 1<<63 - 1, -1 << 63, -1<<63 + 1} {
			// Marshal int64
			m := mp.Get()
			mm := m.MessageMarshaler()
			mm.AppendInt64(fieldNum, value)
			data := m.Marshal(nil)
			mp.Put(m)

			// Unmarshal int64
			var fc FieldContext
			tail, err := fc.NextField(data)
			if err != nil {
				t.Fatalf("unexpected error in NextField(): %s", err)
			}
			if len(tail) > 0 {
				t.Fatalf("unexpected non-empty tail left with %d bytes", len(tail))
			}
			if fc.FieldNum != fieldNum {
				t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
			}
			v, ok := fc.Int64()
			if !ok {
				t.Fatalf("unexpected error in Int64()")
			}
			if v != value {
				t.Fatalf("unexpected value; got %d; want %d", v, value)
			}
		}
	}
}

func TestMarshalUnmarshalMessageUint32(t *testing.T) {
	for _, fieldNum := range []uint32{0, 1, 10, 1000, 1<<32 - 1} {
		for _, value := range []uint32{0, 1, 2, 1<<32 - 1, 1<<32 - 2} {
			// Marshal uint32
			m := mp.Get()
			mm := m.MessageMarshaler()
			mm.AppendUint32(fieldNum, value)
			data := m.Marshal(nil)
			mp.Put(m)

			// Unmarshal uint32
			var fc FieldContext
			tail, err := fc.NextField(data)
			if err != nil {
				t.Fatalf("unexpected error in NextField(): %s", err)
			}
			if len(tail) > 0 {
				t.Fatalf("unexpected non-empty tail left with %d bytes", len(tail))
			}
			if fc.FieldNum != fieldNum {
				t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
			}
			v, ok := fc.Uint32()
			if !ok {
				t.Fatalf("unexpected error in Uint32()")
			}
			if v != value {
				t.Fatalf("unexpected value; got %d; want %d", v, value)
			}
		}
	}
}

func TestMarshalUnmarshalMessageUint64(t *testing.T) {
	for _, fieldNum := range []uint32{0, 1, 10, 1000, 1<<32 - 1} {
		for _, value := range []uint64{0, 1, 2, 1<<64 - 1, 1<<64 - 2} {
			// Marshal uint64
			m := mp.Get()
			mm := m.MessageMarshaler()
			mm.AppendUint64(fieldNum, value)
			data := m.Marshal(nil)
			mp.Put(m)

			// Unmarshal uint64
			var fc FieldContext
			tail, err := fc.NextField(data)
			if err != nil {
				t.Fatalf("unexpected error in NextField(): %s", err)
			}
			if len(tail) > 0 {
				t.Fatalf("unexpected non-empty tail left with %d bytes", len(tail))
			}
			if fc.FieldNum != fieldNum {
				t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
			}
			v, ok := fc.Uint64()
			if !ok {
				t.Fatalf("unexpected error in Uint64()")
			}
			if v != value {
				t.Fatalf("unexpected value; got %d; want %d", v, value)
			}
		}
	}
}

func TestMarshalUnmarshalMessageSint32(t *testing.T) {
	for _, fieldNum := range []uint32{0, 1, 10, 1000, 1<<32 - 1} {
		for _, value := range []int32{0, -1, 1, 1<<31 - 1, -1 << 31, -1<<31 + 1} {
			// Marshal sint32
			m := mp.Get()
			mm := m.MessageMarshaler()
			mm.AppendSint32(fieldNum, value)
			data := m.Marshal(nil)
			mp.Put(m)

			// Unmarshal sint32
			var fc FieldContext
			tail, err := fc.NextField(data)
			if err != nil {
				t.Fatalf("unexpected error in NextField(): %s", err)
			}
			if len(tail) > 0 {
				t.Fatalf("unexpected non-empty tail left with %d bytes", len(tail))
			}
			if fc.FieldNum != fieldNum {
				t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
			}
			v, ok := fc.Sint32()
			if !ok {
				t.Fatalf("unexpected error in Sint32()")
			}
			if v != value {
				t.Fatalf("unexpected value; got %d; want %d", v, value)
			}
		}
	}
}

func TestMarshalUnmarshalMessageSint64(t *testing.T) {
	for _, fieldNum := range []uint32{0, 1, 10, 1000, 1<<32 - 1} {
		for _, value := range []int64{0, -1, 1, 1<<63 - 1, -1 << 63, -1<<63 + 1} {
			// Marshal sint64
			m := mp.Get()
			mm := m.MessageMarshaler()
			mm.AppendSint64(fieldNum, value)
			data := m.Marshal(nil)
			mp.Put(m)

			// Unmarshal sint64
			var fc FieldContext
			tail, err := fc.NextField(data)
			if err != nil {
				t.Fatalf("unexpected error in NextField(): %s", err)
			}
			if len(tail) > 0 {
				t.Fatalf("unexpected non-empty tail left with %d bytes", len(tail))
			}
			if fc.FieldNum != fieldNum {
				t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
			}
			v, ok := fc.Sint64()
			if !ok {
				t.Fatalf("unexpected error in Sint64()")
			}
			if v != value {
				t.Fatalf("unexpected value; got %d; want %d", v, value)
			}
		}
	}
}

func TestMarshalUnmarshalMessageBool(t *testing.T) {
	for _, fieldNum := range []uint32{0, 1, 10, 1000, 1<<32 - 1} {
		for _, value := range []bool{true, false} {
			// Marshal bool
			m := mp.Get()
			mm := m.MessageMarshaler()
			mm.AppendBool(fieldNum, value)
			data := m.Marshal(nil)
			mp.Put(m)

			// Unmarshal bool
			var fc FieldContext
			tail, err := fc.NextField(data)
			if err != nil {
				t.Fatalf("unexpected error in NextField(): %s", err)
			}
			if len(tail) > 0 {
				t.Fatalf("unexpected non-empty tail left with %d bytes", len(tail))
			}
			if fc.FieldNum != fieldNum {
				t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
			}
			v, ok := fc.Bool()
			if !ok {
				t.Fatalf("unexpected error in Bool()")
			}
			if v != value {
				t.Fatalf("unexpected value; got %v; want %v", v, value)
			}
		}
	}
}

func TestMarshalUnmarshalMessageFixed64(t *testing.T) {
	for _, fieldNum := range []uint32{0, 1, 10, 1000, 1<<32 - 1} {
		for _, value := range []uint64{0, 1, 2, 1<<64 - 1, 1<<64 - 2} {
			// Marshal fixed64
			m := mp.Get()
			mm := m.MessageMarshaler()
			mm.AppendFixed64(fieldNum, value)
			data := m.Marshal(nil)
			mp.Put(m)

			// Unmarshal fixed64
			var fc FieldContext
			tail, err := fc.NextField(data)
			if err != nil {
				t.Fatalf("unexpected error in NextField(): %s", err)
			}
			if len(tail) > 0 {
				t.Fatalf("unexpected non-empty tail left with %d bytes", len(tail))
			}
			if fc.FieldNum != fieldNum {
				t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
			}
			v, ok := fc.Fixed64()
			if !ok {
				t.Fatalf("unexpected error in Fixed64()")
			}
			if v != value {
				t.Fatalf("unexpected value; got %d; want %d", v, value)
			}
		}
	}
}

func TestMarshalUnmarshalMessageSfixed64(t *testing.T) {
	for _, fieldNum := range []uint32{0, 1, 10, 1000, 1<<32 - 1} {
		for _, value := range []int64{0, -1, 1, 1<<63 - 1, -1 << 63, -1<<63 + 1} {
			// Marshal sfixed64
			m := mp.Get()
			mm := m.MessageMarshaler()
			mm.AppendSfixed64(fieldNum, value)
			data := m.Marshal(nil)
			mp.Put(m)

			// Unmarshal sfixed64
			var fc FieldContext
			tail, err := fc.NextField(data)
			if err != nil {
				t.Fatalf("unexpected error in NextField(): %s", err)
			}
			if len(tail) > 0 {
				t.Fatalf("unexpected non-empty tail left with %d bytes", len(tail))
			}
			if fc.FieldNum != fieldNum {
				t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
			}
			v, ok := fc.Sfixed64()
			if !ok {
				t.Fatalf("unexpected error in Sfixed64()")
			}
			if v != value {
				t.Fatalf("unexpected value; got %d; want %d", v, value)
			}
		}
	}
}

func TestMarshalUnmarshalMessageDouble(t *testing.T) {
	for _, fieldNum := range []uint32{0, 1, 10, 1000, 1<<32 - 1} {
		for _, value := range []float64{0, -1, 1, 1.0 / 3, -1.0 / 3, 1.234e43, -323.34387e-4} {
			// Marshal double
			m := mp.Get()
			mm := m.MessageMarshaler()
			mm.AppendDouble(fieldNum, value)
			data := m.Marshal(nil)
			mp.Put(m)

			// Unmarshal double
			var fc FieldContext
			tail, err := fc.NextField(data)
			if err != nil {
				t.Fatalf("unexpected error in NextField(): %s", err)
			}
			if len(tail) > 0 {
				t.Fatalf("unexpected non-empty tail left with %d bytes", len(tail))
			}
			if fc.FieldNum != fieldNum {
				t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
			}
			v, ok := fc.Double()
			if !ok {
				t.Fatalf("unexpected error in Double()")
			}
			if v != value {
				t.Fatalf("unexpected value; got %v; want %v", v, value)
			}
		}
	}
}

func TestMarshalUnmarshalMessageString(t *testing.T) {
	for _, fieldNum := range []uint32{0, 1, 10, 1000, 1<<32 - 1} {
		for _, value := range []string{"", "foo", "foo bar baz"} {
			// Marshal string
			m := mp.Get()
			mm := m.MessageMarshaler()
			mm.AppendString(fieldNum, value)
			data := m.Marshal(nil)
			mp.Put(m)

			// Unmarshal string
			var fc FieldContext
			tail, err := fc.NextField(data)
			if err != nil {
				t.Fatalf("unexpected error in NextField(): %s", err)
			}
			if len(tail) > 0 {
				t.Fatalf("unexpected non-empty tail left with %d bytes", len(tail))
			}
			if fc.FieldNum != fieldNum {
				t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
			}
			v, ok := fc.String()
			if !ok {
				t.Fatalf("cannot get string field")
			}
			if v != value {
				t.Fatalf("unexpected value; got %q; want %q", v, value)
			}
		}
	}
}

func TestMarshalUnmarshalMessageBytes(t *testing.T) {
	for _, fieldNum := range []uint32{0, 1, 10, 1000, 1<<32 - 1} {
		for _, value := range []string{"", "foo", "foo bar baz"} {
			// Marshal bytes
			m := mp.Get()
			mm := m.MessageMarshaler()
			mm.AppendBytes(fieldNum, []byte(value))
			data := m.Marshal(nil)
			mp.Put(m)

			// Unmarshal bytes
			var fc FieldContext
			tail, err := fc.NextField(data)
			if err != nil {
				t.Fatalf("unexpected error in NextField(): %s", err)
			}
			if len(tail) > 0 {
				t.Fatalf("unexpected non-empty tail left with %d bytes", len(tail))
			}
			if fc.FieldNum != fieldNum {
				t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
			}
			v, ok := fc.Bytes()
			if !ok {
				t.Fatalf("cannot get bytes field")
			}
			if string(v) != value {
				t.Fatalf("unexpected value; got %q; want %q", v, value)
			}
		}
	}
}

func TestMarshalUnmarshalMessageFixed32(t *testing.T) {
	for _, fieldNum := range []uint32{0, 1, 10, 1000, 1<<32 - 1} {
		for _, value := range []uint32{0, 1, 2, 1<<32 - 1, 1<<32 - 2} {
			// Marshal fixed32
			m := mp.Get()
			mm := m.MessageMarshaler()
			mm.AppendFixed32(fieldNum, value)
			data := m.Marshal(nil)
			mp.Put(m)

			// Unmarshal fixed32
			var fc FieldContext
			tail, err := fc.NextField(data)
			if err != nil {
				t.Fatalf("unexpected error in NextField(): %s", err)
			}
			if len(tail) > 0 {
				t.Fatalf("unexpected non-empty tail left with %d bytes", len(tail))
			}
			if fc.FieldNum != fieldNum {
				t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
			}
			v, ok := fc.Fixed32()
			if !ok {
				t.Fatalf("unexpected error in Fixed32()")
			}
			if v != value {
				t.Fatalf("unexpected value; got %d; want %d", v, value)
			}
		}
	}
}

func TestMarshalUnmarshalMessageSfixed32(t *testing.T) {
	for _, fieldNum := range []uint32{0, 1, 10, 1000, 1<<32 - 1} {
		for _, value := range []int32{0, -1, 1, 1<<31 - 1, -1 << 31, -1<<31 + 1} {
			// Marshal sfixed32
			m := mp.Get()
			mm := m.MessageMarshaler()
			mm.AppendSfixed32(fieldNum, value)
			data := m.Marshal(nil)
			mp.Put(m)

			// Unmarshal sfixed32
			var fc FieldContext
			tail, err := fc.NextField(data)
			if err != nil {
				t.Fatalf("unexpected error in NextField(): %s", err)
			}
			if len(tail) > 0 {
				t.Fatalf("unexpected non-empty tail left with %d bytes", len(tail))
			}
			if fc.FieldNum != fieldNum {
				t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
			}
			v, ok := fc.Sfixed32()
			if !ok {
				t.Fatalf("unexpected error in Sfixed32()")
			}
			if v != value {
				t.Fatalf("unexpected value; got %d; want %d", v, value)
			}
		}
	}
}

func TestMarshalUnmarshalMessageFloat(t *testing.T) {
	for _, fieldNum := range []uint32{0, 1, 10, 1000, 1<<32 - 1} {
		for _, value := range []float32{0, -1, 1, 1.0 / 3, -1.0 / 3, 1.234e20, -323.34387e-4} {
			// Marshal float
			m := mp.Get()
			mm := m.MessageMarshaler()
			mm.AppendFloat(fieldNum, value)
			data := m.Marshal(nil)
			mp.Put(m)

			// Unmarshal float
			var fc FieldContext
			tail, err := fc.NextField(data)
			if err != nil {
				t.Fatalf("unexpected error in NextField(): %s", err)
			}
			if len(tail) > 0 {
				t.Fatalf("unexpected non-empty tail left with %d bytes", len(tail))
			}
			if fc.FieldNum != fieldNum {
				t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
			}
			v, ok := fc.Float()
			if !ok {
				t.Fatalf("unexpected error in Float()")
			}
			if v != value {
				t.Fatalf("unexpected value; got %v; want %v", v, value)
			}
		}
	}
}

func TestMarshalUnmarshalEmptyInt32s(t *testing.T) {
	const fieldNum = 42

	m := mp.Get()
	mm := m.MessageMarshaler()
	mm.AppendInt32s(fieldNum, nil)
	data := m.Marshal(nil)
	mp.Put(m)

	var fc FieldContext
	tail, err := fc.NextField(data)
	if err != nil {
		t.Fatalf("unexpected error in NextField(): %s", err)
	}
	if len(tail) != 0 {
		t.Fatalf("unexpected non-empty tail with len(tail)=%d", len(tail))
	}
	if fc.FieldNum != fieldNum {
		t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
	}
	vs, ok := fc.UnpackInt32s(nil)
	if !ok {
		t.Fatalf("unexpected error in UnpackInt32s()")
	}
	if len(vs) > 0 {
		t.Fatalf("unexpected non-empty vs=%d", vs)
	}
}

func TestMarshalUnmarshalEmptyInt64s(t *testing.T) {
	const fieldNum = 42

	m := mp.Get()
	mm := m.MessageMarshaler()
	mm.AppendInt64s(fieldNum, nil)
	data := m.Marshal(nil)
	mp.Put(m)

	var fc FieldContext
	tail, err := fc.NextField(data)
	if err != nil {
		t.Fatalf("unexpected error in NextField(): %s", err)
	}
	if len(tail) != 0 {
		t.Fatalf("unexpected non-empty tail with len(tail)=%d", len(tail))
	}
	if fc.FieldNum != fieldNum {
		t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
	}
	vs, ok := fc.UnpackInt64s(nil)
	if !ok {
		t.Fatalf("unexpected error in UnpackInt64s()")
	}
	if len(vs) > 0 {
		t.Fatalf("unexpected non-empty vs=%d", vs)
	}
}

func TestMarshalUnmarshalEmptyUint32s(t *testing.T) {
	const fieldNum = 42

	m := mp.Get()
	mm := m.MessageMarshaler()
	mm.AppendUint32s(fieldNum, nil)
	data := m.Marshal(nil)
	mp.Put(m)

	var fc FieldContext
	tail, err := fc.NextField(data)
	if err != nil {
		t.Fatalf("unexpected error in NextField(): %s", err)
	}
	if len(tail) != 0 {
		t.Fatalf("unexpected non-empty tail with len(tail)=%d", len(tail))
	}
	if fc.FieldNum != fieldNum {
		t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
	}
	vs, ok := fc.UnpackUint32s(nil)
	if !ok {
		t.Fatalf("unexpected error in UnpackUint32s()")
	}
	if len(vs) > 0 {
		t.Fatalf("unexpected non-empty vs=%d", vs)
	}
}

func TestMarshalUnmarshalEmptyUint64s(t *testing.T) {
	const fieldNum = 42

	m := mp.Get()
	mm := m.MessageMarshaler()
	mm.AppendUint64s(fieldNum, nil)
	data := m.Marshal(nil)
	mp.Put(m)

	var fc FieldContext
	tail, err := fc.NextField(data)
	if err != nil {
		t.Fatalf("unexpected error in NextField(): %s", err)
	}
	if len(tail) != 0 {
		t.Fatalf("unexpected non-empty tail with len(tail)=%d", len(tail))
	}
	if fc.FieldNum != fieldNum {
		t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
	}
	vs, ok := fc.UnpackUint64s(nil)
	if !ok {
		t.Fatalf("unexpected error in UnpackUint64s()")
	}
	if len(vs) > 0 {
		t.Fatalf("unexpected non-empty vs=%d", vs)
	}
}

func TestMarshalUnmarshalEmptySint32s(t *testing.T) {
	const fieldNum = 42

	m := mp.Get()
	mm := m.MessageMarshaler()
	mm.AppendSint32s(fieldNum, nil)
	data := m.Marshal(nil)
	mp.Put(m)

	var fc FieldContext
	tail, err := fc.NextField(data)
	if err != nil {
		t.Fatalf("unexpected error in NextField(): %s", err)
	}
	if len(tail) != 0 {
		t.Fatalf("unexpected non-empty tail with len(tail)=%d", len(tail))
	}
	if fc.FieldNum != fieldNum {
		t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
	}
	vs, ok := fc.UnpackSint32s(nil)
	if !ok {
		t.Fatalf("unexpected error in UnpackSint32s()")
	}
	if len(vs) > 0 {
		t.Fatalf("unexpected non-empty vs=%d", vs)
	}
}

func TestMarshalUnmarshalEmptySint64s(t *testing.T) {
	const fieldNum = 42

	m := mp.Get()
	mm := m.MessageMarshaler()
	mm.AppendSint64s(fieldNum, nil)
	data := m.Marshal(nil)
	mp.Put(m)

	var fc FieldContext
	tail, err := fc.NextField(data)
	if err != nil {
		t.Fatalf("unexpected error in NextField(): %s", err)
	}
	if len(tail) != 0 {
		t.Fatalf("unexpected non-empty tail with len(tail)=%d", len(tail))
	}
	if fc.FieldNum != fieldNum {
		t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
	}
	vs, ok := fc.UnpackSint64s(nil)
	if !ok {
		t.Fatalf("unexpected error in UnpackSint64s()")
	}
	if len(vs) > 0 {
		t.Fatalf("unexpected non-empty vs=%d", vs)
	}
}

func TestMarshalUnmarshalEmptyBools(t *testing.T) {
	const fieldNum = 42

	m := mp.Get()
	mm := m.MessageMarshaler()
	mm.AppendBools(fieldNum, nil)
	data := m.Marshal(nil)
	mp.Put(m)

	var fc FieldContext
	tail, err := fc.NextField(data)
	if err != nil {
		t.Fatalf("unexpected error in NextField(): %s", err)
	}
	if len(tail) != 0 {
		t.Fatalf("unexpected non-empty tail with len(tail)=%d", len(tail))
	}
	if fc.FieldNum != fieldNum {
		t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
	}
	vs, ok := fc.UnpackBools(nil)
	if !ok {
		t.Fatalf("unexpected error in UnpackBools()")
	}
	if len(vs) > 0 {
		t.Fatalf("unexpected non-empty vs=%v", vs)
	}
}

func TestMarshalUnmarshalEmptyFixed64s(t *testing.T) {
	const fieldNum = 42

	m := mp.Get()
	mm := m.MessageMarshaler()
	mm.AppendFixed64s(fieldNum, nil)
	data := m.Marshal(nil)
	mp.Put(m)

	var fc FieldContext
	tail, err := fc.NextField(data)
	if err != nil {
		t.Fatalf("unexpected error in NextField(): %s", err)
	}
	if len(tail) != 0 {
		t.Fatalf("unexpected non-empty tail with len(tail)=%d", len(tail))
	}
	if fc.FieldNum != fieldNum {
		t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
	}
	vs, ok := fc.UnpackFixed64s(nil)
	if !ok {
		t.Fatalf("unexpected error in UnpackFixed64s()")
	}
	if len(vs) > 0 {
		t.Fatalf("unexpected non-empty vs=%d", vs)
	}
}

func TestMarshalUnmarshalEmptySfixed64s(t *testing.T) {
	const fieldNum = 42

	m := mp.Get()
	mm := m.MessageMarshaler()
	mm.AppendSfixed64s(fieldNum, nil)
	data := m.Marshal(nil)
	mp.Put(m)

	var fc FieldContext
	tail, err := fc.NextField(data)
	if err != nil {
		t.Fatalf("unexpected error in NextField(): %s", err)
	}
	if len(tail) != 0 {
		t.Fatalf("unexpected non-empty tail with len(tail)=%d", len(tail))
	}
	if fc.FieldNum != fieldNum {
		t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
	}
	vs, ok := fc.UnpackSfixed64s(nil)
	if !ok {
		t.Fatalf("unexpected error in UnpackSfixed64s()")
	}
	if len(vs) > 0 {
		t.Fatalf("unexpected non-empty vs=%d", vs)
	}
}

func TestMarshalUnmarshalEmptyDoubles(t *testing.T) {
	const fieldNum = 42

	m := mp.Get()
	mm := m.MessageMarshaler()
	mm.AppendDoubles(fieldNum, nil)
	data := m.Marshal(nil)
	mp.Put(m)

	var fc FieldContext
	tail, err := fc.NextField(data)
	if err != nil {
		t.Fatalf("unexpected error in NextField(): %s", err)
	}
	if len(tail) != 0 {
		t.Fatalf("unexpected non-empty tail with len(tail)=%d", len(tail))
	}
	if fc.FieldNum != fieldNum {
		t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
	}
	vs, ok := fc.UnpackDoubles(nil)
	if !ok {
		t.Fatalf("unexpected error in UnpackDobules()")
	}
	if len(vs) > 0 {
		t.Fatalf("unexpected non-empty vs=%v", vs)
	}
}

func TestMarshalUnmarshalEmptyFixed32s(t *testing.T) {
	const fieldNum = 42

	m := mp.Get()
	mm := m.MessageMarshaler()
	mm.AppendFixed32s(fieldNum, nil)
	data := m.Marshal(nil)
	mp.Put(m)

	var fc FieldContext
	tail, err := fc.NextField(data)
	if err != nil {
		t.Fatalf("unexpected error in NextField(): %s", err)
	}
	if len(tail) != 0 {
		t.Fatalf("unexpected non-empty tail with len(tail)=%d", len(tail))
	}
	if fc.FieldNum != fieldNum {
		t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
	}
	vs, ok := fc.UnpackFixed32s(nil)
	if !ok {
		t.Fatalf("unexpected error in UnpackFixed32s()")
	}
	if len(vs) > 0 {
		t.Fatalf("unexpected non-empty vs=%d", vs)
	}
}

func TestMarshalUnmarshalEmptySfixed32s(t *testing.T) {
	const fieldNum = 42

	m := mp.Get()
	mm := m.MessageMarshaler()
	mm.AppendSfixed32s(fieldNum, nil)
	data := m.Marshal(nil)
	mp.Put(m)

	var fc FieldContext
	tail, err := fc.NextField(data)
	if err != nil {
		t.Fatalf("unexpected error in NextField(): %s", err)
	}
	if len(tail) != 0 {
		t.Fatalf("unexpected non-empty tail with len(tail)=%d", len(tail))
	}
	if fc.FieldNum != fieldNum {
		t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
	}
	vs, ok := fc.UnpackSfixed32s(nil)
	if !ok {
		t.Fatalf("unexpected error in UnpackSfixed32s()")
	}
	if len(vs) > 0 {
		t.Fatalf("unexpected non-empty vs=%d", vs)
	}
}

func TestMarshalUnmarshalEmptyFloats(t *testing.T) {
	const fieldNum = 42

	m := mp.Get()
	mm := m.MessageMarshaler()
	mm.AppendFloats(fieldNum, nil)
	data := m.Marshal(nil)
	mp.Put(m)

	var fc FieldContext
	tail, err := fc.NextField(data)
	if err != nil {
		t.Fatalf("unexpected error in NextField(): %s", err)
	}
	if len(tail) != 0 {
		t.Fatalf("unexpected non-empty tail with len(tail)=%d", len(tail))
	}
	if fc.FieldNum != fieldNum {
		t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
	}
	vs, ok := fc.UnpackFloats(nil)
	if !ok {
		t.Fatalf("unexpected error in UnpackFloats()")
	}
	if len(vs) > 0 {
		t.Fatalf("unexpected non-empty vs=%v", vs)
	}
}

func TestMarshalUnmarshalInt32s(t *testing.T) {
	const fieldNum = 42
	values := []int32{0, -1, 1, 1<<31 - 1, -1 << 31, -1<<31 + 1, 123}

	// marshal int32 values
	m := mp.Get()
	mm := m.MessageMarshaler()
	mm.AppendInt32s(fieldNum, values[:len(values)-1])
	mm.AppendInt32(fieldNum, values[len(values)-1])
	data := m.Marshal(nil)
	mp.Put(m)

	// Unmarshal int32 values
	var vs []int32
	var fc FieldContext
	for len(data) > 0 {
		tail, err := fc.NextField(data)
		if err != nil {
			t.Fatalf("unexpected error in NextField(): %s", err)
		}
		if fc.FieldNum != fieldNum {
			t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
		}
		var ok bool
		vs, ok = fc.UnpackInt32s(vs)
		if !ok {
			t.Fatalf("unexpected error in UnpackInt32s()")
		}
		data = tail
	}

	if !reflect.DeepEqual(vs, values) {
		t.Fatalf("unexpected values; got %d; want %d", vs, values)
	}
}

func TestMarshalUnmarshalInt64s(t *testing.T) {
	const fieldNum = 42
	values := []int64{0, -1, 1, 1<<63 - 1, -1 << 63, -1<<63 + 1, 123}

	// marshal int64 values
	m := mp.Get()
	mm := m.MessageMarshaler()
	mm.AppendInt64s(fieldNum, values[:len(values)-1])
	mm.AppendInt64(fieldNum, values[len(values)-1])
	data := m.Marshal(nil)
	mp.Put(m)

	// Unmarshal int64 values
	var vs []int64
	var fc FieldContext
	for len(data) > 0 {
		tail, err := fc.NextField(data)
		if err != nil {
			t.Fatalf("unexpected error in NextField(): %s", err)
		}
		if fc.FieldNum != fieldNum {
			t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
		}
		var ok bool
		vs, ok = fc.UnpackInt64s(vs)
		if !ok {
			t.Fatalf("unexpected error in UnpackInt64s()")
		}
		data = tail
	}

	if !reflect.DeepEqual(vs, values) {
		t.Fatalf("unexpected values; got %d; want %d", vs, values)
	}
}

func TestMarshalUnmarshalUint32s(t *testing.T) {
	const fieldNum = 42
	values := []uint32{0, 1, 1<<32 - 1, 1<<32 - 2, 123}

	// marshal uint32 values
	m := mp.Get()
	mm := m.MessageMarshaler()
	mm.AppendUint32s(fieldNum, values[:len(values)-1])
	mm.AppendUint32(fieldNum, values[len(values)-1])
	data := m.Marshal(nil)
	mp.Put(m)

	// Unmarshal uint32 values
	var vs []uint32
	var fc FieldContext
	for len(data) > 0 {
		tail, err := fc.NextField(data)
		if err != nil {
			t.Fatalf("unexpected error in NextField(): %s", err)
		}
		if fc.FieldNum != fieldNum {
			t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
		}
		var ok bool
		vs, ok = fc.UnpackUint32s(vs)
		if !ok {
			t.Fatalf("unexpected error in UnpackUint32s()")
		}
		data = tail
	}

	if !reflect.DeepEqual(vs, values) {
		t.Fatalf("unexpected values; got %d; want %d", vs, values)
	}
}

func TestMarshalUnmarshalUint64s(t *testing.T) {
	const fieldNum = 42
	values := []uint64{0, 1, 1<<64 - 1, 1<<64 - 2, 123}

	// marshal uint64 values
	m := mp.Get()
	mm := m.MessageMarshaler()
	mm.AppendUint64s(fieldNum, values[:len(values)-1])
	mm.AppendUint64(fieldNum, values[len(values)-1])
	data := m.Marshal(nil)
	mp.Put(m)

	// Unmarshal uint64 values
	var vs []uint64
	var fc FieldContext
	for len(data) > 0 {
		tail, err := fc.NextField(data)
		if err != nil {
			t.Fatalf("unexpected error in NextField(): %s", err)
		}
		if fc.FieldNum != fieldNum {
			t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
		}
		var ok bool
		vs, ok = fc.UnpackUint64s(vs)
		if !ok {
			t.Fatalf("unexpected error in UnpackUint64s()")
		}
		data = tail
	}

	if !reflect.DeepEqual(vs, values) {
		t.Fatalf("unexpected values; got %d; want %d", vs, values)
	}
}

func TestMarshalUnmarshalSint32s(t *testing.T) {
	const fieldNum = 42
	values := []int32{0, -1, 1, 1<<31 - 1, -1 << 31, -1<<31 + 1, 123}

	// marshal sint32 values
	m := mp.Get()
	mm := m.MessageMarshaler()
	mm.AppendSint32s(fieldNum, values[:len(values)-1])
	mm.AppendSint32(fieldNum, values[len(values)-1])
	data := m.Marshal(nil)
	mp.Put(m)

	// Unmarshal sint32 values
	var vs []int32
	var fc FieldContext
	for len(data) > 0 {
		tail, err := fc.NextField(data)
		if err != nil {
			t.Fatalf("unexpected error in NextField(): %s", err)
		}
		if fc.FieldNum != fieldNum {
			t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
		}
		var ok bool
		vs, ok = fc.UnpackSint32s(vs)
		if !ok {
			t.Fatalf("unexpected error in UnpackSint32s()")
		}
		data = tail
	}

	if !reflect.DeepEqual(vs, values) {
		t.Fatalf("unexpected values; got %d; want %d", vs, values)
	}
}

func TestMarshalUnmarshalSint64s(t *testing.T) {
	const fieldNum = 42
	values := []int64{0, -1, 1, 1<<63 - 1, -1 << 63, -1<<63 + 1, 123}

	// marshal sint64 values
	m := mp.Get()
	mm := m.MessageMarshaler()
	mm.AppendSint64s(fieldNum, values[:len(values)-1])
	mm.AppendSint64(fieldNum, values[len(values)-1])
	data := m.Marshal(nil)
	mp.Put(m)

	// Unmarshal sint64 values
	var vs []int64
	var fc FieldContext
	for len(data) > 0 {
		tail, err := fc.NextField(data)
		if err != nil {
			t.Fatalf("unexpected error in NextField(): %s", err)
		}
		if fc.FieldNum != fieldNum {
			t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
		}
		var ok bool
		vs, ok = fc.UnpackSint64s(vs)
		if !ok {
			t.Fatalf("unexpected error in UnpackSint64s()")
		}
		data = tail
	}

	if !reflect.DeepEqual(vs, values) {
		t.Fatalf("unexpected values; got %d; want %d", vs, values)
	}
}

func TestMarshalUnmarshalBools(t *testing.T) {
	const fieldNum = 42
	values := []bool{true, false, false, true, true}

	// marshal bool values
	m := mp.Get()
	mm := m.MessageMarshaler()
	mm.AppendBools(fieldNum, values[:len(values)-1])
	mm.AppendBool(fieldNum, values[len(values)-1])
	data := m.Marshal(nil)
	mp.Put(m)

	// Unmarshal bool values
	var vs []bool
	var fc FieldContext
	for len(data) > 0 {
		tail, err := fc.NextField(data)
		if err != nil {
			t.Fatalf("unexpected error in NextField(): %s", err)
		}
		if fc.FieldNum != fieldNum {
			t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
		}
		var ok bool
		vs, ok = fc.UnpackBools(vs)
		if !ok {
			t.Fatalf("unexpected error in UnpackBools()")
		}
		data = tail
	}

	if !reflect.DeepEqual(vs, values) {
		t.Fatalf("unexpected values; got %v; want %v", vs, values)
	}
}

func TestMarshalUnmarshalFixed64s(t *testing.T) {
	const fieldNum = 42
	values := []uint64{0, 1, 1<<64 - 1, 1<<64 - 1, 123, 1 << 32, 1<<64 - 2}

	// marshal fixed64 values
	m := mp.Get()
	mm := m.MessageMarshaler()
	mm.AppendFixed64s(fieldNum, values[:len(values)-1])
	mm.AppendFixed64(fieldNum, values[len(values)-1])
	data := m.Marshal(nil)
	mp.Put(m)

	// Unmarshal fixed64 values
	var vs []uint64
	var fc FieldContext
	for len(data) > 0 {
		tail, err := fc.NextField(data)
		if err != nil {
			t.Fatalf("unexpected error in NextField(): %s", err)
		}
		if fc.FieldNum != fieldNum {
			t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
		}
		var ok bool
		vs, ok = fc.UnpackFixed64s(vs)
		if !ok {
			t.Fatalf("unexpected error in UnpackFixed64s()")
		}
		data = tail
	}

	if !reflect.DeepEqual(vs, values) {
		t.Fatalf("unexpected values; got %d; want %d", vs, values)
	}
}

func TestMarshalUnmarshalSfixed64s(t *testing.T) {
	const fieldNum = 42
	values := []int64{0, -1, 1, 1<<63 - 1, -1 << 63, -1<<63 + 1, 123}

	// marshal sfixed64 values
	m := mp.Get()
	mm := m.MessageMarshaler()
	mm.AppendSfixed64s(fieldNum, values[:len(values)-1])
	mm.AppendSfixed64(fieldNum, values[len(values)-1])
	data := m.Marshal(nil)
	mp.Put(m)

	// Unmarshal sfixed64 values
	var vs []int64
	var fc FieldContext
	for len(data) > 0 {
		tail, err := fc.NextField(data)
		if err != nil {
			t.Fatalf("unexpected error in NextField(): %s", err)
		}
		if fc.FieldNum != fieldNum {
			t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
		}
		var ok bool
		vs, ok = fc.UnpackSfixed64s(vs)
		if !ok {
			t.Fatalf("unexpected error in UnpackSfixed64s()")
		}
		data = tail
	}

	if !reflect.DeepEqual(vs, values) {
		t.Fatalf("unexpected values; got %d; want %d", vs, values)
	}
}

func TestMarshalUnmarshalDoubles(t *testing.T) {
	const fieldNum = 42
	values := []float64{0, -1, 1, 1<<63 - 1, -1 << 63, -1<<63 + 1, 123, 1.34, -34.34e2, 1.0 / 3, -1.0 / 3}

	// marshal double values
	m := mp.Get()
	mm := m.MessageMarshaler()
	mm.AppendDoubles(fieldNum, values[:len(values)-1])
	mm.AppendDouble(fieldNum, values[len(values)-1])
	data := m.Marshal(nil)
	mp.Put(m)

	// Unmarshal double values
	var vs []float64
	var fc FieldContext
	for len(data) > 0 {
		tail, err := fc.NextField(data)
		if err != nil {
			t.Fatalf("unexpected error in NextField(): %s", err)
		}
		if fc.FieldNum != fieldNum {
			t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
		}
		var ok bool
		vs, ok = fc.UnpackDoubles(vs)
		if !ok {
			t.Fatalf("unexpected error in UnpackDoubles()")
		}
		data = tail
	}

	if !reflect.DeepEqual(vs, values) {
		t.Fatalf("unexpected values; got %v; want %v", vs, values)
	}
}

func TestMarshalUnmarshalFixed32s(t *testing.T) {
	const fieldNum = 42
	values := []uint32{0, 1, 1<<32 - 1, 1<<32 - 1, 123, 1 << 16, 1<<32 - 2}

	// marshal fixed32 values
	m := mp.Get()
	mm := m.MessageMarshaler()
	mm.AppendFixed32s(fieldNum, values[:len(values)-1])
	mm.AppendFixed32(fieldNum, values[len(values)-1])
	data := m.Marshal(nil)
	mp.Put(m)

	// Unmarshal fixed32 values
	var vs []uint32
	var fc FieldContext
	for len(data) > 0 {
		tail, err := fc.NextField(data)
		if err != nil {
			t.Fatalf("unexpected error in NextField(): %s", err)
		}
		if fc.FieldNum != fieldNum {
			t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
		}
		var ok bool
		vs, ok = fc.UnpackFixed32s(vs)
		if !ok {
			t.Fatalf("unexpected error in UnpackFixed32s()")
		}
		data = tail
	}

	if !reflect.DeepEqual(vs, values) {
		t.Fatalf("unexpected values; got %d; want %d", vs, values)
	}
}

func TestMarshalUnmarshalSfixed32s(t *testing.T) {
	const fieldNum = 42
	values := []int32{0, 1, -1, 1<<31 - 1, -1 << 31, 123, 1 << 16, -1<<31 + 1}

	// marshal sfixed32 values
	m := mp.Get()
	mm := m.MessageMarshaler()
	mm.AppendSfixed32s(fieldNum, values[:len(values)-1])
	mm.AppendSfixed32(fieldNum, values[len(values)-1])
	data := m.Marshal(nil)
	mp.Put(m)

	// Unmarshal sfixed32 values
	var vs []int32
	var fc FieldContext
	for len(data) > 0 {
		tail, err := fc.NextField(data)
		if err != nil {
			t.Fatalf("unexpected error in NextField(): %s", err)
		}
		if fc.FieldNum != fieldNum {
			t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
		}
		var ok bool
		vs, ok = fc.UnpackSfixed32s(vs)
		if !ok {
			t.Fatalf("unexpected error in UnpackSfixed32s()")
		}
		data = tail
	}

	if !reflect.DeepEqual(vs, values) {
		t.Fatalf("unexpected values; got %d; want %d", vs, values)
	}
}

func TestMarshalUnmarshalFloats(t *testing.T) {
	const fieldNum = 42
	values := []float32{0, -1, 1, 1<<63 - 1, -1 << 63, -1<<63 + 1, 123, 1.34, -34.34e2, 1.0 / 3, -1.0 / 3}

	// marshal float values
	m := mp.Get()
	mm := m.MessageMarshaler()
	mm.AppendFloats(fieldNum, values[:len(values)-1])
	mm.AppendFloat(fieldNum, values[len(values)-1])
	data := m.Marshal(nil)
	mp.Put(m)

	// Unmarshal float values
	var vs []float32
	var fc FieldContext
	for len(data) > 0 {
		tail, err := fc.NextField(data)
		if err != nil {
			t.Fatalf("unexpected error in NextField(): %s", err)
		}
		if fc.FieldNum != fieldNum {
			t.Fatalf("unexpected fieldNum; got %d; want %d", fc.FieldNum, fieldNum)
		}
		var ok bool
		vs, ok = fc.UnpackFloats(vs)
		if !ok {
			t.Fatalf("unexpected error in UnpackFloats()")
		}
		data = tail
	}

	if !reflect.DeepEqual(vs, values) {
		t.Fatalf("unexpected values; got %v; want %v", vs, values)
	}
}

func TestMarshalUnmarshalMessage(t *testing.T) {
	type label struct {
		Name  string
		Value string
	}
	type sample struct {
		Value     float64
		Timestamp int64
	}
	type timeseries struct {
		Labels  []label
		Samples []sample
	}
	type writeRequest struct {
		Timeseries []timeseries
	}

	const timeseriesFieldNum = 1
	const labelFieldNum = 1
	const sampleFieldNum = 2
	const labelNameFieldNum = 1
	const labelValueFieldNum = 2
	const sampleValueFieldNum = 1
	const sampleTimestampFieldNum = 2

	marshalWriteRequest := func(wr *writeRequest) []byte {
		m := mp.Get()
		mm := m.MessageMarshaler()
		for _, ts := range wr.Timeseries {
			tsm := mm.AppendMessage(timeseriesFieldNum)
			for _, label := range ts.Labels {
				lm := tsm.AppendMessage(labelFieldNum)
				lm.AppendString(labelNameFieldNum, label.Name)
				lm.AppendString(labelValueFieldNum, label.Value)
			}
			for _, sample := range ts.Samples {
				sm := tsm.AppendMessage(sampleFieldNum)
				sm.AppendDouble(sampleValueFieldNum, sample.Value)
				sm.AppendInt64(sampleTimestampFieldNum, sample.Timestamp)
			}
		}
		data := m.Marshal(nil)
		mp.Put(m)
		return data
	}

	unmarshalLabel := func(src []byte) (label, error) {
		var lbl label
		var fc FieldContext
		for len(src) > 0 {
			tail, err := fc.NextField(src)
			if err != nil {
				return lbl, fmt.Errorf("cannot obtain next field in label: %w", err)
			}
			switch fc.FieldNum {
			case labelNameFieldNum:
				name, ok := fc.String()
				if !ok {
					return lbl, fmt.Errorf("cannot unmarshal label name")
				}
				lbl.Name = name
			case labelValueFieldNum:
				value, ok := fc.String()
				if !ok {
					return lbl, fmt.Errorf("cannot unmarshal label value")
				}
				lbl.Value = value
			default:
				return lbl, fmt.Errorf("unexpected fieldNum=%d in label", fc.FieldNum)
			}
			src = tail
		}
		return lbl, nil
	}
	unmarshalSample := func(src []byte) (sample, error) {
		var smpl sample
		var fc FieldContext
		for len(src) > 0 {
			tail, err := fc.NextField(src)
			if err != nil {
				return smpl, fmt.Errorf("cannot obtain next field in sample: %w", err)
			}
			switch fc.FieldNum {
			case sampleValueFieldNum:
				value, ok := fc.Double()
				if !ok {
					return smpl, fmt.Errorf("cannot unmarshal sample value")
				}
				smpl.Value = value
			case sampleTimestampFieldNum:
				timestamp, ok := fc.Int64()
				if !ok {
					return smpl, fmt.Errorf("cannot unmarshal sample timestamp")
				}
				smpl.Timestamp = timestamp
			default:
				return smpl, fmt.Errorf("unexpectef fieldNum=%d in sample", fc.FieldNum)
			}
			src = tail
		}
		return smpl, nil
	}
	unmarshalTimeseries := func(src []byte) (timeseries, error) {
		var ts timeseries
		var fc FieldContext
		for len(src) > 0 {
			tail, err := fc.NextField(src)
			if err != nil {
				return ts, fmt.Errorf("cannot obtain next field in timeseries: %w", err)
			}
			switch fc.FieldNum {
			case labelFieldNum:
				data, ok := fc.MessageData()
				if !ok {
					return ts, fmt.Errorf("cannot obtain message data for label")
				}
				label, err := unmarshalLabel(data)
				if err != nil {
					return ts, fmt.Errorf("cannot unmarshal label: %w", err)
				}
				ts.Labels = append(ts.Labels, label)
			case sampleFieldNum:
				data, ok := fc.MessageData()
				if !ok {
					return ts, fmt.Errorf("cannot obtain message data for sample")
				}
				sample, err := unmarshalSample(data)
				if err != nil {
					return ts, fmt.Errorf("cannot unmarshal sample: %w", err)
				}
				ts.Samples = append(ts.Samples, sample)
			default:
				return ts, fmt.Errorf("unexpected fieldNum=%d in timeseries", fc.FieldNum)
			}
			src = tail
		}
		return ts, nil
	}
	unmarshalWriteRequest := func(src []byte) (*writeRequest, error) {
		var tss []timeseries
		var fc FieldContext
		for len(src) > 0 {
			tail, err := fc.NextField(src)
			if err != nil {
				return nil, fmt.Errorf("cannot obtain next field in writeRequest: %w", err)
			}
			switch fc.FieldNum {
			case timeseriesFieldNum:
				data, ok := fc.MessageData()
				if !ok {
					return nil, fmt.Errorf("cannot obtain message data for timeseries")
				}
				ts, err := unmarshalTimeseries(data)
				if err != nil {
					return nil, fmt.Errorf("cannot unmarshal timeseries: %w", err)
				}
				tss = append(tss, ts)
			default:
				return nil, fmt.Errorf("unexpected fieldNum=%d in writeRequest", fc.FieldNum)
			}
			src = tail
		}
		wr := &writeRequest{
			Timeseries: tss,
		}
		return wr, nil
	}

	wr := &writeRequest{
		Timeseries: []timeseries{
			{
				Labels: []label{
					{
						Name:  "foo",
						Value: "bar",
					},
					{
						Name:  "abc",
						Value: "wwe",
					},
				},
				Samples: []sample{
					{
						Value:     123.323,
						Timestamp: 883434,
					},
					{
						Value:     -1884.4329,
						Timestamp: -883432,
					},
				},
			},
		},
	}

	data := marshalWriteRequest(wr)
	result, err := unmarshalWriteRequest(data)
	if err != nil {
		t.Fatalf("unexpected error during  unmarshaling: %s", err)
	}
	if !reflect.DeepEqual(wr, result) {
		t.Fatalf("unexpected result; got %v; want %v", result, wr)
	}
}

func TestMarshalUnmarshalEmptyMessage(t *testing.T) {
	const firstMessageFieldNum = 1
	const secondMessageFieldNum = 1

	m := mp.Get()
	mm := m.MessageMarshaler()
	msg := mm.AppendMessage(firstMessageFieldNum)
	_ = msg.AppendMessage(secondMessageFieldNum)
	data := m.Marshal(nil)
	mp.Put(m)

	var fc FieldContext
	tail, err := fc.NextField(data)
	if err != nil {
		t.Fatalf("cannot obtain the next field in the first message: %s", err)
	}
	if fc.FieldNum != firstMessageFieldNum {
		t.Fatalf("unexpected field num in the first message; got %d; want %d", fc.FieldNum, firstMessageFieldNum)
	}
	if len(tail) != 0 {
		t.Fatalf("unexpected tail left after the first message; len(tail)=%d", len(tail))
	}
	data, ok := fc.MessageData()
	if !ok {
		t.Fatalf("unexpected error when obtaining the first message data")
	}
	tail, err = fc.NextField(data)
	if err != nil {
		t.Fatalf("cannot obtain the next field in the second message: %s", err)
	}
	if fc.FieldNum != secondMessageFieldNum {
		t.Fatalf("unexpected field num in the second message; got %d; want %d", fc.FieldNum, secondMessageFieldNum)
	}
	if len(tail) != 0 {
		t.Fatalf("unexpected tail left after the second message; len(tail)=%d", len(tail))
	}
	data, ok = fc.MessageData()
	if !ok {
		t.Fatalf("unexpected error when obtaining the second message data")
	}
	if len(data) != 0 {
		t.Fatalf("unexpected length for the second message; got %d; want 0", len(data))
	}
}

func TestMarshalUnmarshalEmptyMessageAndNextField(t *testing.T) {
	m := mp.Get()
	mm := m.MessageMarshaler()
	_ = mm.AppendMessage(1)
	mm.AppendString(2, "foo")
	data := m.Marshal(nil)
	mp.Put(m)

	var fc FieldContext

	// read empty message
	tail, err := fc.NextField(data)
	if err != nil {
		t.Fatalf("cannot read next field: %s", err)
	}
	if fc.FieldNum != 1 {
		t.Fatalf("unexpected fieldNum; got %d; want 1", fc.FieldNum)
	}
	data, ok := fc.MessageData()
	if !ok {
		t.Fatalf("cannot get message data")
	}
	if len(data) != 0 {
		t.Fatalf("unexpected non-empty message data: %X", data)
	}

	// read the next string
	tail, err = fc.NextField(tail)
	if err != nil {
		t.Fatalf("cannot read next field: %s", err)
	}
	if fc.FieldNum != 2 {
		t.Fatalf("unexpected fieldNum; got %d; want 2", fc.FieldNum)
	}
	s, ok := fc.String()
	if !ok {
		t.Fatalf("cannot get string")
	}
	if s != "foo" {
		t.Fatalf("unexpected string read; got %q; want %q", s, "foo")
	}

	// make sure there is an empty tail left
	if len(tail) != 0 {
		t.Fatalf("unexpected non-empty tail left: %X", tail)
	}
}

func TestMarshalInterleaved(t *testing.T) {
	m := mp.Get()
	mm := m.MessageMarshaler()
	mm.AppendInt64(1, 123)
	msg := mm.AppendMessage(42)
	mm.AppendDouble(3, 4.2)
	msg.AppendInt64(100, 1234)
	mm.AppendBool(2, true)
	data := m.Marshal(nil)
	mp.Put(m)

	var fc FieldContext
	var err error

	expectNextField := func(expectedFieldNum uint32) {
		t.Helper()
		data, err = fc.NextField(data)
		if err != nil {
			t.Fatalf("cannot get next field: %s", err)
		}
		if fc.FieldNum != expectedFieldNum {
			t.Fatalf("unexpected field num; got %d; want %d", fc.FieldNum, expectedFieldNum)
		}
	}

	expectNextField(1)
	i64, ok := fc.Int64()
	if !ok {
		t.Fatalf("cannot read int64")
	}
	if i64 != 123 {
		t.Fatalf("unexpected int64 value; got %d; want 123", i64)
	}

	expectNextField(42)
	msgData, ok := fc.MessageData()
	if !ok {
		t.Fatalf("cannot read message data")
	}
	msgData, err = fc.NextField(msgData)
	if err != nil {
		t.Fatalf("cannot get next field in embedded message: %s", err)
	}
	if fc.FieldNum != 100 {
		t.Fatalf("unexpected field num; got %d; want 100", fc.FieldNum)
	}
	i64, ok = fc.Int64()
	if !ok {
		t.Fatalf("cannot read int64")
	}
	if i64 != 1234 {
		t.Fatalf("unexpected int64 value; got %d; want 1234", i64)
	}
	if len(msgData) > 0 {
		t.Fatalf("unexpected data left in msgData=%X", msgData)
	}

	expectNextField(3)
	d, ok := fc.Double()
	if !ok {
		t.Fatalf("cannot read double: %s", err)
	}
	if d != 4.2 {
		t.Fatalf("unexpected double value; got %v; want 4.2", d)
	}

	expectNextField(2)
	v, ok := fc.Bool()
	if !ok {
		t.Fatalf("cannot read bool: %s", err)
	}
	if !v {
		t.Fatalf("unexpected bool value; got false; want true")
	}

	if len(data) > 0 {
		t.Fatalf("unexpected data left: %X", data)
	}
}

func TestNextFieldFailure(t *testing.T) {
	f := func(data []byte) {
		t.Helper()
		var fc FieldContext
		_, err := fc.NextField(data)
		if err == nil {
			t.Fatalf("expecting non-nil error")
		}
	}

	// empty message
	f(nil)

	// incorrectly encoded message tag
	f([]byte{0xff})

	// too big fieldNum
	f([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0x7f})

	// incorrectly encoded varint
	f([]byte{1 << 3, 0xff})

	// too small i64
	f([]byte{byte(1<<3 | wireTypeI64), 0xff})

	// too small i32
	f([]byte{byte(1<<3 | wireTypeI32), 0xff})

	// incorrectly encoded len for wireTypeLen
	f([]byte{byte(1<<3 | wireTypeLen), 0xff})

	// too small message for wireTypelen
	f([]byte{byte(1<<3 | wireTypeLen), 0x01})
	f([]byte{0xff, byte(1<<3 | wireTypeLen), 0x01})
	f([]byte{byte(1<<3 | wireTypeLen), 0xff, 0x7f})

	// unknown wireType
	f([]byte{1<<3 | 7})
}

func TestFieldContextWrongWireType(t *testing.T) {
	m := mp.Get()
	mm := m.MessageMarshaler()
	mm.AppendFixed32(42, 8932)
	data := m.Marshal(nil)
	mp.Put(m)

	var fc FieldContext
	tail, err := fc.NextField(data)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if len(tail) != 0 {
		t.Fatalf("unexpected non-empty tail; len(tail)=%d", len(tail))
	}

	i32, ok := fc.Int32()
	if ok {
		t.Fatalf("expecting non-nil error")
	}
	if i32 != 0 {
		t.Fatalf("unexpected i32=%d; want 0", i32)
	}

	i64, ok := fc.Int64()
	if ok {
		t.Fatalf("expecting non-nil error")
	}
	if i64 != 0 {
		t.Fatalf("unexpected i64=%d; want 0", i64)
	}

	u32, ok := fc.Uint32()
	if ok {
		t.Fatalf("expecting non-nil error")
	}
	if u32 != 0 {
		t.Fatalf("unexpected u32=%d; want 0", u32)
	}

	u64, ok := fc.Uint64()
	if ok {
		t.Fatalf("expecting non-nil error")
	}
	if u64 != 0 {
		t.Fatalf("unexpected n=%d; want 0", u64)
	}

	i32, ok = fc.Sint32()
	if ok {
		t.Fatalf("expecting non-nil error")
	}
	if i32 != 0 {
		t.Fatalf("unexpected i32=%d; want 0", i32)
	}

	i64, ok = fc.Sint64()
	if ok {
		t.Fatalf("expecting non-nil error")
	}
	if i64 != 0 {
		t.Fatalf("unexpected n=%d; want 0", i64)
	}

	b, ok := fc.Bool()
	if ok {
		t.Fatalf("expecting non-nil error")
	}
	if b {
		t.Fatalf("unexpected b=true; want false")
	}

	u64, ok = fc.Fixed64()
	if ok {
		t.Fatalf("expecting non-nil error")
	}
	if u64 != 0 {
		t.Fatalf("unexpected u64=%d; want 0", u64)
	}

	i64, ok = fc.Sfixed64()
	if ok {
		t.Fatalf("expecting non-nil error")
	}
	if i64 != 0 {
		t.Fatalf("unexpected i64=%d; want 0", i64)
	}

	f64, ok := fc.Double()
	if ok {
		t.Fatalf("expecting non-nil error")
	}
	if f64 != 0 {
		t.Fatalf("unexpected f64=%v; want 0", f64)
	}

	s, ok := fc.String()
	if ok {
		t.Fatalf("expecting missing string")
	}
	if s != "" {
		t.Fatalf("unexpected s=%q; want %q", s, "")
	}

	bts, ok := fc.Bytes()
	if ok {
		t.Fatalf("expecting missing bytes")
	}
	if bts != nil {
		t.Fatalf("unexpected bts=%q; want nil", bts)
	}

	msgData, ok := fc.MessageData()
	if ok {
		t.Fatalf("expecting missing message data")
	}
	if msgData != nil {
		t.Fatalf("unexpected msgData=%q; want nil", msgData)
	}

	if _, ok := fc.UnpackInt32s(nil); ok {
		t.Fatalf("expecting non-nil error")
	}
	if _, ok := fc.UnpackInt64s(nil); ok {
		t.Fatalf("expecting non-nil error")
	}
	if _, ok := fc.UnpackUint32s(nil); ok {
		t.Fatalf("expecting non-nil error")
	}
	if _, ok := fc.UnpackUint64s(nil); ok {
		t.Fatalf("expecting non-nil error")
	}
	if _, ok := fc.UnpackSint32s(nil); ok {
		t.Fatalf("expecting non-nil error")
	}
	if _, ok := fc.UnpackSint64s(nil); ok {
		t.Fatalf("expecting non-nil error")
	}
	if _, ok := fc.UnpackBools(nil); ok {
		t.Fatalf("expecting non-nil error")
	}
	if _, ok := fc.UnpackFixed64s(nil); ok {
		t.Fatalf("expecting non-nil error")
	}
	if _, ok := fc.UnpackSfixed64s(nil); ok {
		t.Fatalf("expecting non-nil error")
	}
	if _, ok := fc.UnpackDoubles(nil); ok {
		t.Fatalf("expecting non-nil error")
	}

	m = mp.Get()
	mm = m.MessageMarshaler()
	mm.AppendFixed64(42, 8932)
	data = m.Marshal(data[:0])
	mp.Put(m)

	tail, err = fc.NextField(data)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if len(tail) != 0 {
		t.Fatalf("unexpected non-empty tail; len(tail)=%d", len(tail))
	}

	u32, ok = fc.Fixed32()
	if ok {
		t.Fatalf("expecting non-nil error")
	}
	if u32 != 0 {
		t.Fatalf("unexpected u32=%d; want 0", u32)
	}

	i32, ok = fc.Sfixed32()
	if ok {
		t.Fatalf("expecting non-nil error")
	}
	if i32 != 0 {
		t.Fatalf("unexpected i32=%d; want 0", i32)
	}

	f32, ok := fc.Float()
	if ok {
		t.Fatalf("expecting non-nil error")
	}
	if f32 != 0 {
		t.Fatalf("unexpected f32=%v; want 0", f32)
	}

	if _, ok := fc.UnpackFixed32s(nil); ok {
		t.Fatalf("expecting non-nil error")
	}
	if _, ok := fc.UnpackSfixed32s(nil); ok {
		t.Fatalf("expecting non-nil error")
	}
	if _, ok := fc.UnpackFloats(nil); ok {
		t.Fatalf("expecting non-nil error")
	}
}

func TestDecodeIntOverflow(t *testing.T) {
	u64 := uint64(1<<64 - 1)

	m := mp.Get()
	mm := m.MessageMarshaler()
	mm.AppendUint64(1, u64)
	data := m.Marshal(nil)
	mp.Put(m)

	var fc FieldContext
	tail, err := fc.NextField(data)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if len(tail) != 0 {
		t.Fatalf("unexpected non-empty tail; len(tail)=%d", len(tail))
	}

	i32, ok := fc.Int32()
	if ok {
		t.Fatalf("expecting non-nil error")
	}
	if i32 != 0 {
		t.Fatalf("unexpected i32=%d; want 0", i32)
	}

	u32, ok := fc.Uint32()
	if ok {
		t.Fatalf("expecting non-nil error")
	}
	if u32 != 0 {
		t.Fatalf("unexpected u32=%d; want 0", u32)
	}

	i32, ok = fc.Sint32()
	if ok {
		t.Fatalf("expecting non-nil error")
	}
	if i32 != 0 {
		t.Fatalf("unexpected i32=%d; want 0", i32)
	}

	b, ok := fc.Bool()
	if ok {
		t.Fatalf("expecting non-nil error")
	}
	if b {
		t.Fatalf("unexpected b=true; want false")
	}

	if _, ok := fc.UnpackInt32s(nil); ok {
		t.Fatalf("expecting non-nil err")
	}
	if _, ok := fc.UnpackUint32s(nil); ok {
		t.Fatalf("expecting non-nil err")
	}
	if _, ok := fc.UnpackSint32s(nil); ok {
		t.Fatalf("expecting non-nil err")
	}
	if _, ok := fc.UnpackBools(nil); ok {
		t.Fatalf("expecting non-nil err")
	}

	m = mp.Get()
	mm = m.MessageMarshaler()
	mm.AppendUint64s(1, []uint64{u64})
	data = m.Marshal(nil)
	mp.Put(m)

	tail, err = fc.NextField(data)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if len(tail) != 0 {
		t.Fatalf("unexpected non-empty tail; len(tail)=%d", len(tail))
	}

	if _, ok := fc.UnpackInt32s(nil); ok {
		t.Fatalf("expecting non-nil err")
	}
	if _, ok := fc.UnpackUint32s(nil); ok {
		t.Fatalf("expecting non-nil err")
	}
	if _, ok := fc.UnpackSint32s(nil); ok {
		t.Fatalf("expecting non-nil err")
	}
	if _, ok := fc.UnpackBools(nil); ok {
		t.Fatalf("expecting non-nil err")
	}
}

func TestUnmarshalWithoutMessageMarshaler(t *testing.T) {
	m := mp.Get()
	data := m.Marshal(nil)
	mp.Put(m)
	if len(data) > 0 {
		t.Fatalf("unexpected non-empty marshaled data returned: %X", data)
	}
}

func TestDecodeArrayInvalidVarint(t *testing.T) {
	m := mp.Get()
	mm := m.MessageMarshaler()
	mm.AppendUint64s(1, []uint64{0})
	data := m.Marshal(nil)
	mp.Put(m)

	data[len(data)-1] |= 0x80
	var fc FieldContext
	tail, err := fc.NextField(data)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if len(tail) != 0 {
		t.Fatalf("unexpected non-empty tail; len(tail)=%d", len(tail))
	}

	if _, ok := fc.UnpackInt32s(nil); ok {
		t.Fatalf("expecting non-nil err")
	}
	if _, ok := fc.UnpackInt64s(nil); ok {
		t.Fatalf("expecting non-nil err")
	}
	if _, ok := fc.UnpackUint32s(nil); ok {
		t.Fatalf("expecting non-nil err")
	}
	if _, ok := fc.UnpackUint64s(nil); ok {
		t.Fatalf("expecting non-nil err")
	}
	if _, ok := fc.UnpackSint32s(nil); ok {
		t.Fatalf("expecting non-nil err")
	}
	if _, ok := fc.UnpackSint64s(nil); ok {
		t.Fatalf("expecting non-nil err")
	}
	if _, ok := fc.UnpackBools(nil); ok {
		t.Fatalf("expecting non-nil err")
	}
	if _, ok := fc.UnpackFixed64s(nil); ok {
		t.Fatalf("expecting non-nil err")
	}
	if _, ok := fc.UnpackSfixed64s(nil); ok {
		t.Fatalf("expecting non-nil err")
	}
	if _, ok := fc.UnpackDoubles(nil); ok {
		t.Fatalf("expecting non-nil err")
	}
	if _, ok := fc.UnpackFixed32s(nil); ok {
		t.Fatalf("expecting non-nil err")
	}
	if _, ok := fc.UnpackSfixed32s(nil); ok {
		t.Fatalf("expecting non-nil err")
	}
	if _, ok := fc.UnpackFloats(nil); ok {
		t.Fatalf("expecting non-nil err")
	}
}

func TestMarshalWithLen(t *testing.T) {
	m := mp.Get()

	// Verify marshaling of empty message
	data := m.MarshalWithLen(nil)
	if !bytes.Equal(data, []byte{0}) {
		t.Fatalf("unexpected data for empty message; got %X; want 00", data)
	}

	// Verify marshaling of complext message
	m.Reset()
	mm := m.MessageMarshaler()
	mm.AppendString(1, "foo")
	mm.AppendBool(2, true)
	mm.AppendDouble(3, 123.456)
	mm.AppendInt64(4, -1234)
	data = m.MarshalWithLen(nil)

	mp.Put(m)

	msgLen, tail, ok := UnmarshalMessageLen(data)
	if !ok {
		t.Fatalf("cannot read message length")
	}
	data = tail
	if msgLen != len(data) {
		t.Fatalf("unexpected message length read; got %d; want %d", msgLen, len(data))
	}

	var fc FieldContext
	var err error
	data, err = fc.NextField(data)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if fc.FieldNum != 1 {
		t.Fatalf("unexpected field num read; got %d; want 1", fc.FieldNum)
	}
	s, ok := fc.String()
	if !ok {
		t.Fatalf("cannot read string")
	}
	if s != "foo" {
		t.Fatalf("unexpected string read; got %q; want %q", s, "foo")
	}

	data, err = fc.NextField(data)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if fc.FieldNum != 2 {
		t.Fatalf("unexpected field num read; got %d; want 2", fc.FieldNum)
	}
	v, ok := fc.Bool()
	if !ok {
		t.Fatalf("cannot read bool")
	}
	if !v {
		t.Fatalf("unexpected bool read; got false; want true")
	}

	data, err = fc.NextField(data)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if fc.FieldNum != 3 {
		t.Fatalf("unexpected field num read; got %d; want 3", fc.FieldNum)
	}
	d, ok := fc.Double()
	if !ok {
		t.Fatalf("cannot read double")
	}
	if d != 123.456 {
		t.Fatalf("unexpected double value read; got %v; want %v", d, 123.456)
	}

	data, err = fc.NextField(data)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if fc.FieldNum != 4 {
		t.Fatalf("unexpected field num read; got %d; want 4", fc.FieldNum)
	}
	i64, ok := fc.Int64()
	if !ok {
		t.Fatalf("cannot read int64")
	}
	if i64 != -1234 {
		t.Fatalf("unexpected int64 read; got %d; want %d", i64, -1234)
	}

	if len(data) != 0 {
		t.Fatalf("unexpected tail left: %X", data)
	}
}

func TestMarshalLongMessage(t *testing.T) {
	b := make([]byte, 1024)

	m := mp.Get()
	mm := m.MessageMarshaler()
	msg := mm.AppendMessage(1230)
	msg.AppendBytes(234, b)
	data := m.Marshal(nil)
	mp.Put(m)

	var fc FieldContext
	tail, err := fc.NextField(data)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if len(tail) != 0 {
		t.Fatalf("unexpected non-empty tail: %X", tail)
	}
	if fc.FieldNum != 1230 {
		t.Fatalf("unexpected fieldNum; got %d; want 1230", fc.FieldNum)
	}
	msgData, ok := fc.MessageData()
	if !ok {
		t.Fatalf("cannot read message data")
	}
	tail, err = fc.NextField(msgData)
	if err != nil {
		t.Fatalf("cannot read messge field: %s", err)
	}
	if len(tail) != 0 {
		t.Fatalf("unexpected non-empty tail after message field: %X", tail)
	}
	if fc.FieldNum != 234 {
		t.Fatalf("unexpected fieldNum; got %d; want 234", fc.FieldNum)
	}
	b1, ok := fc.Bytes()
	if !ok {
		t.Fatalf("cannot read bytes")
	}
	if !bytes.Equal(b, b1) {
		t.Fatalf("unexpected bytes read\ngot\n%X\nwant\n%X", b1, b)
	}
}

func TestUnmarshalMessageLenFailure(t *testing.T) {
	f := func(data []byte) {
		t.Helper()
		_, _, ok := UnmarshalMessageLen(data)
		if ok {
			t.Fatalf("expecting failure")
		}
	}

	// Empty data
	f(nil)

	// Invalid encoding
	f([]byte{0xff})

	// Too big message length
	f([]byte{0xff, 0xff, 0xff, 0xff, 0x08})
}

func TestUnmarshalMessageLenSuccess(t *testing.T) {
	f := func(data []byte, msgLenExpected int, tailExpected []byte) {
		t.Helper()
		msgLen, tail, ok := UnmarshalMessageLen(data)
		if !ok {
			t.Fatalf("cannot unmarshal message length")
		}
		if msgLen != msgLenExpected {
			t.Fatalf("unexpected msgLen; got %d; want %d", msgLen, msgLenExpected)
		}
		if !bytes.Equal(tail, tailExpected) {
			t.Fatalf("unexpected tail; got %X; want %X", tail, tailExpected)
		}
	}

	f([]byte{0x7f}, 0x7f, nil)
	f([]byte{0x80, 0x00, 0xab}, 0, []byte{0xab})
	f([]byte{0x80, 0x80, 0x00, 0xab}, 0, []byte{0xab})
	f([]byte{0xff, 0x7f, 0x00, 0xab}, (1<<14)-1, []byte{0x00, 0xab})
	f([]byte{0xff, 0xff, 0xff, 0xff, 0x07, 0x00, 0xab}, (1<<31)-1, []byte{0x00, 0xab})
}

var mp MarshalerPool
