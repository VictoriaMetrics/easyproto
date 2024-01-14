package easyproto_test

import (
	"fmt"

	"github.com/VictoriaMetrics/easyproto"
)

// Timeseries is a named time series.
//
// It has the following protobuf v3 definition:
//
//	message timeseries {
//	  string name = 1;
//	  repeated sample samples = 2;
//	}
type Timeseries struct {
	Name    string
	Samples []Sample
}

// Sample represents a sample for the named time series.
//
// It has the following protobuf v3 definition:
//
//	message sample {
//	  double value = 1;
//	  int64 timestamp = 2;
//	}
type Sample struct {
	Value     float64
	Timestamp int64
}

// MarshalProtobuf marshals ts into protobuf message, appends this message to dst and returns the result.
//
// This function doesn't allocate memory on repeated calls.
func (ts *Timeseries) MarshalProtobuf(dst []byte) []byte {
	m := mp.Get()
	ts.marshalProtobuf(m.MessageMarshaler())
	dst = m.Marshal(dst)
	mp.Put(m)
	return dst
}

func (ts *Timeseries) marshalProtobuf(mm *easyproto.MessageMarshaler) {
	mm.AppendString(1, ts.Name)
	for _, s := range ts.Samples {
		s.marshalProtobuf(mm.AppendMessage(2))
	}
}

func (s *Sample) marshalProtobuf(mm *easyproto.MessageMarshaler) {
	mm.AppendDouble(1, s.Value)
	mm.AppendInt64(2, s.Timestamp)
}

var mp easyproto.MarshalerPool

// UnmarshalProtobuf unmarshals ts from protobuf message at src.
func (ts *Timeseries) UnmarshalProtobuf(src []byte) (err error) {
	// Set default Timeseries values
	ts.Name = ""
	ts.Samples = ts.Samples[:0]

	// Parse Timeseries message at src
	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read next field in Timeseries message")
		}
		switch fc.FieldNum {
		case 1:
			name, ok := fc.String()
			if !ok {
				return fmt.Errorf("cannot read Timeseries name")
			}
			// name refers to src. This means that the name changes when src changes.
			// Make a copy with strings.Clone(name) if needed.
			ts.Name = name
		case 2:
			data, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot read Timeseries sample data")
			}
			ts.Samples = append(ts.Samples, Sample{})
			s := &ts.Samples[len(ts.Samples)-1]
			if err := s.UnmarshalProtobuf(data); err != nil {
				return fmt.Errorf("cannot unmarshal sample: %w", err)
			}
		}
	}
	return nil
}

// UnmarshalProtobuf unmarshals s from protobuf message at src.
func (s *Sample) UnmarshalProtobuf(src []byte) (err error) {
	// Set default Sample values
	s.Value = 0
	s.Timestamp = 0

	// Parse Sample message at src
	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read next field in sample")
		}
		switch fc.FieldNum {
		case 1:
			value, ok := fc.Double()
			if !ok {
				return fmt.Errorf("cannot read sample value")
			}
			s.Value = value
		case 2:
			timestamp, ok := fc.Int64()
			if !ok {
				return fmt.Errorf("cannot read sample timestamp")
			}
			s.Timestamp = timestamp
		}
	}
	return nil
}

func Example() {
	ts := &Timeseries{
		Name: "foo",
		Samples: []Sample{
			{
				Value:     123,
				Timestamp: -453426,
			},
			{
				Value:     -234.344,
				Timestamp: 23328434,
			},
		},
	}
	data := ts.MarshalProtobuf(nil)

	var tsNew Timeseries
	if err := tsNew.UnmarshalProtobuf(data); err != nil {
		fmt.Printf("unexpected error: %s\n", err)
		return
	}
	fmt.Printf("name: %s\n", tsNew.Name)
	fmt.Printf("samples:\n")
	for _, s := range tsNew.Samples {
		fmt.Printf("  {value:%v, timestamp:%d}\n", s.Value, s.Timestamp)
	}

	// Output:
	// name: foo
	// samples:
	//   {value:123, timestamp:-453426}
	//   {value:-234.344, timestamp:23328434}
}
