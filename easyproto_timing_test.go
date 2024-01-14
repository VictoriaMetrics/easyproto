package easyproto

import (
	"fmt"
	"sync"
	"testing"
)

func BenchmarkMarshalComplexMessage(b *testing.B) {
	const seriesCount = 1_000

	b.ReportAllocs()
	b.SetBytes(seriesCount)
	b.RunParallel(func(pb *testing.PB) {
		var buf []byte
		for pb.Next() {
			buf = marshalComplexMessage(buf[:0], seriesCount)
		}
	})
}

func BenchmarkUnmarshalComplexMessage(b *testing.B) {
	const seriesCount = 1_000

	data := marshalComplexMessage(nil, seriesCount)

	b.ReportAllocs()
	b.SetBytes(seriesCount)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			unmarshalComplexMessage(data)
		}
	})
}

func marshalComplexMessage(dst []byte, seriesCount int) []byte {
	m := mp.Get()
	mm := m.MessageMarshaler()
	for i := 0; i < seriesCount; i++ {
		mmTimeseries := mm.AppendMessage(1)
		for j := 0; j < 20; j++ {
			mmLabel := mmTimeseries.AppendMessage(1)
			mmLabel.AppendString(1, "instance")
			mmLabel.AppendString(2, "foo-bar-baz-aaa-bbb")
		}
		for j := 0; j < 1; j++ {
			mmSample := mmTimeseries.AppendMessage(2)
			mmSample.AppendDouble(1, float64(j)+1.23)
			mmSample.AppendInt64(2, int64(j)*73287)
		}
	}
	dst = m.Marshal(dst)
	mp.Put(m)
	return dst
}

func unmarshalComplexMessage(src []byte) {
	type label struct {
		name  string
		value string
	}
	type sample struct {
		value     float64
		timestamp int64
	}
	type timeseries struct {
		labels  []label
		samples []sample
	}
	type writeRequest struct {
		tss []timeseries
	}
	resetTimeseries := func(ts *timeseries) {
		labels := ts.labels
		for i := range labels {
			labels[i] = label{}
		}
		ts.labels = labels[:0]

		samples := ts.samples
		for i := range samples {
			samples[i] = sample{}
		}
		ts.samples = samples[:0]
	}
	resetWriteRequest := func(wr *writeRequest) {
		tss := wr.tss
		for i := range tss {
			resetTimeseries(&tss[i])
		}
		wr.tss = tss[:0]
	}
	unmarshalSample := func(smpl *sample, src []byte) error {
		var fc FieldContext
		for len(src) > 0 {
			tail, err := fc.NextField(src)
			if err != nil {
				return fmt.Errorf("cannot get next field: %w", err)
			}
			switch fc.FieldNum {
			case 1:
				value, ok := fc.Double()
				if !ok {
					return fmt.Errorf("cannot unmarshal sample value")
				}
				smpl.value = value
			case 2:
				timestamp, ok := fc.Int64()
				if !ok {
					return fmt.Errorf("cannot unmarshal timestamp")
				}
				smpl.timestamp = timestamp
			}
			src = tail
		}
		return nil
	}
	unmarshalLabel := func(lbl *label, src []byte) error {
		var fc FieldContext
		for len(src) > 0 {
			tail, err := fc.NextField(src)
			if err != nil {
				return fmt.Errorf("cannot get next field: %w", err)
			}
			switch fc.FieldNum {
			case 1:
				name, ok := fc.String()
				if !ok {
					return fmt.Errorf("cannot unmarshal label name")
				}
				lbl.name = name
			case 2:
				value, ok := fc.String()
				if !ok {
					return fmt.Errorf("cannot unmarshal label value")
				}
				lbl.value = value
			}
			src = tail
		}
		return nil
	}
	unmarshalTimeseries := func(ts *timeseries, src []byte) error {
		labels := ts.labels
		samples := ts.samples
		var fc FieldContext
		for len(src) > 0 {
			tail, err := fc.NextField(src)
			if err != nil {
				return fmt.Errorf("cannot get next field: %w", err)
			}
			switch fc.FieldNum {
			case 1:
				data, ok := fc.MessageData()
				if !ok {
					return fmt.Errorf("cannot get label data")
				}
				if len(labels) < cap(labels) {
					labels = labels[:len(labels)+1]
				} else {
					labels = append(labels, label{})
				}
				label := &labels[len(labels)-1]
				if err := unmarshalLabel(label, data); err != nil {
					return fmt.Errorf("cannot unmarshal label: %w", err)
				}
			case 2:
				data, ok := fc.MessageData()
				if !ok {
					return fmt.Errorf("cannot get sample data")
				}
				if len(samples) < cap(samples) {
					samples = samples[:len(samples)+1]
				} else {
					samples = append(samples, sample{})
				}
				sample := &samples[len(samples)-1]
				if err := unmarshalSample(sample, data); err != nil {
					return fmt.Errorf("cannot unmarshal sample: %w", err)
				}
			}
			src = tail
		}
		ts.labels = labels
		ts.samples = samples
		return nil
	}
	unmarshalWriteRequest := func(wr *writeRequest, src []byte) error {
		tss := wr.tss
		var fc FieldContext
		for len(src) > 0 {
			tail, err := fc.NextField(src)
			if err != nil {
				return fmt.Errorf("cannot get next field: %w", err)
			}
			data, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot get message data")
			}
			switch fc.FieldNum {
			case 1:
				if len(tss) < cap(tss) {
					tss = tss[:len(tss)+1]
				} else {
					tss = append(tss, timeseries{})
				}
				ts := &tss[len(tss)-1]
				if err := unmarshalTimeseries(ts, data); err != nil {
					return fmt.Errorf("cannot unmarshal timeseries: %w", err)
				}
			}
			src = tail
		}
		wr.tss = tss
		return nil
	}

	v := writeRequestPool.Get()
	if v == nil {
		v = &writeRequest{}
	}
	wr := v.(*writeRequest)
	if err := unmarshalWriteRequest(wr, src); err != nil {
		panic(fmt.Errorf("cannot unmarshal writeRequest: %w", err))
	}
	resetWriteRequest(wr)
	writeRequestPool.Put(wr)
}

var writeRequestPool sync.Pool
