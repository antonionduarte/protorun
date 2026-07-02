package protorun

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"sort"
	"sync"
)

// WireCodec is the reflective default Codec[M]: it encodes any M whose
// fields are made of the supported kinds without a hand-written codec.
// Where BinaryCodec only handles fixed-size structs, WireCodec adds
// strings, byte slices, slices, maps, nested structs, and pointers to
// structs, so most real message types can just use it (via Handle).
//
// M must be a pointer to a struct, same as BinaryCodec. The supported
// field kinds are: bool, the sized integers (int8/16/32/64,
// uint8/16/32/64), float32/64, string, []byte, slices of any supported
// kind, arrays of fixed-size kinds, maps of supported key/value kinds,
// nested structs (by value), and pointers to structs. Unexported fields
// are skipped; a `wire:"-"` struct tag skips a field too.
//
// Rejected at plan-compile time (surfaced by Marshal/Unmarshal, and
// eagerly by Handle): platform-sized int/uint (not portable on the
// wire), interface, chan, func, complex, and unsafe.Pointer.
//
// The wire format is normative in docs/wire-format.md ("WireCodec
// payload format"). Schema evolution is explicitly not a goal: fields
// carry no tags or numbers on the wire, so renaming or reordering them
// is a wire break — consistent with the framework's WireName stance.
//
// A per-type encode/decode plan is compiled once via reflection and
// cached (the same trick wireIDCache uses), so the steady-state cost is
// a plan lookup rather than per-field reflect.TypeOf.
type WireCodec[M Message] struct{}

func (WireCodec[M]) Marshal(m M) ([]byte, error) {
	rv := reflect.ValueOf(m)
	if rv.Kind() != reflect.Pointer {
		return nil, fmt.Errorf("WireCodec[M]: M must be a pointer to struct, got %T", m)
	}
	if rv.IsNil() {
		return nil, fmt.Errorf("WireCodec.Marshal: nil %T", m)
	}
	elem := rv.Type().Elem()
	if elem.Kind() != reflect.Struct {
		return nil, fmt.Errorf("WireCodec[M]: M must point to a struct, got *%s", elem.Kind())
	}
	plan, err := wirePlanFor(elem)
	if err != nil {
		return nil, err
	}
	// enc only reads rv, so Marshal never mutates m.
	return plan.enc(nil, rv.Elem()), nil
}

func (WireCodec[M]) Unmarshal(data []byte) (M, error) {
	var zero M
	t := reflect.TypeOf(zero)
	if t == nil || t.Kind() != reflect.Pointer || t.Elem().Kind() != reflect.Struct {
		return zero, fmt.Errorf("WireCodec[M]: M must be a pointer to struct, got %T", zero)
	}
	plan, err := wirePlanFor(t.Elem())
	if err != nil {
		return zero, err
	}
	ptr := reflect.New(t.Elem())
	rest, err := plan.dec(data, ptr.Elem())
	if err != nil {
		return zero, err
	}
	if len(rest) != 0 {
		return zero, fmt.Errorf("WireCodec.Unmarshal: %d trailing byte(s)", len(rest))
	}
	return ptr.Interface().(M), nil
}

// --- Plan compiler ---
//
// A valueCodec (un)marshals a single reflect.Value of one type. A struct
// plan is a valueCodec whose enc/dec walk field valueCodecs in
// declaration order. Plans are compiled once per struct type and cached
// in wirePlans; the compile itself recurses over nested kinds.

// valueCodec encodes/decodes one value in place. dec returns the
// remaining bytes so callers can chain field by field. min is the
// smallest number of bytes a value of this type can ever occupy on the
// wire; it bounds length-prefixed decodes so fuzzed lengths can't drive
// an unbounded allocation or loop.
type valueCodec struct {
	enc func(dst []byte, v reflect.Value) []byte
	dec func(data []byte, v reflect.Value) ([]byte, error)
	min int
}

// wirePlanResult caches either a compiled plan or the compile error for
// a type, so a rejected type isn't re-analysed on every Marshal.
type wirePlanResult struct {
	codec valueCodec
	err   error
}

var wirePlans sync.Map // map[reflect.Type]*wirePlanResult

// wirePlanFor returns the cached plan for struct type t, compiling it on
// first use.
func wirePlanFor(t reflect.Type) (valueCodec, error) {
	if r, ok := wirePlans.Load(t); ok {
		res := r.(*wirePlanResult)
		return res.codec, res.err
	}
	c := &wireCompiler{inProgress: make(map[reflect.Type]*valueCodec)}
	codec, err := c.compile(t)
	res := &wirePlanResult{codec: codec, err: err}
	actual, _ := wirePlans.LoadOrStore(t, res)
	stored := actual.(*wirePlanResult)
	return stored.codec, stored.err
}

// wireCompiler carries the in-progress set that breaks recursive-type
// cycles: a struct reached through a pointer/slice/map back-reference
// resolves to a forwarding codec that reads the placeholder filled in
// once the outer compile completes.
type wireCompiler struct {
	inProgress map[reflect.Type]*valueCodec
}

func (c *wireCompiler) compile(t reflect.Type) (valueCodec, error) {
	if p, ok := c.inProgress[t]; ok {
		return forwardCodec(p), nil
	}
	if vc, ok := scalarCodec(t.Kind()); ok {
		return vc, nil
	}
	switch t.Kind() {
	case reflect.String:
		return stringCodec, nil
	case reflect.Slice:
		return c.compileSlice(t)
	case reflect.Array:
		return c.compileArray(t)
	case reflect.Map:
		return c.compileMap(t)
	case reflect.Struct:
		return c.compileStruct(t)
	case reflect.Pointer:
		return c.compilePointer(t)
	default:
		return valueCodec{}, rejectKind(t)
	}
}

// forwardCodec resolves through a placeholder that is filled in once the
// referenced type finishes compiling. Used only to break compile-time
// recursion; the extra indirection is one pointer load at run time.
func forwardCodec(p *valueCodec) valueCodec {
	return valueCodec{
		enc: func(dst []byte, v reflect.Value) []byte { return p.enc(dst, v) },
		dec: func(data []byte, v reflect.Value) ([]byte, error) { return p.dec(data, v) },
		min: 1, // a back-reference is always behind a pointer/slice/map prefix
	}
}

func (c *wireCompiler) compileStruct(t reflect.Type) (valueCodec, error) {
	placeholder := &valueCodec{}
	c.inProgress[t] = placeholder
	defer delete(c.inProgress, t)

	type fieldEntry struct {
		index int
		codec valueCodec
	}
	var fields []fieldEntry
	minTotal := 0
	for i := range t.NumField() {
		f := t.Field(i)
		if f.PkgPath != "" { // unexported: not on the wire, skipped
			continue
		}
		if f.Tag.Get("wire") == "-" {
			continue
		}
		fc, err := c.compile(f.Type)
		if err != nil {
			return valueCodec{}, fmt.Errorf("field %s: %w", f.Name, err)
		}
		fields = append(fields, fieldEntry{index: i, codec: fc})
		minTotal += fc.min
	}
	vc := valueCodec{
		enc: func(dst []byte, v reflect.Value) []byte {
			for _, fe := range fields {
				dst = fe.codec.enc(dst, v.Field(fe.index))
			}
			return dst
		},
		dec: func(data []byte, v reflect.Value) ([]byte, error) {
			var err error
			for _, fe := range fields {
				if data, err = fe.codec.dec(data, v.Field(fe.index)); err != nil {
					return nil, err
				}
			}
			return data, nil
		},
		min: minTotal,
	}
	*placeholder = vc
	return vc, nil
}

func (c *wireCompiler) compilePointer(t reflect.Type) (valueCodec, error) {
	elem := t.Elem()
	if elem.Kind() != reflect.Struct {
		return valueCodec{}, fmt.Errorf(
			"pointer to %s: only pointer-to-struct is supported", elem.Kind())
	}
	ec, err := c.compile(elem)
	if err != nil {
		return valueCodec{}, err
	}
	return valueCodec{
		enc: func(dst []byte, v reflect.Value) []byte {
			if v.IsNil() {
				return append(dst, 0)
			}
			return ec.enc(append(dst, 1), v.Elem())
		},
		dec: func(data []byte, v reflect.Value) ([]byte, error) {
			if len(data) < 1 {
				return nil, fmt.Errorf("WireCodec: truncated pointer presence byte")
			}
			present := data[0]
			data = data[1:]
			switch present {
			case 0:
				v.Set(reflect.Zero(t))
				return data, nil
			case 1:
				np := reflect.New(elem)
				rest, err := ec.dec(data, np.Elem())
				if err != nil {
					return nil, err
				}
				v.Set(np)
				return rest, nil
			default:
				return nil, fmt.Errorf("WireCodec: invalid pointer presence byte %d", present)
			}
		},
		min: 1,
	}, nil
}

func (c *wireCompiler) compileSlice(t reflect.Type) (valueCodec, error) {
	if t.Elem().Kind() == reflect.Uint8 {
		return bytesCodec, nil // []byte fast path
	}
	elemCodec, err := c.compile(t.Elem())
	if err != nil {
		return valueCodec{}, err
	}
	return valueCodec{
		enc: func(dst []byte, v reflect.Value) []byte {
			n := v.Len()
			dst = binary.AppendUvarint(dst, uint64(n))
			for i := range n {
				dst = elemCodec.enc(dst, v.Index(i))
			}
			return dst
		},
		dec: func(data []byte, v reflect.Value) ([]byte, error) {
			n, rest, err := readCount(data, elemCodec.min)
			if err != nil {
				return nil, err
			}
			slice := reflect.MakeSlice(t, 0, capHint(n))
			for range n {
				ev := reflect.New(t.Elem()).Elem()
				if rest, err = elemCodec.dec(rest, ev); err != nil {
					return nil, err
				}
				slice = reflect.Append(slice, ev)
			}
			v.Set(slice)
			return rest, nil
		},
		min: 1,
	}, nil
}

func (c *wireCompiler) compileArray(t reflect.Type) (valueCodec, error) {
	if _, ok := scalarCodec(t.Elem().Kind()); !ok {
		return valueCodec{}, fmt.Errorf(
			"array of %s: only arrays of fixed-size kinds are supported", t.Elem().Kind())
	}
	elemCodec, _ := scalarCodec(t.Elem().Kind())
	n := t.Len()
	return valueCodec{
		// No length prefix: the count is fixed by the array type.
		enc: func(dst []byte, v reflect.Value) []byte {
			for i := range n {
				dst = elemCodec.enc(dst, v.Index(i))
			}
			return dst
		},
		dec: func(data []byte, v reflect.Value) ([]byte, error) {
			var err error
			for i := range n {
				if data, err = elemCodec.dec(data, v.Index(i)); err != nil {
					return nil, err
				}
			}
			return data, nil
		},
		min: n * elemCodec.min,
	}, nil
}

func (c *wireCompiler) compileMap(t reflect.Type) (valueCodec, error) {
	keyCodec, err := c.compile(t.Key())
	if err != nil {
		return valueCodec{}, fmt.Errorf("map key: %w", err)
	}
	valCodec, err := c.compile(t.Elem())
	if err != nil {
		return valueCodec{}, fmt.Errorf("map value: %w", err)
	}
	entryMin := keyCodec.min + valCodec.min
	return valueCodec{
		enc: func(dst []byte, v reflect.Value) []byte {
			return encodeMap(dst, v, keyCodec, valCodec)
		},
		dec: func(data []byte, v reflect.Value) ([]byte, error) {
			return decodeMap(data, v, t, keyCodec, valCodec, entryMin)
		},
		min: 1,
	}, nil
}

// encodeMap writes the map with keys sorted by their encoded byte order.
// Sorting makes Marshal deterministic: a stable payload is what lets
// retransmission and dedup key off the payload hash. See docs/wire-
// format.md.
func encodeMap(dst []byte, v reflect.Value, keyCodec, valCodec valueCodec) []byte {
	type entry struct{ key, val []byte }
	entries := make([]entry, 0, v.Len())
	iter := v.MapRange()
	for iter.Next() {
		entries = append(entries, entry{
			key: keyCodec.enc(nil, iter.Key()),
			val: valCodec.enc(nil, iter.Value()),
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].key, entries[j].key) < 0
	})
	dst = binary.AppendUvarint(dst, uint64(len(entries)))
	for _, e := range entries {
		dst = append(dst, e.key...)
		dst = append(dst, e.val...)
	}
	return dst
}

func decodeMap(data []byte, v reflect.Value, t reflect.Type,
	keyCodec, valCodec valueCodec, entryMin int,
) ([]byte, error) {
	n, rest, err := readCount(data, entryMin)
	if err != nil {
		return nil, err
	}
	m := reflect.MakeMapWithSize(t, capHint(n))
	for range n {
		key := reflect.New(t.Key()).Elem()
		if rest, err = keyCodec.dec(rest, key); err != nil {
			return nil, err
		}
		val := reflect.New(t.Elem()).Elem()
		if rest, err = valCodec.dec(rest, val); err != nil {
			return nil, err
		}
		m.SetMapIndex(key, val)
	}
	v.Set(m)
	return rest, nil
}

// --- Length-prefix helpers ---

// maxZeroSizeCount caps the element count of a slice/map whose entries
// can encode to zero bytes (e.g. []struct{}), so a fuzzed length prefix
// can't drive an unbounded decode loop.
const maxZeroSizeCount = 1 << 20

// readCount reads a uvarint element count and rejects it when the
// remaining bytes cannot possibly hold that many entries. elemMin is the
// smallest byte size one entry can occupy; when it is 0 the count is
// bounded by maxZeroSizeCount instead (there is no per-entry floor to
// check against).
func readCount(data []byte, elemMin int) (int, []byte, error) {
	n, w := binary.Uvarint(data)
	if w <= 0 {
		return 0, nil, fmt.Errorf("WireCodec: invalid length prefix")
	}
	rest := data[w:]
	switch {
	case elemMin > 0:
		if n > uint64(len(rest))/uint64(elemMin) {
			return 0, nil, fmt.Errorf("WireCodec: length %d exceeds remaining %d bytes", n, len(rest))
		}
	case n > maxZeroSizeCount:
		return 0, nil, fmt.Errorf("WireCodec: length %d exceeds zero-size cap", n)
	}
	return int(n), rest, nil
}

// capHint bounds the pre-allocation for a decoded slice/map so a large
// (but individually validated) count still grows incrementally rather
// than reserving everything up front.
func capHint(n int) int {
	const maxPrealloc = 4096
	return min(n, maxPrealloc)
}

// stringCodec and bytesCodec share the [uvarint len || raw bytes] shape.
var (
	stringCodec = valueCodec{
		enc: func(dst []byte, v reflect.Value) []byte {
			s := v.String()
			return append(binary.AppendUvarint(dst, uint64(len(s))), s...)
		},
		dec: func(data []byte, v reflect.Value) ([]byte, error) {
			b, rest, err := readRaw(data)
			if err != nil {
				return nil, err
			}
			v.SetString(string(b))
			return rest, nil
		},
		min: 1,
	}

	bytesCodec = valueCodec{
		enc: func(dst []byte, v reflect.Value) []byte {
			b := v.Bytes()
			return append(binary.AppendUvarint(dst, uint64(len(b))), b...)
		},
		dec: func(data []byte, v reflect.Value) ([]byte, error) {
			b, rest, err := readRaw(data)
			if err != nil {
				return nil, err
			}
			if b == nil {
				v.SetBytes(nil)
			} else {
				out := make([]byte, len(b))
				copy(out, b)
				v.SetBytes(out)
			}
			return rest, nil
		},
		min: 1,
	}
)

// readRaw reads a uvarint-prefixed byte run, returning it as a subslice
// of data (the caller copies when it must outlive data). A zero length
// yields a nil slice, so empty and nil round-trip to the same value.
func readRaw(data []byte) ([]byte, []byte, error) {
	n, w := binary.Uvarint(data)
	if w <= 0 {
		return nil, nil, fmt.Errorf("WireCodec: invalid length prefix")
	}
	rest := data[w:]
	if n > uint64(len(rest)) {
		return nil, nil, fmt.Errorf("WireCodec: length %d exceeds remaining %d bytes", n, len(rest))
	}
	if n == 0 {
		return nil, rest, nil
	}
	return rest[:n], rest[n:], nil
}

// --- Scalars ---

// scalarCodec returns the fixed-size little-endian codec for a scalar
// kind, or ok=false for any other kind (including the rejected platform-
// sized int/uint). Fixed scalars match the rest of the application body
// format: little-endian, no length prefix.
func scalarCodec(k reflect.Kind) (valueCodec, bool) {
	vc, ok := scalarCodecs[k]
	return vc, ok
}

var scalarCodecs = map[reflect.Kind]valueCodec{
	reflect.Bool: fixedCodec(1,
		func(dst []byte, v reflect.Value) []byte {
			if v.Bool() {
				return append(dst, 1)
			}
			return append(dst, 0)
		},
		func(b []byte, v reflect.Value) { v.SetBool(b[0] != 0) }),

	reflect.Int8: fixedCodec(1,
		func(dst []byte, v reflect.Value) []byte { return append(dst, byte(v.Int())) },
		func(b []byte, v reflect.Value) { v.SetInt(int64(int8(b[0]))) }),
	reflect.Uint8: fixedCodec(1,
		func(dst []byte, v reflect.Value) []byte { return append(dst, byte(v.Uint())) },
		func(b []byte, v reflect.Value) { v.SetUint(uint64(b[0])) }),

	reflect.Int16: fixedCodec(2,
		func(dst []byte, v reflect.Value) []byte { return binary.LittleEndian.AppendUint16(dst, uint16(v.Int())) },
		func(b []byte, v reflect.Value) { v.SetInt(int64(int16(binary.LittleEndian.Uint16(b)))) }),
	reflect.Uint16: fixedCodec(2,
		func(dst []byte, v reflect.Value) []byte { return binary.LittleEndian.AppendUint16(dst, uint16(v.Uint())) },
		func(b []byte, v reflect.Value) { v.SetUint(uint64(binary.LittleEndian.Uint16(b))) }),

	reflect.Int32: fixedCodec(4,
		func(dst []byte, v reflect.Value) []byte { return binary.LittleEndian.AppendUint32(dst, uint32(v.Int())) },
		func(b []byte, v reflect.Value) { v.SetInt(int64(int32(binary.LittleEndian.Uint32(b)))) }),
	reflect.Uint32: fixedCodec(4,
		func(dst []byte, v reflect.Value) []byte { return binary.LittleEndian.AppendUint32(dst, uint32(v.Uint())) },
		func(b []byte, v reflect.Value) { v.SetUint(uint64(binary.LittleEndian.Uint32(b))) }),

	reflect.Int64: fixedCodec(8,
		func(dst []byte, v reflect.Value) []byte { return binary.LittleEndian.AppendUint64(dst, uint64(v.Int())) },
		func(b []byte, v reflect.Value) { v.SetInt(int64(binary.LittleEndian.Uint64(b))) }),
	reflect.Uint64: fixedCodec(8,
		func(dst []byte, v reflect.Value) []byte { return binary.LittleEndian.AppendUint64(dst, v.Uint()) },
		func(b []byte, v reflect.Value) { v.SetUint(binary.LittleEndian.Uint64(b)) }),

	reflect.Float32: fixedCodec(4,
		func(dst []byte, v reflect.Value) []byte {
			return binary.LittleEndian.AppendUint32(dst, math.Float32bits(float32(v.Float())))
		},
		func(b []byte, v reflect.Value) { v.SetFloat(float64(math.Float32frombits(binary.LittleEndian.Uint32(b)))) }),
	reflect.Float64: fixedCodec(8,
		func(dst []byte, v reflect.Value) []byte {
			return binary.LittleEndian.AppendUint64(dst, math.Float64bits(v.Float()))
		},
		func(b []byte, v reflect.Value) { v.SetFloat(math.Float64frombits(binary.LittleEndian.Uint64(b))) }),
}

// fixedCodec builds a valueCodec for a fixed-size scalar: put writes the
// bytes, get reads exactly size bytes back. dec bounds-checks so a
// truncated tail is an error, not a panic.
func fixedCodec(size int,
	put func(dst []byte, v reflect.Value) []byte,
	get func(b []byte, v reflect.Value),
) valueCodec {
	return valueCodec{
		enc: put,
		dec: func(data []byte, v reflect.Value) ([]byte, error) {
			if len(data) < size {
				return nil, fmt.Errorf("WireCodec: truncated %d-byte scalar", size)
			}
			get(data[:size], v)
			return data[size:], nil
		},
		min: size,
	}
}

// rejectKind returns the plan-compile error for an unsupported kind,
// steering the platform-sized integers toward a portable choice.
func rejectKind(t reflect.Type) error {
	switch t.Kind() {
	case reflect.Int, reflect.Uint, reflect.Uintptr:
		return fmt.Errorf(
			"%s is not portable on the wire: use a sized type (int32/int64/uint32/uint64)", t.Kind())
	case reflect.Complex64, reflect.Complex128:
		return fmt.Errorf("complex numbers are not supported")
	default:
		return fmt.Errorf("unsupported kind %s", t.Kind())
	}
}
