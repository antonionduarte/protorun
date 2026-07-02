// Package wire provides small length-prefixed read/write helpers for the
// protocol runtime's wire format. Variable-length values (strings, byte
// slices) are length-prefixed so they can be embedded inside a fixed-
// length message envelope without ambiguity. Host serialization lives in
// `transport` (alongside the Host type itself) and is built on top
// of these primitives.
package wire

import (
	"encoding/binary"
	"fmt"
	"io"
)

// WriteUint16 writes v as a little-endian uint16.
func WriteUint16(w io.Writer, v uint16) error {
	return binary.Write(w, binary.LittleEndian, v)
}

// ReadUint16 reads a little-endian uint16.
func ReadUint16(r io.Reader) (uint16, error) {
	var v uint16
	if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
		return 0, err
	}
	return v, nil
}

// WriteUint32 writes v as a little-endian uint32.
func WriteUint32(w io.Writer, v uint32) error {
	return binary.Write(w, binary.LittleEndian, v)
}

// ReadUint32 reads a little-endian uint32.
func ReadUint32(r io.Reader) (uint32, error) {
	var v uint32
	if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
		return 0, err
	}
	return v, nil
}

// WriteUint64 writes v as a little-endian uint64.
func WriteUint64(w io.Writer, v uint64) error {
	return binary.Write(w, binary.LittleEndian, v)
}

// ReadUint64 reads a little-endian uint64.
func ReadUint64(r io.Reader) (uint64, error) {
	var v uint64
	if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
		return 0, err
	}
	return v, nil
}

// WriteBytes writes a length-prefixed byte slice: [uint32 LE len][bytes].
// Empty slices are written as a 0-length prefix.
func WriteBytes(w io.Writer, b []byte) error {
	if err := WriteUint32(w, uint32(len(b))); err != nil {
		return err
	}
	if len(b) == 0 {
		return nil
	}
	_, err := w.Write(b)
	return err
}

// ReadBytes reads a length-prefixed byte slice written by WriteBytes.
func ReadBytes(r io.Reader) ([]byte, error) {
	n, err := ReadUint32(r)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("ReadBytes: %w", err)
	}
	return buf, nil
}

// WriteString writes a length-prefixed UTF-8 string.
func WriteString(w io.Writer, s string) error { return WriteBytes(w, []byte(s)) }

// ReadString reads a length-prefixed UTF-8 string written by WriteString.
func ReadString(r io.Reader) (string, error) {
	b, err := ReadBytes(r)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
