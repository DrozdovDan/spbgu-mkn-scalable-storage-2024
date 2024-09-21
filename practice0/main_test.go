package main

import (
	"testing"
)

func TestFiller(t *testing.T) {
	b := [100]byte{}
	zero := byte('0')
	one := byte('1')
	filler(b[:], zero, one)
	hasZero := false
	hasOne := false
	for _, v := range b {
		if v == zero {
			hasZero = true
		}
		if v == one {
			hasOne = true
		}
	}

	if !hasZero || !hasOne {
		t.Error("Fail")
	}
}
