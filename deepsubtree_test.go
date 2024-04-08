package iavl

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"testing"

	"github.com/chrispappas/golang-generics-set/set"

	"pgregory.net/rapid"

	ics23 "github.com/confio/ics23/go"

	db "github.com/tendermint/tm-db"
)

const (
	cacheSize = math.MaxUint16
)

func bootstrap(f fatalf) *helper {
	h := helper{
		f: f,
	}

	fastStorage := false
	tree, err := NewMutableTreeWithOpts(db.NewMemDB(), cacheSize, nil, !fastStorage)
	h.NoError(err)
	tree.SetTracingEnabled(true)
	dst := NewDeepSubTree(db.NewMemDB(), cacheSize, !fastStorage, 0)
	h.tree = tree
	h.dst = dst
	return &h
}

func FuzzPropertyBased(f *testing.F) {
	// TODO: sanity check that it works
	f.Fuzz(rapid.MakeFuzz(testWithRapid))
}

// go test -run=TestPropertyBased -rapid.checks=10000 -rapid.steps=50
func TestPropertyBased(t *testing.T) {
	/*
	  -rapid.checks int
	    	rapid: number of checks to perform (default 100)
	  -rapid.debug
	    	rapid: debugging output
	  -rapid.debugvis
	    	rapid: debugging visualization
	  -rapid.failfile string
	    	rapid: fail file to use to reproduce test failure
	  -rapid.log
	    	rapid: eager verbose output to stdout (to aid with unrecoverable test failures)
	  -rapid.nofailfile
	    	rapid: do not write fail files on test failures
	  -rapid.seed uint
	    	rapid: PRNG seed to start with (0 to use a random one)
	  -rapid.shrinktime duration
	    	rapid: maximum time to spend on test case minimization (default 30s)
	  -rapid.steps int
	    	rapid: average number of Repeat actions to execute (default 30)
	  -rapid.v
	    	rapid: verbose output
	*/
	rapid.Check(t, testWithRapid)
}

func testWithRapid(t *rapid.T) {
	h := bootstrap(t)

	key := rapid.IntRange(-12, 12)

	keys := make(set.Set[int])

	iterateGen := rapid.Custom[IterateCmd](func(t *rapid.T) IterateCmd {
		return IterateCmd{
			L:         key.Draw(t, "L"),
			R:         key.Draw(t, "R"),
			Ascending: rapid.Bool().Draw(t, "Ascending"),
			StopAfter: rapid.IntRange(0, 100).Draw(t, "StopAfter"),
		}
	})

	_ = iterateGen

	ops := map[string]func(*rapid.T){
		"": func(t *rapid.T) { // Check
			var err error
			hash1, err := h.tree.WorkingHash()
			h.NoError(err)
			hash2, err := h.dst.MutableTree.WorkingHash()
			h.NoError(err)
			if !bytes.Equal(hash1, hash2) {
				t.Fatal("tree and dst roots are not equal", err)
			}
		},
		"get": func(t *rapid.T) {
			err := h.get(key.Draw(t, "k"))
			h.NoError(err)
		},
		"set": func(t *rapid.T) {
			kv := key.Draw(t, "kv")
			keys.Add(kv)
			err := h.set(kv, kv)
			h.NoError(err)
		},
		/*
			TODO: remove seems to cause some problems in conjunction with rebuild-from-scratch
		*/
		"remove": func(t *rapid.T) {
			k := key.Draw(t, "k")
			//if !keys.Has(k) {
			//	t.Logf("noop remove")
			//	return
			//}
			keys.Delete(k)
			// TODO: remove should be useable without the key present
			err := h.remove(k)
			h.NoError(err)
		},
		"has": func(t *rapid.T) {
			err := h.has(key.Draw(t, "k"))
			h.NoError(err)
		},
		"iterate": func(t *rapid.T) {
			cmd := iterateGen.Draw(t, "iterate")
			_, err := h.iterate(cmd.L, cmd.R, cmd.Ascending, cmd.StopAfter)
			h.NoError(err)
		},
		"rebuild from scratch": func(t *rapid.T) {
			/*
				TODO: For some reason this reveals an error if followed up by remove and then get
				I WAS able to replicate it on manavs branch
			*/

			if keys.Len() == 0 {
				return
			}
			rootHash, err := h.tree.WorkingHash()
			h.NoError(err)

			h.dst = NewDeepSubTree(db.NewMemDB(), cacheSize, true, h.tree.version) // TODO: refac to avoid fast storage hardcode
			h.dst.SetInitialRootHash(h.tree.root.hash)

			for _, kv := range keys.Values() {
				proof, err := h.tree.GetMembershipProof(toBz(kv))
				h.NoError(err)
				err = h.dst.AddExistenceProofs([]*ics23.ExistenceProof{
					proof.GetExist(),
				}, rootHash)
				h.NoError(err)
			}

			// TODO(danwt): explain
			for _, kv := range keys.Values() {
				err = h.set(kv, kv)
				h.NoError(err)
			}
		},
	}
	Pick(ops,
		"",
		"get",
		"set",
		"remove",
		"has",
		"iterate",
		//"rebuild from scratch",
	)
	t.Repeat(ops)
}

// Does the Set operation on full IAVL tree first, gets the witness data generated from
// the operation, and uses that witness data to perform the same operation on the Deep Subtree
func (h *helper) set(keyI, valueI int) error {
	key := toBz(keyI)
	value := toBz(valueI)

	_, err := h.tree.Set(key, value)
	if err != nil {
		return err
	}
	_, _, err = h.tree.SaveVersion()
	h.NoError(err)

	h.copyWitnessData(1)

	_, err = h.dst.Set(key, value)
	if err != nil {
		return err
	}
	_, _, err = h.dst.SaveVersion()
	h.NoError(err)

	return nil
}

// Does the Get operation on full IAVL tree first, gets the witness data generated from
// the operation, and uses that witness data to perform the same operation on the Deep Subtree
func (h *helper) get(keyI int) error {
	key := toBz(keyI)

	expect, err := h.tree.Get(key)
	if err != nil {
		return fmt.Errorf("tree get: %w", err)
	}

	h.copyWitnessData(1)

	got, err := h.dst.Get(key)
	if err != nil {
		return fmt.Errorf("dst get: %w", err)
	}
	if !bytes.Equal(expect, got) {
		return fmt.Errorf("get mismatch: key: %x: expect %x: got: %x", key, expect, got)
	}

	return nil
}

// Does the Remove operation on full IAVL tree first, gets the witness data generated from
// the operation, and uses that witness data to peform the same operation on the Deep Subtree
func (h *helper) remove(keyI int) error {
	key := toBz(keyI)

	expectVal, expectWasRemoved, err := h.tree.Remove(key)
	if err != nil {
		return err
	}

	_, _, err = h.tree.SaveVersion()
	h.NoError(err)

	if expectWasRemoved {
		h.copyWitnessData(1)
	}

	gotVal, gotWasRemoved, err := h.dst.Remove(key)
	if err != nil {
		return err
	}

	_, _, err = h.dst.SaveVersion()
	h.NoError(err)

	if expectWasRemoved != gotWasRemoved {
		return fmt.Errorf("remove mismatch: key: %d: expectWasRemoved: %t: gotWasRemoved: %t", keyI, expectWasRemoved, gotWasRemoved)
	}

	if !bytes.Equal(expectVal, gotVal) {
		return fmt.Errorf("remove mismatch: key: %d: expectVal: %x: gotVal: %x", keyI, expectVal, gotVal)
	}

	return nil
}

// Does the Has operation on full IAVL tree first, gets the witness data generated from
// the operation, and uses that witness data to perform the same operation on the Deep Subtree
func (h *helper) has(keyI int) error {
	key := toBz(keyI)

	expect, err := h.tree.Has(key)
	if err != nil {
		return fmt.Errorf("tree has: %w", err)
	}

	h.copyWitnessData(1)

	got, err := h.dst.Has(key)
	if err != nil {
		return fmt.Errorf("dst has: key: %d: %w", keyI, err)
	}

	if got != expect {
		return fmt.Errorf("has mismatch: key: %d: expect %t: got: %t", keyI, expect, got)
	}

	return nil
}

// Does the Iterate operation  TODO:..
// if stopAfter is positive, the iterations will be capped
// returns the number of tree items visited
func (h *helper) iterate(startI, endI int, ascending bool, stopAfter int) (nVisited int, err error) {
	/*
		TODO: do I need to explicitly test Domain,Valid,Next,Key,Value,Error,Close? In varying dynamic (non usual) ways?
	*/

	start := toBz(startI)
	end := toBz(endI)

	l := len(h.tree.witnessData)

	// Set key-value pair in IAVL tree
	itTree, err := h.tree.Iterator(start, end, ascending)
	if err != nil {
		return
	}

	type result struct {
		start []byte
		end   []byte
		key   []byte
		value []byte
		err   error
	}

	var results []result

	// TODO: do I need an operation for New()?

	i := 0
	for {
		if stopAfter != 0 && stopAfter <= i {
			break
		}
		valid := itTree.Valid()
		h.NoErrorIterators() // check valid
		if !valid {
			break
		}

		i++
		s, e := itTree.Domain()
		k := itTree.Key()
		v := itTree.Value()
		err := itTree.Error()

		results = append(results, result{
			start: s,
			end:   e,
			key:   k,
			value: v,
			err:   err,
		})
		itTree.Next()
		h.NoErrorIterators() // check next
	}

	itTreeCloseErr := itTree.Close()

	h.copyWitnessData(len(h.tree.witnessData) - l)

	// Set key-value pair in IAVL tree
	itDST, err := h.dst.Iterator(start, end, ascending)
	if err != nil {
		err = fmt.Errorf("dst iterator init: %w", err)
		return
	}

	i = 0

	for {
		if stopAfter != 0 && stopAfter <= i {
			break
		}
		valid := itDST.Valid()
		h.NoErrorIterators() // check valid
		if !valid {
			break
		}
		s, e := itDST.Domain()
		k := itDST.Key()
		v := itDST.Value()
		err := itDST.Error()
		r := results[i]
		i++
		if !bytes.Equal(r.start, s) || !bytes.Equal(r.end, e) {
			return 0, fmt.Errorf("start/end mismatch")
		}
		if !bytes.Equal(r.key, k) {
			return 0, fmt.Errorf("key mismatch: expect: %x: got: %x", r.key, k)
		}
		if !bytes.Equal(r.value, v) {
			return 0, fmt.Errorf("value mismatch: expect: %x: got: %x", r.value, v)
		}
		if !errors.Is(r.err, err) || !errors.Is(err, r.err) { // TODO: makes sense?
			return 0, fmt.Errorf("error mismatch")
		}
		itDST.Next()
		h.NoErrorIterators() // check next
	}

	if i != len(results) {
		return 0, fmt.Errorf("valid cnt mismatch: expect %d: got %d", len(results), i)
	}

	itDSTCloseErr := itDST.Close()

	if !errors.Is(itTreeCloseErr, itDSTCloseErr) || !errors.Is(itDSTCloseErr, itTreeCloseErr) { // TODO: makes sense?
		return 0, fmt.Errorf("close error mismatch")
	}

	return len(results), nil
}

type fatalf interface {
	Fatalf(format string, args ...any)
}

type helper struct {
	f    fatalf
	tree *MutableTree
	dst  *DeepSubTree
}

func (h *helper) copyWitnessData(n int) {
	// TODO: clone necessary?
	h.dst.SetWitnessData(slices.Clone(h.tree.witnessData[len(h.tree.witnessData)-n:]))
}

func (h *helper) NoError(err error) {
	if err != nil {
		h.f.Fatalf("%v", err)
	}
}

func (h *helper) NoErrorIterators() {
	errs := slices.Clone(h.tree.iterErrors)
	errs = append(errs, h.dst.iterErrors...)
	if 0 < len(errs) {
		h.f.Fatalf("%v", errs)
	}
}

func toBz(i int) []byte {
	return []byte(strconv.Itoa(i))
}

type IterateCmd struct {
	L, R      int
	Ascending bool
	StopAfter int
}
