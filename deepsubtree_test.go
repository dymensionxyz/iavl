package iavl

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"testing"

	"pgregory.net/rapid"

	"github.com/chrispappas/golang-generics-set/set"
	ics23 "github.com/confio/ics23/go"

	"github.com/stretchr/testify/require"
	db "github.com/tendermint/tm-db"
)

type op int

const (
	Set op = iota
	Remove
	Get
	Has
	Iterate
)

const (
	cacheSize = math.MaxUint16
)

// Returns whether given trees have equal hashes
func haveEqualRoots(tree1 *MutableTree, tree2 *MutableTree) (bool, error) {
	rootHash, err := tree1.WorkingHash()
	if err != nil {
		return false, err
	}

	treeWorkingHash, err := tree2.WorkingHash()
	if err != nil {
		return false, err
	}

	// Check root hashes are equal
	return bytes.Equal(rootHash, treeWorkingHash), nil
}

// Tests creating an empty Deep Subtree
func TestEmptyDeepSubtree(t *testing.T) {
	require := require.New(t)
	getTree := func() *MutableTree {
		tree, err := getTestTree(0)
		require.NoError(err)
		return tree
	}

	tree := getTree()

	dst := NewDeepSubTree(db.NewMemDB(), 100, false, 0)

	areEqual, err := haveEqualRoots(dst.MutableTree, tree)
	require.NoError(err)
	require.True(areEqual)
}

// Tests creating deep subtree step-by-step to match a regular tree, and checks if roots are equal
func TestDeepSubTreeCreateFromProofs(t *testing.T) {
	require := require.New(t)
	getTree := func() *MutableTree {
		tree, err := getTestTree(5)
		require.NoError(err)

		_, err = tree.Set([]byte("e"), []byte{5})
		require.NoError(err)
		_, err = tree.Set([]byte("d"), []byte{4})
		require.NoError(err)
		_, err = tree.Set([]byte("c"), []byte{3})
		require.NoError(err)
		_, err = tree.Set([]byte("b"), []byte{2})
		require.NoError(err)
		_, err = tree.Set([]byte("a"), []byte{1})
		require.NoError(err)

		_, _, err = tree.SaveVersion()
		require.NoError(err)
		return tree
	}

	tree := getTree()
	rootHash, err := tree.WorkingHash()
	require.NoError(err)

	dst := NewDeepSubTree(db.NewMemDB(), 100, false, tree.version)
	require.NoError(err)
	dst.SetInitialRootHash(tree.root.hash)

	// insert key/value pairs in tree
	allkeys := [][]byte{
		[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"),
	}

	// Put all keys inside the tree one by one
	for _, key := range allkeys {
		ics23proof, err := tree.GetMembershipProof(key)
		require.NoError(err)
		err = dst.AddExistenceProofs([]*ics23.ExistenceProof{
			ics23proof.GetExist(),
		}, rootHash)
		require.NoError(err)
	}

	areEqual, err := haveEqualRoots(dst.MutableTree, tree)
	require.NoError(err)
	require.True(areEqual)
}

// Performs the Set operation on full IAVL tree first, gets the witness data generated from
// the operation, and uses that witness data to perform the same operation on the Deep Subtree
func (h *helper) set(key, value int) error {
	// Set key-value pair in IAVL tree
	_, err := tree.Set(key, value)
	if err != nil {
		return err
	}
	_, _, err = tree.SaveVersion()
	h.NoError(err)
	witness := tree.witnessData[len(tree.witnessData)-1]
	dst.SetWitnessData([]WitnessData{witness})

	// Set key-value pair in DST
	_, err = dst.Set(key, value)
	if err != nil {
		return err
	}
	_, _, err = dst.SaveVersion()
	h.NoError(err)

	areEqual, err := haveEqualRoots(dst.MutableTree, tree)
	if err != nil {
		return err
	}
	if !areEqual {
		return errors.New("iavl and deep subtree roots are not equal")
	}
	return nil
}

// Performs the Remove operation on full IAVL tree first, gets the witness data generated from
// the operation, and uses that witness data to peform the same operation on the Deep Subtree
func (h *helper) remove(key []byte) error {
	if key == nil {
		return nil
	}
	tree, dst := h.tree, h.dst

	// Set key-value pair in IAVL tree
	_, _, err := tree.Remove(key)
	if err != nil {
		return err
	}
	_, _, err = tree.SaveVersion()
	h.NoError(err)

	witness := tree.witnessData[len(tree.witnessData)-1]
	dst.SetWitnessData([]WitnessData{witness})

	_, removed, err := dst.Remove(key)
	if err != nil {
		return err
	}
	if !removed {
		return fmt.Errorf("remove key from dst: %s", string(key))
	}
	_, _, err = dst.SaveVersion()
	h.NoError(err)

	areEqual, err := haveEqualRoots(dst.MutableTree, tree)
	if err != nil {
		return err
	}
	if !areEqual {
		return errors.New("iavl and deep subtree roots are not equal")
	}
	return nil
}

// Performs the Get operation on full IAVL tree first, gets the witness data generated from
// the operation, and uses that witness data to perform the same operation on the Deep Subtree
func (h *helper) get(key []byte) error {
	if key == nil {
		return nil
	}
	tree, dst := h.tree, h.dst

	treeValue, err := tree.Get(key)
	if err != nil {
		return fmt.Errorf("tree get: %w", err)
	}
	witness := tree.witnessData[len(tree.witnessData)-1]
	dst.SetWitnessData([]WitnessData{witness})

	dstValue, err := dst.Get(key)
	if err != nil {
		return fmt.Errorf("dst get: %w", err)
	}
	if !bytes.Equal(dstValue, treeValue) {
		return fmt.Errorf("get mismatch: key: %x: expect %x: got: %x", key, treeValue, dstValue)
	}

	return nil
}

// Performs the Has operation on full IAVL tree first, gets the witness data generated from
// the operation, and uses that witness data to perform the same operation on the Deep Subtree
func (h *helper) has(key []byte) error {
	if key == nil {
		return nil
	}
	tree, dst := h.tree, h.dst

	hasExpect, err := tree.Has(key)
	if err != nil {
		return fmt.Errorf("tree has: %w", err)
	}
	witness := tree.witnessData[len(tree.witnessData)-1]
	dst.SetWitnessData([]WitnessData{witness})

	hasGot, err := dst.Has(key)
	if err != nil {
		return fmt.Errorf("dst has: key: %x: %w", key, err)
	}

	if hasGot != hasExpect {
		return fmt.Errorf("has mismatch: key: %x: expect %t: got: %t", key, hasExpect, hasGot)
	}

	return nil
}

// Performs the Iterate operation  TODO:..
// if stopAfter is positive, the iterations will be capped
// returns the number of tree items visited
func (h *helper) iterate(start, end []byte, ascending bool, stopAfter uint8) (nVisited int, err error) {
	if start == nil || end == nil {
		return
	}
	tree, dst := h.tree, h.dst

	/*
		TODO: do I need to explicitly test Domain,Valid,Next,Key,Value,Error,Close? In varying dynamic (non usual) ways?
	*/

	l := len(tree.witnessData)

	// Set key-value pair in IAVL tree
	itTree, err := tree.Iterator(start, end, ascending)
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

	i := uint8(0)
	for ; itTree.Valid() && (stopAfter == 0 || i < stopAfter); itTree.Next() {

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
	}

	h.NoError(itTree.(TracingIterator).err)

	itTreeCloseErr := itTree.Close()

	dst.SetWitnessData(slices.Clone(tree.witnessData[l:])) // TODO: need clone?

	// Set key-value pair in IAVL tree
	itDST, err := dst.Iterator(start, end, ascending)
	if err != nil {
		return
	}

	i = 0
	for ; itDST.Valid() && (stopAfter == 0 || i < stopAfter); itDST.Next() {
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
	}
	// h.NoError(itDST.(VerifyingIterator).err)

	if int(i) != len(results) {
		return 0, fmt.Errorf("valid cnt mismatch: expect %d: got %d", len(results), i)
	}

	itDSTCloseErr := itDST.Close()

	if !errors.Is(itTreeCloseErr, itDSTCloseErr) || !errors.Is(itDSTCloseErr, itTreeCloseErr) { // TODO: makes sense?
		return 0, fmt.Errorf("close error mismatch")
	}

	return len(results), nil
}

type helper struct {
	t       *rapid.T
	tree    *MutableTree
	dst     *DeepSubTree
	keys    set.Set[string]
	drawOp  *rapid.Generator[op]
	drawInt *rapid.Generator[int]
}

func (h *helper) NoError(err error, msgAndArgs ...any) {
	if err != nil {
		h.t.Fatalf("%v", err)
	}
}

func (h *helper) getOp() op {
	return h.drawOp.Draw(h.t, "op")
}

func (h *helper) getInt(label string) int {
	return h.drawInt.Draw(h.t, label)
}

func (h *helper) getKey(canBeNew bool) int {
	i := h.getInt("key")
	/*
			TODO: I need to make the key return a sensible number,
		 Maybe I can combine generators, or use the state machine
	*/
}

func TestRapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		choices := []op{
			Set,
			// int(Get),
			// int(Has),
			// int(Remove),
			Iterate,
		}
		getOp := rapid.SampledFrom(choices)
		getInt := rapid.IntRange(0, 100)
		h := helper{
			t:       t,
			drawOp:  getOp,
			drawInt: getInt,
			keys:    make(set.Set[string]),
		}

		/*
			rollapp to hub, hub to rollapp
			when transfer
		*/

		fastStorage := false
		t.Logf("fast storage: %t\n", fastStorage)
		tree, err := NewMutableTreeWithOpts(db.NewMemDB(), cacheSize, nil, !fastStorage)
		h.NoError(err)
		tree.SetTracingEnabled(true)
		dst := NewDeepSubTree(db.NewMemDB(), cacheSize, !fastStorage, 0)
		h.tree = tree
		h.dst = dst

		numOps := h.getInt("num ops")
		for i := 0; i < numOps; i++ {
			switch h.getOp() {
			case Set:
				keyToAdd, err := tc.getKey(true, true)
				require.NoError(err)
				t.Logf("%d: Add: %x\n", i, keyToAdd)
				value := make([]byte, 32)
				binary.BigEndian.PutUint64(value, uint64(i))
				err = h.set(keyToAdd, value)
				if err != nil {
					t.Fatal(err)
				}
			case Remove:
				keyToDelete, err := tc.getKey(false, false)
				require.NoError(err)
				t.Logf("%d: Remove: %x\n", i, keyToDelete)
				err = h.remove(keyToDelete)
				if err != nil {
					t.Fatal(err)
				}
				keys.Delete(string(keyToDelete))
			case Get:
				keyToGet, err := tc.getKey(true, false)
				require.NoError(err)
				t.Logf("%d: Get: %x\n", i, keyToGet)
				err = h.get(keyToGet)
				if err != nil {
					t.Fatal(err)
				}
			case Has:
				keyToGet, err := tc.getKey(true, false)
				require.NoError(err)
				t.Logf("%d: Has: %x\n", i, keyToGet)
				err = h.has(keyToGet)
				if err != nil {
					t.Fatal(err)
				}
			case Iterate:
				keyA, err := tc.getKey(true, false)
				require.NoError(err)
				keyB, err := tc.getKey(true, false)
				require.NoError(err)
				ascending := tc.getByte()%2 == 0
				var stopAfter uint8
				if tc.getByte()%2 == 0 {
					stopAfter = tc.getByte()
				}
				t.Logf("%d: Iterate: [%x,%x,%t,%d]\n", i, keyA, keyB, ascending, stopAfter)
				_, err = h.iterate(keyA, keyB, ascending, stopAfter)
				if err != nil {
					t.Fatal(err)
				}
			}
		}

		s := rapid.SliceOf(rapid.String()).Draw(t, "s")
		sort.Strings(s)
		if !sort.StringsAreSorted(s) {
			t.Fatalf("unsorted after sort: %v", s)
		}
	})
}
