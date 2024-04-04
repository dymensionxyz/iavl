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

	"github.com/stretchr/testify/require"
	db "github.com/tendermint/tm-db"
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

func withRapidSM(t *rapid.T) {
	h := helper{
		t: t,
	}

	fastStorage := false
	tree, err := NewMutableTreeWithOpts(db.NewMemDB(), cacheSize, nil, !fastStorage)
	h.NoError(err)
	tree.SetTracingEnabled(true)
	dst := NewDeepSubTree(db.NewMemDB(), cacheSize, !fastStorage, 0)
	h.tree = tree
	h.dst = dst

	sm := SM{
		t:    t,
		h:    &h,
		keys: make(set.Set[int]),
	}

	sm.getGen = rapid.Make[GetCmd]()
	sm.setGen = rapid.Make[SetCmd]()
	sm.removeGen = rapid.Make[RemoveCmd]()
	sm.hasGen = rapid.Make[HasCmd]()
	sm.iterateGen = rapid.Make[IterateCmd]()

	t.Repeat(rapid.StateMachineActions(sm))
}

func TestFoo(t *testing.T) {
	rapid.Check(t, withRapidSM)
}

func FuzzFoo(f *testing.F) {
	// TODO: sanity check
	f.Fuzz(rapid.MakeFuzz(withRapidSM))
}

// Does the Set operation on full IAVL tree first, gets the witness data generated from
// the operation, and uses that witness data to perform the same operation on the Deep Subtree
func (h *helper) set(keyI, valueI int) error {
	key := toBz(keyI)
	value := toBz(valueI)
	// Set key-value pair in IAVL tree
	_, err := h.tree.Set(key, value)
	if err != nil {
		return err
	}
	_, _, err = h.tree.SaveVersion()
	h.NoError(err)
	witness := h.tree.witnessData[len(h.tree.witnessData)-1]
	h.dst.SetWitnessData([]WitnessData{witness})

	// Set key-value pair in DST
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

	treeValue, err := h.tree.Get(key)
	if err != nil {
		return fmt.Errorf("tree get: %w", err)
	}
	witness := h.tree.witnessData[len(h.tree.witnessData)-1]
	h.dst.SetWitnessData([]WitnessData{witness})

	dstValue, err := h.dst.Get(key)
	if err != nil {
		return fmt.Errorf("dst get: %w", err)
	}
	if !bytes.Equal(dstValue, treeValue) {
		return fmt.Errorf("get mismatch: key: %x: expect %x: got: %x", key, treeValue, dstValue)
	}

	return nil
}

// Does the Remove operation on full IAVL tree first, gets the witness data generated from
// the operation, and uses that witness data to peform the same operation on the Deep Subtree
func (h *helper) remove(keyI int) error {
	key := toBz(keyI)

	// Set key-value pair in IAVL tree
	_, _, err := h.tree.Remove(key)
	if err != nil {
		return err
	}
	_, _, err = h.tree.SaveVersion()
	h.NoError(err)

	witness := h.tree.witnessData[len(h.tree.witnessData)-1]
	h.dst.SetWitnessData([]WitnessData{witness})

	_, removed, err := h.dst.Remove(key)
	if err != nil {
		return err
	}
	if !removed {
		return fmt.Errorf("remove key from dst: %s", string(key))
	}
	_, _, err = h.dst.SaveVersion()
	h.NoError(err)

	return nil
}

// Does the Has operation on full IAVL tree first, gets the witness data generated from
// the operation, and uses that witness data to perform the same operation on the Deep Subtree
func (h *helper) has(keyI int) error {
	key := toBz(keyI)

	hasExpect, err := h.tree.Has(key)
	if err != nil {
		return fmt.Errorf("tree has: %w", err)
	}
	witness := h.tree.witnessData[len(h.tree.witnessData)-1]
	h.dst.SetWitnessData([]WitnessData{witness})

	hasGot, err := h.dst.Has(key)
	if err != nil {
		return fmt.Errorf("dst has: key: %x: %w", key, err)
	}

	if hasGot != hasExpect {
		return fmt.Errorf("has mismatch: key: %x: expect %t: got: %t", key, hasExpect, hasGot)
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

	h.dst.SetWitnessData(slices.Clone(h.tree.witnessData[l:])) // TODO: need clone?

	// Set key-value pair in IAVL tree
	itDST, err := h.dst.Iterator(start, end, ascending)
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

	// h.NoError(itDST.(VerifyingIterator).err) TODO: bring back?

	if i != len(results) {
		return 0, fmt.Errorf("valid cnt mismatch: expect %d: got %d", len(results), i)
	}

	itDSTCloseErr := itDST.Close()

	if !errors.Is(itTreeCloseErr, itDSTCloseErr) || !errors.Is(itDSTCloseErr, itTreeCloseErr) { // TODO: makes sense?
		return 0, fmt.Errorf("close error mismatch")
	}

	return len(results), nil
}

type helper struct {
	t    *rapid.T
	tree *MutableTree
	dst  *DeepSubTree
}

func (h *helper) NoError(err error, msgAndArgs ...any) {
	if err != nil {
		h.t.Fatalf("%v", err)
	}
}

type SM struct {
	t          *rapid.T
	h          *helper
	keys       set.Set[int]
	setGen     *rapid.Generator[SetCmd]
	getGen     *rapid.Generator[GetCmd]
	removeGen  *rapid.Generator[RemoveCmd]
	hasGen     *rapid.Generator[HasCmd]
	iterateGen *rapid.Generator[IterateCmd]
}

func toBz(i int) []byte {
	return []byte(strconv.Itoa(i))
}

func (sm SM) Check(t *rapid.T) {
	areEqual, err := haveEqualRoots(sm.h.dst.MutableTree, sm.h.tree)
	sm.h.NoError(err)
	if !areEqual {
		t.Fatal("iavl and deep subtree roots are not equal")
	}
}

func (sm SM) Set(t *rapid.T) {
	cmd := sm.setGen.Draw(t, "set")
	sm.keys.Add(cmd.K)
	err := sm.h.set(cmd.K, cmd.V)
	sm.h.NoError(err)
}

func (sm SM) Get(t *rapid.T) {
	cmd := sm.getGen.Draw(t, "get")
	err := sm.h.get(cmd.K)
	sm.h.NoError(err)
}

func (sm SM) Remove(t *rapid.T) {
	cmd := sm.removeGen.Draw(t, "remove")
	if !sm.keys.Has(cmd.K) {
		t.Logf("noop remove")
		return
	}
	sm.keys.Delete(cmd.K)
	err := sm.h.remove(cmd.K)
	sm.h.NoError(err)
}

func (sm SM) Has(t *rapid.T) {
	cmd := sm.hasGen.Draw(t, "has")
	err := sm.h.has(cmd.K)
	sm.h.NoError(err)
}

func (sm SM) Iterate(t *rapid.T) {
	cmd := sm.iterateGen.Draw(t, "iterate")
	_, err := sm.h.iterate(cmd.L, cmd.R, cmd.Ascending, cmd.StopAfter)
	sm.h.NoError(err)
}

type SetCmd struct {
	K, V int
}

type GetCmd struct {
	K int
}

type RemoveCmd struct {
	K int
}

type HasCmd struct {
	K int
}

type IterateCmd struct {
	L, R      int
	Ascending bool
	StopAfter int
}
