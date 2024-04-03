package iavl

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"testing"

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
	Noop
)

const (
	cacheSize = math.MaxUint16
)

// TODO: maybe need to impl tm.DB in full? TODO(danwt): remove this
type mustImpl interface {
	Has(key []byte) (bool, error)
	Iterator(start, end []byte, ascending bool) (db.Iterator, error)
}

var _ mustImpl = (*DeepSubTree)(nil)

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

// Tests that setting new values in the deep subtree, results in a correctly updated root hash.
// Reference: https://ethresear.ch/t/data-availability-proof-friendly-state-tree-transitions/1453/23
func TestDeepSubtreeWithValueUpdates(t *testing.T) {
	require := require.New(t)
	getTree := func() *MutableTree {
		tree, err := getTestTree(5)
		require.NoError(err)

		tree.SetTracingEnabled(true)

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

	testCases := [][][]byte{
		{
			[]byte("a"), []byte("b"),
		},
		{
			[]byte("c"), []byte("d"),
		},
	}

	for _, subsetKeys := range testCases {
		tree := getTree()
		rootHash, err := tree.WorkingHash()
		require.NoError(err)
		dst := NewDeepSubTree(db.NewMemDB(), 100, true, tree.version)
		require.NoError(err)
		dst.SetInitialRootHash(tree.root.hash)
		for _, subsetKey := range subsetKeys {
			ics23proof, err := tree.GetMembershipProof(subsetKey)
			require.NoError(err)
			err = dst.AddExistenceProofs([]*ics23.ExistenceProof{
				ics23proof.GetExist(),
			}, rootHash)
			require.NoError(err)
		}

		areEqual, err := haveEqualRoots(dst.MutableTree, tree)
		require.NoError(err)
		require.True(areEqual)

		tc := testContext{
			tree:    tree,
			dst:     dst,
			require: require,
		}

		values := [][]byte{{10}, {20}}
		for i, subsetKey := range subsetKeys {
			err := tc.set(subsetKey, values[i])
			require.NoError(err)
		}
	}
}

// Makes sure that the root hash of the deep subtree is equal to the root hash of the IAVL tree
// whenever we add new nodes or delete existing nodes
func TestDeepSubtreeWithAddsAndDeletes(t *testing.T) {
	require := require.New(t)
	getTree := func() *MutableTree {
		tree, err := getTestTree(5)
		require.NoError(err)

		tree.SetTracingEnabled(true)

		_, err = tree.Set([]byte("b"), []byte{2})
		require.NoError(err)
		_, err = tree.Set([]byte("a"), []byte{1})
		require.NoError(err)

		_, _, err = tree.SaveVersion()
		require.NoError(err)
		return tree
	}
	tree := getTree()

	subsetKeys := [][]byte{
		[]byte("b"),
	}
	rootHash, err := tree.WorkingHash()
	require.NoError(err)
	dst := NewDeepSubTree(db.NewMemDB(), 100, true, tree.version)
	require.NoError(err)
	dst.SetInitialRootHash(tree.root.hash)
	for _, subsetKey := range subsetKeys {
		ics23proof, err := tree.GetMembershipProof(subsetKey)
		require.NoError(err)
		err = dst.AddExistenceProofs([]*ics23.ExistenceProof{
			ics23proof.GetExist(),
		}, rootHash)
		require.NoError(err)
	}

	keysToAdd := [][]byte{
		[]byte("c"), []byte("d"),
	}
	valuesToAdd := [][]byte{
		{3}, {4},
	}
	tc := testContext{
		tree:    tree,
		dst:     dst,
		require: require,
	}
	require.Equal(len(keysToAdd), len(valuesToAdd))

	// Add all the keys we intend to add and check root hashes stay equal
	for i, keyToAdd := range keysToAdd {
		err := tc.set(keyToAdd, valuesToAdd[i])
		require.NoError(err)
		err = tc.has(keyToAdd)
		require.NoError(err)

	}

	require.Equal(len(keysToAdd), len(valuesToAdd))

	// Delete all the keys we added and check root hashes stay equal
	for i := range keysToAdd {
		keyToDelete := keysToAdd[i]

		err := tc.remove(keyToDelete)
		require.NoError(err)
	}
}

// A simple sanity check for the iterator - rely on the fuzz test instead, for completeness
func TestDeepSubtreeWithIterator(t *testing.T) {
	require := require.New(t)
	getTree := func() *MutableTree {
		tree, err := getTestTree(5)
		require.NoError(err)

		tree.SetTracingEnabled(true)

		_, err = tree.Set([]byte("f"), []byte{5})
		require.NoError(err)
		_, err = tree.Set([]byte("e"), []byte{4})
		require.NoError(err)
		_, err = tree.Set([]byte("d"), []byte{3})
		require.NoError(err)
		_, err = tree.Set([]byte("c"), []byte{2})
		require.NoError(err)
		_, err = tree.Set([]byte("b"), []byte{1})
		require.NoError(err)
		// don't include 'a', to improve readability of edge iterators

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
		[]byte("f"), []byte("e"), []byte("d"), []byte("c"), []byte("b"),
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

	tc := testContext{
		tree:    tree,
		dst:     dst,
		require: require,
	}

	type tCase struct {
		start, end          []byte
		ascending           bool
		stopAfter           uint8
		nVisitedSanityCheck int
	}

	for _, c := range []tCase{
		// tree has b,c,d,e,f
		{[]byte("a"), []byte("z"), true, 0, 5},
		{[]byte("a"), []byte("c"), true, 0, 1},
		{[]byte("b"), []byte("z"), true, 0, 5},
		{[]byte("b"), []byte("f"), true, 0, 4},
		{[]byte("a"), []byte("c"), false, 0, 1},
		{[]byte("b"), []byte("z"), false, 0, 5},
		{[]byte("b"), []byte("f"), false, 0, 4},
		{[]byte("a"), []byte("c"), true, 2, 1},
		{[]byte("b"), []byte("z"), true, 2, 2},
		{[]byte("b"), []byte("f"), true, 2, 2},
		{[]byte("a"), []byte("c"), false, 2, 1},
		{[]byte("b"), []byte("z"), false, 2, 2},
		{[]byte("b"), []byte("f"), false, 2, 2},
	} {
		t.Run(fmt.Sprintf("start=%s,end=%s,ascending=%t,stopAfter=%d", c.start, c.end, c.ascending, c.stopAfter), func(t *testing.T) {
			nVisited, err := tc.iterate(c.start, c.end, c.ascending, c.stopAfter)
			require.NoError(err)
			require.Equal(c.nVisitedSanityCheck, nVisited)
		})
	}
}

// Is a replication for a bug found during fuzzing
func TestReplication(t *testing.T) {
	require := require.New(t)

	for _, fastStorage := range []bool{false} {
		t.Run("fastStorage="+fmt.Sprint(fastStorage), func(t *testing.T) {
			tree, err := NewMutableTreeWithOpts(db.NewMemDB(), cacheSize, nil, !fastStorage)
			require.NoError(err)
			tree.SetTracingEnabled(true)
			dst := NewDeepSubTree(db.NewMemDB(), cacheSize, !fastStorage, 0)
			keys := make(set.Set[string])
			tc := testContext{
				tree:    tree,
				dst:     dst,
				keys:    keys,
				require: require,
			}
			err = tc.set([]byte("a"), []byte{1})
			require.NoError(err)

			err = tc.set([]byte("b"), []byte{1})
			require.NoError(err)

			n, err := tc.iterate([]byte("a"), []byte("c"), true, 0)
			require.NoError(err)
			require.Equal(2, n)

			err = tc.set([]byte("a"), []byte{1})
			require.NoError(err)

			n, err = tc.iterate([]byte("a"), []byte("c"), true, 0)
			require.NoError(err)
			require.Equal(2, n)
		})
	}
}

type testContext struct {
	r        *bytes.Reader
	tree     *MutableTree
	dst      *DeepSubTree
	keys     set.Set[string]
	require  *require.Assertions
	byteReqs int
}

func (tc *testContext) getByte() byte {
	bz, err := tc.r.ReadByte()
	tc.byteReqs++
	tc.require.NoError(err, "byte request %d", tc.byteReqs)
	return bz
}

// If genRandom, returns a random NEW key, half of the time. If addsNewKey is true, adds the key to the set of keys.
// Otherwise, returns a randomly picked existing key
// If we're not creating keys, and none already exist, returns the blank key
func (tc *testContext) getKey(genRandom bool, addsNewKey bool) (key []byte, err error) {
	tree, r, keys := tc.tree, tc.r, tc.keys
	if genRandom && tc.getByte() < math.MaxUint8/2 {

		// generate a key
		k := make([]byte, 4)
		_, err := r.Read(k)
		tc.require.NoError(err)

		n := tc.getByte() % 16
		binary.BigEndian.PutUint32(k, uint32(n))

		/////////
		// TODO: why is this block here? can use has instead?
		_, err = tree.Get(k)
		if err != nil {
			return nil, err
		}
		////////

		if addsNewKey {
			keys.Add(string(k))
		}
		return k, nil
	}
	if keys.Len() == 0 {
		return nil, nil
	}
	keyList := keys.Values()
	kString := keyList[int(tc.getByte())%len(keys)]
	return []byte(kString), nil
}

// Performs the Set operation on full IAVL tree first, gets the witness data generated from
// the operation, and uses that witness data to perform the same operation on the Deep Subtree
func (tc *testContext) set(key []byte, value []byte) error {
	if key == nil {
		return nil
	}
	tree, dst := tc.tree, tc.dst

	// Set key-value pair in IAVL tree
	_, err := tree.Set(key, value)
	if err != nil {
		return err
	}
	_, _, err = tree.SaveVersion()
	tc.require.NoError(err)
	witness := tree.witnessData[len(tree.witnessData)-1]
	dst.SetWitnessData([]WitnessData{witness})

	// Set key-value pair in DST
	_, err = dst.Set(key, value)
	if err != nil {
		return err
	}
	_, _, err = dst.SaveVersion()
	tc.require.NoError(err)

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
func (tc *testContext) remove(key []byte) error {
	if key == nil {
		return nil
	}
	tree, dst := tc.tree, tc.dst

	// Set key-value pair in IAVL tree
	_, _, err := tree.Remove(key)
	if err != nil {
		return err
	}
	_, _, err = tree.SaveVersion()
	tc.require.NoError(err)

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
	tc.require.NoError(err)

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
func (tc *testContext) get(key []byte) error {
	if key == nil {
		return nil
	}
	tree, dst := tc.tree, tc.dst

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
func (tc *testContext) has(key []byte) error {
	if key == nil {
		return nil
	}
	tree, dst := tc.tree, tc.dst

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
func (tc *testContext) iterate(start, end []byte, ascending bool, stopAfter uint8) (nVisited int, err error) {
	if start == nil || end == nil {
		return
	}
	tree, dst := tc.tree, tc.dst

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

	tc.require.NoError(itTree.(TracingIterator).err)

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
	// tc.require.NoError(itDST.(VerifyingIterator).err)

	if int(i) != len(results) {
		return 0, fmt.Errorf("valid cnt mismatch: expect %d: got %d", len(results), i)
	}

	itDSTCloseErr := itDST.Close()

	if !errors.Is(itTreeCloseErr, itDSTCloseErr) || !errors.Is(itDSTCloseErr, itTreeCloseErr) { // TODO: makes sense?
		return 0, fmt.Errorf("close error mismatch")
	}

	return len(results), nil
}

// Fuzz tests different combinations of Get, Remove, Set, Has, Iterate operations generated in
// a random order with keys related to operations chosen randomly
// go test -run=FuzzAllOps -count=1 --fuzz=Fuzz .
func FuzzAllOps(f *testing.F) {
	f.Fuzz(func(t *testing.T, input []byte) {
		require := require.New(t)
		if len(input) < 200 {
			return
		}

		r := bytes.NewReader(input)
		tc := testContext{
			r:       r,
			require: require,
		}
		// It should always be false?
		// fastStorage := tc.getByte()%2 == 0
		fastStorage := false
		t.Logf("fast storage: %t\n", fastStorage)

		tree, err := NewMutableTreeWithOpts(db.NewMemDB(), cacheSize, nil, !fastStorage)
		require.NoError(err)
		tree.SetTracingEnabled(true)
		dst := NewDeepSubTree(db.NewMemDB(), cacheSize, !fastStorage, 0)
		keys := make(set.Set[string])
		tc.tree = tree
		tc.dst = dst
		tc.keys = keys

		bytesNeededWorstCase := 20 // we might need up to this many, per operation
		for i := 0; bytesNeededWorstCase < r.Len(); i++ {
			tc.byteReqs = 0
			b, err := r.ReadByte()
			require.NoError(err)
			op := op(int(b) % int(Noop)) // TODO: noop
			require.NoError(err)
			switch op {
			// TODO: this only tests keys which are known to be present.. is that right?
			case Set:
				keyToAdd, err := tc.getKey(true, true)
				require.NoError(err)
				t.Logf("%d: Add: %x\n", i, keyToAdd)
				value := make([]byte, 32)
				binary.BigEndian.PutUint64(value, uint64(i))
				err = tc.set(keyToAdd, value)
				if err != nil {
					t.Fatal(err)
				}
			case Remove:
				keyToDelete, err := tc.getKey(false, false)
				require.NoError(err)
				t.Logf("%d: Remove: %x\n", i, keyToDelete)
				err = tc.remove(keyToDelete)
				if err != nil {
					t.Fatal(err)
				}
				keys.Delete(string(keyToDelete))
			case Get:
				keyToGet, err := tc.getKey(true, false)
				require.NoError(err)
				t.Logf("%d: Get: %x\n", i, keyToGet)
				err = tc.get(keyToGet)
				if err != nil {
					t.Fatal(err)
				}
			case Has:
				keyToGet, err := tc.getKey(true, false)
				require.NoError(err)
				t.Logf("%d: Has: %x\n", i, keyToGet)
				err = tc.has(keyToGet)
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
				_, err = tc.iterate(keyA, keyB, ascending, stopAfter)
				if err != nil {
					t.Fatal(err)
				}

			default:
				panic("unhandled default case")
			}
		}
		t.Log("Done")
	})
}
