package lsmtree

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path"
	"sync"
	"time"

	"github.com/lindend/distdb/internal/sstable"

	"github.com/rs/zerolog/log"
)

const Kilobyte = 1024
const Megabyte = 1024 * Kilobyte
const Gigabyte = 1024 * Megabyte
const Terabyte = 1024 * Gigabyte

const (
	RecordKindWrite  uint64 = 0x1000
	RecordKindDelete uint64 = 0x1001
)

type layer struct {
	name      string
	maxChunks int
	chunks    []*chunk
	lock      *sync.RWMutex
}

type lsmLayerJson struct {
	Name      string
	MaxChunks int
	Chunks    []lsmChunkJson
}

type lsmChunkJson struct {
	Name      string
	ChunkType chunkType
}

type lsmTreeJson struct {
	Layers           []lsmLayerJson
	Root             lsmChunkJson
	MaxRootChunkSize uint64
}

type LsmTree struct {
	layers           []layer
	exit             chan int
	rootChunk        *chunk
	maxRootChunkSize uint64
	rootDir          string
}

func createChunkData(chunkType chunkType, rootDir string, name string) (chunkData, error) {
	switch chunkType {
	case chunkTypeSkiplist:
		return newSkipListChunk(path.Join(rootDir, fmt.Sprintf("wal-%v.log", name)))
	case chunkTypeSSTable:
		tbl, err := sstable.LoadSSTable(rootDir, name)
		if err != nil {
			return nil, err
		}
		return &sstableChunk{tbl}, nil
	}
	panic("Unknown chunkType")
}

func NewLsmTree(rootDir string) (*LsmTree, error) {
	// Try to load an existing tree
	existingTree, err := load(rootDir)
	if existingTree != nil {
		return existingTree, nil
	}

	// If the error is not DoesntExist
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	// Create a new tree instead
	rootChunkName := randomString(6)
	chunkData, err := createChunkData(chunkTypeSkiplist, rootDir, rootChunkName)
	if err != nil {
		return nil, err
	}

	rootChunk := &chunk{
		name:      rootChunkName,
		data:      chunkData,
		chunkType: chunkTypeSkiplist,
	}

	tree := &LsmTree{
		rootDir:          rootDir,
		rootChunk:        rootChunk,
		maxRootChunkSize: 16 * Megabyte,
		layers: []layer{
			{
				name:      "layer-0",
				maxChunks: 4,
				chunks:    []*chunk{},
				lock:      &sync.RWMutex{},
			},
			{
				name:      "layer-1",
				maxChunks: 8,
				chunks:    []*chunk{},
				lock:      &sync.RWMutex{},
			},
			{
				name:      "layer-2",
				maxChunks: 4,
				chunks:    []*chunk{},
				lock:      &sync.RWMutex{},
			},
			{
				name:      "layer-3",
				maxChunks: 0,
				chunks:    []*chunk{},
				lock:      &sync.RWMutex{},
			},
		},
		exit: make(chan int),
	}

	go tree.mergeProcess()

	return tree, nil
}

type entry struct {
	key  string
	kind uint64
	data []byte
}

func getEntry(it chunkIterator) *entry {
	if it == nil {
		return nil
	}
	kind, key, data := it.value()
	return &entry{
		key:  key,
		kind: kind,
		data: data,
	}
}

// Finds the minimum entry by key
func getMin(e []*entry) (int, bool) {
	exists := false
	min := -1
	minKey := ""

	for i := 0; i < len(e); i++ {
		if e[i] != nil {
			if !exists || e[i].key < minKey {
				minKey = e[i].key
				min = i
			}
			exists = true
		}
	}
	return min, exists
}

// Spins up a thread that populates the return channel with ordered
// entries merged from all the iterators. When all entries have been
// returned, the channel will be closed.
func getEntries(its []chunkIterator) <-chan entry {
	entries := make([]*entry, len(its))
	// Populate all entries with the current values
	for i := 0; i < len(its); i++ {
		entries[i] = getEntry(its[i])
	}

	resultChan := make(chan entry, 4)

	go func() {
		for {
			// Find the key with the lowest index
			min, exists := getMin(entries)
			// if we couldn't find any keys, we are done
			if !exists {
				close(resultChan)
				break
			}
			if entries[min] != nil {
				resultChan <- *entries[min]
			}
			// Progress the chosen iterator and load the next value
			its[min] = its[min].next()
			entries[min] = getEntry(its[min])
		}
	}()

	return resultChan
}

func parallellMerge(its []chunkIterator, tbl *sstable.SSTableBuilder) error {
	prevKey := ""

	entries := getEntries(its)

	for entry := range entries {
		if entry.key != prevKey {
			tbl.Write(entry.key, RecordKindWrite, entry.data)
		}
		prevKey = entry.key
	}
	return nil
}

var randomFileChars = []rune("abcdefghijklmnopqrstuvwxyz1234567890")

func randomString(length int) string {
	res := make([]rune, length)

	for i := range res {
		res[i] = randomFileChars[rand.Intn(len(randomFileChars))]
	}
	return string(res)
}

func (tree *LsmTree) hasChunkWithName(layer int, name string) bool {
	for i := range tree.layers[layer].chunks {
		if tree.layers[layer].chunks[i].name == name {
			return true
		}
	}
	return false
}

func (tree *LsmTree) generateChunkName(layer int) string {
	for {
		name := fmt.Sprintf("layer-%v-%v", layer, randomString(6))
		if !tree.hasChunkWithName(layer, name) {
			return name
		}
	}
}

// Merges an LsmTree layer with the next one. Will always merge to
// a SSTable. Should support concurrent reads and writes will performing
// the merge. Not concurrent merges though.
func (tree *LsmTree) mergeLayer(layerIdx int) error {
	start := time.Now()

	l := tree.layers[layerIdx]
	chunks := l.chunks

	numEntries := int64(0)
	for i := 0; i < len(chunks); i++ {
		numEntries += chunks[i].data.numEntries()
	}

	nextLayerIdx := layerIdx
	if layerIdx < len(tree.layers)-1 {
		nextLayerIdx = layerIdx + 1
	}

	log.Debug().
		Int("layer", layerIdx).
		Int("target", nextLayerIdx).
		Msg("Merging layers")

	chunkName := tree.generateChunkName(nextLayerIdx)
	// Create a new SSTable chunk with a random name to merge to
	tblBuilder, err := sstable.NewSSTable(uint(numEntries), tree.rootDir, chunkName)
	if err != nil {
		return err
	}

	// Prepare chunk iterators
	chunkIts := make([]chunkIterator, len(chunks))
	for i := 0; i < len(chunks); i++ {
		chunkIts[i] = chunks[i].data.iterator()
	}

	// Merge chunks into next layer
	parallellMerge(chunkIts, tblBuilder)

	sstable, err := tblBuilder.Build()
	if err != nil {
		return err
	}

	sstableLayer := sstableChunk{
		tbl: sstable,
	}

	// Lock layer we are merging to update the chunk info
	l.lock.Lock()
	defer l.lock.Unlock()

	// Clear chunks of layer we merged from
	l.chunks = nil

	nextLayer := &tree.layers[nextLayerIdx]
	if layerIdx != nextLayerIdx {
		// If we are merging to a new layer, get a lock on that layer too
		nextLayer.lock.Lock()
		defer nextLayer.lock.Unlock()
	}

	newChunk := &chunk{
		name:      chunkName,
		data:      &sstableLayer,
		chunkType: chunkTypeSSTable,
	}

	nextLayer.chunks = append([]*chunk{newChunk}, nextLayer.chunks...)

	tree.save()

	// Everything is merged and saved, delete old chunks
	for i := range chunks {
		chunks[i].data.delete()
	}

	log.Info().Dur("duration", time.Since(start)).Msg("Merge complete")
	return nil
}

// Save the structure of the LSM tree to a file
func (tree *LsmTree) save() error {
	fileName := path.Join(tree.rootDir, "lsm.json")
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)

	layers := make([]lsmLayerJson, len(tree.layers))
	for i := range layers {
		chunks := make([]lsmChunkJson, len(tree.layers[i].chunks))
		for j := range tree.layers[i].chunks {
			chunks[j] = lsmChunkJson{
				Name:      tree.layers[i].chunks[j].name,
				ChunkType: tree.layers[i].chunks[j].chunkType,
			}
		}

		layers[i] = lsmLayerJson{
			Name:      tree.layers[i].name,
			MaxChunks: tree.layers[i].maxChunks,
			Chunks:    chunks,
		}
	}
	data := lsmTreeJson{
		Layers:           layers,
		MaxRootChunkSize: tree.maxRootChunkSize,
		Root: lsmChunkJson{
			Name:      tree.rootChunk.name,
			ChunkType: tree.rootChunk.chunkType,
		},
	}

	err = encoder.Encode(data)
	if err != nil {
		return err
	}

	return nil
}

func load(rootDir string) (*LsmTree, error) {
	fileName := path.Join(rootDir, "lsm.json")
	f, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	decoder := json.NewDecoder(f)

	m := lsmTreeJson{}
	err = decoder.Decode(&m)
	if err != nil {
		return nil, err
	}

	skiplist, err := createChunkData(chunkTypeSkiplist, rootDir, m.Root.Name)
	if err != nil {
		return nil, err
	}

	rootChunk := &chunk{
		name:      m.Root.Name,
		data:      skiplist,
		chunkType: m.Root.ChunkType,
	}

	layers := make([]layer, len(m.Layers))
	for i := range layers {
		l := m.Layers[i]
		chunks := make([]*chunk, len(l.Chunks))
		for j := range chunks {
			c := l.Chunks[j]
			chunkData, err := createChunkData(c.ChunkType, rootDir, c.Name)
			if err != nil {
				return nil, err
			}
			chunks[j] = &chunk{
				name:      c.Name,
				data:      chunkData,
				chunkType: c.ChunkType,
			}
		}

		layers[i] = layer{
			name:      m.Layers[i].Name,
			maxChunks: m.Layers[i].MaxChunks,
			chunks:    chunks,
			lock:      &sync.RWMutex{},
		}
	}

	tree := &LsmTree{
		layers:           layers,
		rootChunk:        rootChunk,
		exit:             make(chan int),
		rootDir:          rootDir,
		maxRootChunkSize: m.MaxRootChunkSize,
	}

	go tree.mergeProcess()

	return tree, nil

}

// Merge process running in the background, compacting layers
// that are full.
func (tree *LsmTree) mergeProcess() {
	for {
		time.Sleep(time.Second * 2)
		for i := 0; i < len(tree.layers); i++ {
			l := tree.layers[i]
			if len(l.chunks) > l.maxChunks {
				tree.mergeLayer(i)
			}
		}
	}
}

func (tree *LsmTree) Set(key string, data []byte) error {
	tree.rootChunk.data.set(key, RecordKindWrite, data)

	if tree.rootChunk.data.size() > tree.maxRootChunkSize {
		log.Debug().
			Msg("Root chunk full, pushing to layer0")

		root := tree.rootChunk
		tree.layers[0].chunks = append([]*chunk{root}, tree.layers[0].chunks...)
		chunkName := randomString(6)
		skiplistChunk, err := createChunkData(chunkTypeSkiplist, tree.rootDir, chunkName)
		if err != nil {
			return err
		}
		tree.rootChunk = &chunk{
			name:      chunkName,
			data:      skiplistChunk,
			chunkType: chunkTypeSkiplist,
		}

		return tree.save()
	}
	return nil
}

func (tree *LsmTree) LayerSizes() []uint64 {
	result := make([]uint64, len(tree.layers))

	for i := 0; i < len(tree.layers); i++ {
		total := uint64(0)
		layer := tree.layers[i]
		for j := 0; j < len(layer.chunks); j++ {
			total += layer.chunks[j].data.size()
		}
		result[i] = total
	}
	return result
}

func (tree *LsmTree) Get(key string) (data []byte, exists bool, err error) {
	kind, data, exists, err := tree.rootChunk.data.get(key)
	if err != nil {
		return nil, false, err
	}
	if err != nil {
		return nil, false, err
	}

	if kind == RecordKindDelete {
		return nil, false, nil
	}

	if exists {
		return data, true, nil
	}

	for i := 0; i < len(tree.layers); i++ {
		layer := tree.layers[i]
		layer.lock.RLock()
		defer layer.lock.RUnlock()

		for j := 0; j < len(layer.chunks); j++ {
			kind, data, exists, err := layer.chunks[j].data.get(key)
			if err != nil {
				return nil, false, err
			}

			if kind == RecordKindDelete {
				return nil, false, nil
			}

			if exists {
				return data, true, nil
			}
		}
	}
	return make([]byte, 0), false, nil
}
