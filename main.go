package main

import (
	"fmt"
	"math/rand"
	"os"
	"runtime/pprof"
	"strconv"
	"time"

	"github.com/lindend/distdb/internal/lsmtree"
	"github.com/lindend/distdb/internal/sstable"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})

	tree()

	// tbl(10000000, "test", "small")
	// perfProf()
	// readTable()
	// tbl(300000000, "D:/dev/test", "big")
}

func tree() {
	tree, err := lsmtree.NewLsmTree("D:/dev/test/tree")
	if err != nil {
		panic(err)
	}

	data, _, _ := tree.Get("message1000")
	fmt.Println(string(data))

	start := time.Now()
	for i := 0; i < 100000000; i++ {
		tree.Set("message"+strconv.Itoa(i), []byte("Hello World "+strconv.Itoa(i)))
	}
	fmt.Println("Insert: ", time.Since(start))
	fmt.Println("Size", tree.LayerSizes())
	start = time.Now()
	data, _, _ = tree.Get("message1000")
	fmt.Println("Get: ", time.Since(start))
	fmt.Println(string(data))
}

func tbl(numElements int64, root, name string) {
	tblBuilder, err := sstable.NewSSTable(uint(numElements), root, name)
	if err != nil {
		panic(err.Error())
	}

	start := time.Now()
	for i := int64(0); i < numElements; i++ {
		if i > 0 && i%10000000 == 0 {
			fmt.Println(i, "of", numElements)
		}
		tblBuilder.Write(fmt.Sprintf("hej%012d", i), 0, []byte(fmt.Sprintf("hejsanthisisaslightlylongermessage%d", i)))
	}
	fmt.Println("Write SSTable: ", time.Since(start))
	fmt.Println("Write SSTable: ", time.Since(start)/time.Duration(numElements), "/element")

	tbl, err := tblBuilder.Build()

	if err != nil {
		panic(err.Error())
	}
	start = time.Now()
	var d []byte
	_, d, _, _ = tbl.Read(fmt.Sprintf("hej%012d", 29904))
	for i := int64(0); i < numElements; i++ {
		exists := false
		_, d, exists, _ = tbl.Read(fmt.Sprintf("hej%012d", i))
		if !exists {
			fmt.Println("missing", i)
		}
	}
	fmt.Println("Read SSTable: ", time.Since(start))
	fmt.Println(string(d))
}

func perfProf() {
	tbl, err := sstable.LoadSSTable("D:/dev/test", "big")
	if err != nil {
		panic(err)
	}

	f, _ := os.Create("cpuprofile.prof")
	pprof.StartCPUProfile(f)
	start := time.Now()
	const numValues = 100000
	for i := 0; i < numValues; i++ {
		tbl.Read(fmt.Sprintf("hej%012d", rand.Intn(300000000)))
	}
	fmt.Println("Read SSTable: ", time.Since(start)/numValues)
	pprof.StopCPUProfile()

}

func readTable() {
	tbl, _ := sstable.LoadSSTable("D:/dev/test", "big")
	tblSize, _ := tbl.Size()
	fmt.Println("SSTable size: ", tblSize)
	start := time.Now()
	_, d, _, _ := tbl.Read("hej000002000000")
	fmt.Println("Read SSTable: ", time.Since(start))
	fmt.Println("Data: ", string(d))
}
