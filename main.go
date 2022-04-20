// Raw socket connection with no validation and string quoting logic.
// Refer to protocol description:
// http://questdb.io/docs/reference/api/ilp/overview

package main

import (
	"fmt"
	"log"
	"math"
	"net"
	"sync"
	"time"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type Timeline struct {
	Time      *int64 `parquet:"name=time, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=true, logicaltype.unit=NANOS"`
	Duration  *int64 `parquet:"name=duration, type=INT64"`
	Id        *int64 `parquet:"name=id, type=INT64"`
	Name      *int32 `parquet:"name=name, type=INT32"`
	Category  *int32 `parquet:"name=category, type=INT32"`
	Precision *int32 `parquet:"name=precision, type=INT32"`
	// Name      string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	// Category  *string `parquet:"name=category, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	// Precision *string `parquet:"name=precision, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

func loadParquet(f string, ch chan string) {
	fr, err := local.NewLocalFileReader(f)
	if err != nil {
		log.Fatal(err)
	}
	pr, err := reader.NewParquetReader(fr, new(Timeline), 20)
	num_rows := int(pr.GetNumRows())
	rows, err := pr.ReadByNumber(num_rows)

	maxi := 0.0
	for i, row := range rows {
		addPoints(ch, row.(Timeline), i)
		maxi = math.Max(maxi, float64(i))
	}
	fmt.Println(maxi)

	fr.Close()
	pr.ReadStop()
	close(ch)
}

func generateBatches(f string) <-chan string {
	ch := make(chan string, 10)
	go loadParquet(f, ch)
	return ch
}

func addPoints(ch chan string, row Timeline, rowIndex int) {
	ch <- fmt.Sprintf("gpu,name=%d,category=%d,precision=%d duration=%d,id=%d, %d", *row.Name,
		*row.Category, *row.Precision, *row.Duration, *row.Id, *row.Time)
}

func sendBatch(ch <-chan string) {
	host := "127.0.0.1:9009"
	tcpAddr, err := net.ResolveTCPAddr("tcp4", host)
	checkErr(err)

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	checkErr(err)
	defer conn.Close()
	for s := range ch {
		_, err := conn.Write([]byte(fmt.Sprintf("%s\n", s)))
		checkErr(err)
	}
}

func main() {
	stime := time.Now().UTC()
	ch := generateBatches("/home/ubuntu/dev/questdb-demo/data/mrcnn_p4d_1node_gpu_kernels_trimmed.parquet")

	var wg sync.WaitGroup
	for i := 0; i < 256; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sendBatch(ch)
		}()
	}
	wg.Wait()
	elapsed_time := time.Since(stime).Seconds()
	fmt.Println("Elapsed ", elapsed_time)

	// rows := [2]string{
	// 	fmt.Sprintf("trades,name=test_ilp1 value=12.4 %d", time.Now().UnixNano()),
	// 	fmt.Sprintf("trades,name=test_ilp2 value=11.4 %d", time.Now().UnixNano()),
	// }
	// for _, s := range rows {
	// 	_, err = conn.Write([]byte(fmt.Sprintf("%s\n", s)))
	// 	checkErr(err)
	// }

	//   result, err := ioutil.ReadAll(conn)
	//   checkErr(err)

	//   fmt.Println(string(result))
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
