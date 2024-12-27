package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

func main() {
	file, err := os.Open("./FloridaBlue_GBO_in-network-rates.json")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	decoder := json.NewDecoder(bufio.NewReaderSize(file, 1024*1024))
	cnt := 0
	start := time.Now()
	for {
		_, err := decoder.Token()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			fmt.Println("Error reading token:", err)
			return
		}
		//fmt.Printf("Token: %v\n", token)
		cnt += 1
		if cnt%1_000_000 == 0 {
			elapsed := time.Since(start)
			fmt.Printf("Tokens read: %v M in %v\n", cnt/1_000_000, elapsed)
		}
	}
}
