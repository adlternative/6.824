// package main
package raft

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
)

func AnalysisLog(fileName string) error {

	fi, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer fi.Close()

	br := bufio.NewReader(fi)
	for {
		line, _, c := br.ReadLine()
		if c == io.EOF {
			break
		}
		blocks := strings.Split(string(line), " ")
		if len(blocks) < 6 {
			fmt.Println(blocks)
			continue
		}
		re := regexp.MustCompile(`^S\[(\d){3}\]$`)
		matched := re.Find([]byte(blocks[3]))
		if matched == nil {
			continue
		} else if server_index, err := strconv.Atoi(string(matched[2 : len(matched)-1])); err != nil {
			return err
		} else {
			for i := 0; i < 3*server_index; i++ {
				fmt.Print("\t")
			}

			fmt.Print(blocks[2:])
			// 	fmt.Print(block)
			// }
			fmt.Print("\n")
		}

		// term_block := blocks[3]

	}
	return nil
}

// func main() {
// 	if err := AnalysisLog("./D.err"); err != nil {
// 		fmt.Print(err.Error())
// 	}
// }
