package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/olebedev/config"
)

func Swap(dir string, wg *sync.WaitGroup) {
	ch_min := make(chan struct{})
	ch_max := make(chan struct{})
	var wg1 sync.WaitGroup
	files_num := []int{}
	min := 1000
	max := -1000

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		if strings.Contains(file.Name(), ".log") {
			num, err := strconv.Atoi(file.Name()[:len(file.Name())-4])
			if err != nil {
				log.Fatal(err)
			}
			files_num = append(files_num, num)
		}
	}
	if len(files_num) == 0 || len(files_num) == 1 {
		log.Fatalf("directory have only %v files, at least we should have 2 one", len(files_num))
	}

	for _, v := range files_num {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}

	min_data, err := os.ReadFile(fmt.Sprintf("%v.log", min))
	if err != nil {
		log.Fatal("error with read file")
	}
	max_data, err := os.ReadFile(fmt.Sprintf("%v.log", max))
	if err != nil {
		log.Fatal("error with read file")
	}

	if len(max_data) == 0 && len(min_data) == 0 {
		log.Fatal("files are empty")
	}

	min_file, err := os.OpenFile(fmt.Sprintf("%v.log", min), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		log.Fatal("error with open file")
	}
	max_file, err := os.OpenFile(fmt.Sprintf("%v.log", max), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		log.Fatal("error with open file")
	}

	wg1.Add(2)

	go func() {
		for _, j := range max_data {
			wg.Add(1)
			go Write(j, min_file, wg, ch_min)
			<-ch_min
		}
		wg1.Done()
	}()

	go func() {
		for _, j := range min_data {
			wg.Add(1)
			go Write(j, max_file, wg, ch_max)
			<-ch_max
		}
		wg1.Done()
	}()

	wg1.Wait()
}

func Write(b byte, file *os.File, wg *sync.WaitGroup, ch chan struct{}) {
	file.Write([]byte{b})
	wg.Done()
	ch <- struct{}{}
}

func ReadConf(name string) string {
	file, err := ioutil.ReadFile(name)
	if err != nil {
		log.Fatal("error with read config")
	}
	yamlString := string(file)

	cfg, err := config.ParseYaml(yamlString)
	if err != nil {
		log.Fatal("error with parse config")
	}
	path, err := cfg.String("path")
	if err != nil {
		log.Fatal("error with read var")
	}
	return path
}

func main() {
	var wg sync.WaitGroup
	path := ReadConf("cnf.yml")

	Swap(path, &wg)

	wg.Wait()
}
