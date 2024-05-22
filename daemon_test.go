package main

import (
	"bytes"
	"gearmanx/pkg/command"
	"gearmanx/pkg/consts"
	"gearmanx/pkg/parser"

	"io"
	"net"
	"sync"
	"testing"
)

func BenchmarkNewByteWithData(b *testing.B) {
	payload := []byte("\x00reverse\x00test")
	for i := 0; i < b.N; i++ {
		command.NewByteWithData(consts.REQUEST, consts.SUBMIT_JOB_BG, payload)
	}
}

func TestParserWithNet(t *testing.T) {
	cli, server := net.Pipe()

	wg := sync.WaitGroup{}

	payload := []byte("\x00reverse\x00test")
	for i := 0; i < 10982; i++ {
		payload = append(payload, 1)
	}

	req := command.NewByteWithData(consts.REQUEST, consts.SUBMIT_JOB_BG, payload)

	go func() {
		buf := make([]byte, 1024)
		var bsize int
		var err error
		fragmented_buf := bytes.Buffer{}

		for {
			bsize, err = server.Read(buf)
			if err == io.EOF {
				break
			}

			if err != nil {
				t.Errorf("server.Read> %s\n", err)
				break
			}

			// fmt.Printf("Read %d bytes, data: %v\n", bsize, buf)
			commands := parser.Parse(buf, bsize, &fragmented_buf)
			if len(commands) == 1 {
				if bytes.Equal(commands[0].Bytes(), req) {
					// t.Logf("commands[0].Bytes() : %v\n        req : %v", commands[0].Bytes(), req)
					wg.Done()
				} else {
					t.Errorf("commands[0].Bytes() : %v\n        req : %v", commands[0].Bytes(), req)
					wg.Done()
				}
			}
		}

		server.Close()
	}()

	wg.Add(1)
	cli.Write(req)

	wg.Wait()
	cli.Close()
}

func TestParserWithNetMultiJob(t *testing.T) {
	cli, server := net.Pipe()

	wg := sync.WaitGroup{}

	payload := []byte("\x00reverse\x00test")
	for i := 0; i < 10982; i++ {
		payload = append(payload, 1)
	}

	task1 := command.NewByteWithData(consts.REQUEST, consts.SUBMIT_JOB_BG, payload)
	task2 := command.NewByteWithData(consts.REQUEST, consts.GRAB_JOB)

	task := append(task1, task2...)

	go func() {
		buf := make([]byte, 1024)
		var bsize int
		var err error
		fragmented_buf := bytes.Buffer{}

		for {
			bsize, err = server.Read(buf)
			if err == io.EOF {
				break
			}

			if err != nil {
				t.Errorf("server.Read> %s\n", err)
				break
			}

			// fmt.Printf("Read %d bytes, data: %v\n", bsize, buf)
			commands := parser.Parse(buf, bsize, &fragmented_buf)
			if len(commands) == 2 {
				if !bytes.Equal(commands[0].Bytes(), task1) {
					t.Errorf("commands[0].Bytes() : %v\n        req : %v", commands[0].Bytes(), task1)
				}

				if !bytes.Equal(commands[1].Bytes(), task2) {
					t.Errorf("commands[0].Bytes() : %v\n        req : %v", commands[1].Bytes(), task2)
				}
				wg.Done()
			}
		}

		server.Close()
	}()

	wg.Add(1)
	cli.Write(task)

	wg.Wait()
	cli.Close()
}
