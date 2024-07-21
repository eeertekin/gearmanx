package parser2_test

import (
	"bytes"
	"fmt"
	"gearmanx/pkg/command"

	"gearmanx/pkg/consts"
	"gearmanx/pkg/parser2"
	"io"
	"math/rand/v2"
	"testing"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.IntN(len(letterBytes))]
	}
	return b
}

func TestParserMulti(t *testing.T) {
	conn := bytes.Buffer{}

	job_count := 0
	for i := 0; i < 1023; i++ {
		conn.Write(command.Request(consts.CAN_DO, []byte("reverse")))
		job_count++

		conn.Write(command.Request(consts.CANT_DO, []byte("reverse")))
		job_count++

		conn.Write(command.Response(consts.WORK_COMPLETE, []byte("reverse")))
		job_count++
	}
	jobs_all := []*command.Command{}

	buf := make([]byte, 1024)
	left_buf := []byte{}
	cmds := []*command.Command{}
	for {
		bsize, err := conn.Read(buf)
		if err == io.EOF {
			break
		}

		if len(left_buf) > 0 {
			buf = append(left_buf, buf...)
			bsize += len(left_buf)
		}

		left_buf, cmds = parser2.Parse(buf[:bsize])
		jobs_all = append(jobs_all, cmds...)
	}
	_ = cmds

	if len(jobs_all) != job_count {
		t.Errorf("len(jobs_all) failed: %d != %d", len(jobs_all), job_count)
	}
}

func TestParserLarge(t *testing.T) {
	data := command.Request(consts.CAN_DO, RandBytes(8192))

	conn := bytes.NewBuffer(data)

	buf := make([]byte, 1024)
	left_buf := []byte{}
	cmds := []*command.Command{}
	for {
		bsize, err := conn.Read(buf)
		if err == io.EOF {
			break
		}

		if len(left_buf) > 0 {
			buf = append(left_buf, buf...)
			bsize += len(left_buf)
		}

		left_buf, cmds = parser2.Parse(buf[:bsize])
	}

	if len(cmds) != 1 {
		t.Errorf("len(cmds) failed: %d != %d", len(cmds), 1)
	}
}

func TestParserBroken(t *testing.T) {
	conn := bytes.Buffer{}
	// conn.Write([]byte("\x00RE"))

	job_count := 0
	for i := 0; i < 1024; i++ {
		conn.Write(command.Request(consts.CAN_DO, []byte(fmt.Sprintf("reverse%d", i))))
		job_count++
	}
	jobs_all := []*command.Command{}

	buf := make([]byte, 1024)
	left_buf := []byte{}
	cmds := []*command.Command{}
	for {
		bsize, err := conn.Read(buf)
		if err == io.EOF {
			break
		}

		if len(left_buf) > 0 {
			buf = append(left_buf, buf...)
			bsize += len(left_buf)
		}

		left_buf, cmds = parser2.Parse(buf[:bsize])
		jobs_all = append(jobs_all, cmds...)
	}
	_ = cmds

	if len(jobs_all) != job_count {
		t.Errorf("len(jobs_all) failed: %d != %d", len(jobs_all), job_count)
	} else {
		t.Logf("jobs added: %d\n\n", job_count)
		t.Logf("jobs parsed: %d\n\n", len(jobs_all))
	}
}
