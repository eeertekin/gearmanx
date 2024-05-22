package main

import (
	"bytes"
	"gearmanx/pkg/command"
	"gearmanx/pkg/consts"
	"testing"
)

func TestNewByteWithData(t *testing.T) {
	test_fn_name := "test_job"
	test_job_id := "ID-123"
	test_payload := "job-data-123"

	test_data := command.NewByteWithData(
		consts.REQUEST,
		consts.SUBMIT_JOB,

		[]byte(test_fn_name), consts.NULLTERM,
		[]byte(test_job_id), consts.NULLTERM,
		[]byte(test_payload),
	)

	cmd := command.Parse(test_data)

	if cmd.Type != consts.REQUEST {
		t.Errorf("\nType %d wanted, got %d", consts.REQUEST, cmd.Type)
	}

	if cmd.Task != consts.SUBMIT_JOB {
		t.Errorf("\nTask %d wanted, got %d", consts.SUBMIT_JOB, cmd.Task)
	}

	if cmd.Size != 28 {
		t.Errorf("\nSize %d wanted, got %d", 28, cmd.Size)
	}

	ID, Fn, Payload := cmd.ParsePayload()

	if !bytes.Equal(ID, []byte(test_job_id)) {
		t.Errorf("\nJobID %d wanted, got %d", 28, cmd.Size)
	}

	if Fn != test_fn_name {
		t.Errorf("\nFunc %s wanted, got %s", test_fn_name, Fn)
	}

	if !bytes.Equal(Payload, []byte(test_payload)) {
		t.Errorf("\nPayload %v wanted,\n    got %v", []byte(test_payload), Payload)
	}

}
