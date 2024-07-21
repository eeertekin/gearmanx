package command_test

import (
	"bytes"
	"gearmanx/pkg/command"
	"gearmanx/pkg/consts"
	"testing"
)

func TestCommandBytes(t *testing.T) {
	static_req := []byte("\x00REQ\x00\x00\x00\x01\x00\x00\x00\x07reverse")

	c := command.Command{
		Type: consts.REQUEST,
		Task: consts.CAN_DO,
		Data: []byte("reverse"),
	}
	cmd_req := c.Bytes()

	if !bytes.Equal(static_req, cmd_req) {
		t.Errorf("static vs cmd failed: %x != %x", static_req, cmd_req)
	}
}

func TestCommandRequestBytes(t *testing.T) {
	static_req := []byte("\x00REQ\x00\x00\x00\x01\x00\x00\x00\x07reverse")

	c := command.Request(consts.CAN_DO, []byte("reverse"))

	if !bytes.Equal(static_req, c) {
		t.Errorf("static vs cmd failed: %x != %x", static_req, c)
	}
}

func TestCommandResponseBytes(t *testing.T) {
	static_req := []byte("\x00RES\x00\x00\x00\x0a\x00\x00\x00\x00")

	c := command.Response(consts.NO_JOB, nil)

	if !bytes.Equal(static_req, c) {
		t.Errorf("static vs cmd failed: %v != %v", static_req, c)
	}
}

func TestNewByteWithData(t *testing.T) {
	test_fn_name := "test_job"
	test_job_id := "ID-123"
	test_payload := "job-data-123"

	test_data := command.Request(
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

func TestNewByteWithDataResponse(t *testing.T) {
	test_handler := "x:h:123"
	test_payload := "job-data-123"

	test_data := command.Response(
		consts.WORK_COMPLETE,

		[]byte(test_handler), consts.NULLTERM,
		[]byte(test_payload),
	)

	cmd := command.Parse(test_data)

	if cmd.Type != consts.RESPONSE {
		t.Errorf("\nType %d wanted, got %d", consts.RESPONSE, cmd.Type)
	}

	if cmd.Task != consts.WORK_COMPLETE {
		t.Errorf("\nTask %d wanted, got %d", consts.WORK_COMPLETE, cmd.Task)
	}

	if cmd.Size != 20 {
		t.Errorf("\nSize %d wanted, got %d", 20, cmd.Size)
	}

	ID, Payload := cmd.ParseResult()

	if !bytes.Equal(ID, []byte(test_handler)) {
		t.Errorf("\nJobID %d wanted, got %d", 28, cmd.Size)
	}

	if !bytes.Equal(Payload, []byte(test_payload)) {
		t.Errorf("\nPayload %v wanted,\n    got %v", []byte(test_payload), Payload)
	}

}
