package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"gearmanx/pkg/admin"
	"gearmanx/pkg/command"
	"gearmanx/pkg/consts"
	"gearmanx/pkg/daemon"
	"gearmanx/pkg/http"
	"gearmanx/pkg/jobs"
	"gearmanx/pkg/models"
	"gearmanx/pkg/utils"
	"gearmanx/pkg/workers"

	"io"
	"net"
)

var exception_res = []byte{}
var exception_req = []byte{}

func ParseCommands(raw []byte) (commands []*command.Command) {
	com := command.Command{}
	raw = com.Parse(raw)
	commands = append(commands, &com)
	if len(raw) != 0 {
		commands = append(commands, ParseCommands(raw)...)
	}

	return commands
}

func init() {
	exception_req = command.NewByteWithData(consts.REQUEST, consts.OPTION_REQ, []byte("exceptions"))
	exception_res = command.NewByteWithData(consts.RESPONSE, consts.OPTION_RES, []byte("exceptions"))
}

func main() {
	// debug.SetGCPercent(-1)
	// debug.SetMemoryLimit(512 * 1024 * 1024)
	go http.Serve()

	listen_port := flag.Int("p", 4730, "port")
	flag.Parse()

	daemon.ListenAndServe(
		fmt.Sprintf(":%d", *listen_port),
		Serve,
	)
}

type IAM struct {
	Type      string // worker, client
	ID        string
	Functions []string
}

func Serve(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, 1024)
	var err error
	var bsize int

	iam := IAM{
		Type: "CLIENT",
	}

	fragmented_buf := bytes.Buffer{}
	next_package_at := 0

	for {
		bsize, err = conn.Read(buf)
		if err == io.EOF {
			break
		}

		if err != nil {
			fmt.Printf("err> %s\n", err)
			continue
		}

		if bytes.Equal(buf[0:bsize], exception_req) {
			conn.Write(exception_res)
			continue
		}

		if isAdminOperation(conn, buf) {
			continue
		}

		commands := []*command.Command{}

		if bytes.HasPrefix(buf, consts.NULLTERM) {
			size := int(binary.BigEndian.Uint32(buf[8:12]))
			next_package_at = size + 12
			if size >= cap(buf) {
				fmt.Printf("Possible fragmented command %d > %d setting cap to %d \n", size, cap(buf), next_package_at)
				fragmented_buf.Write(buf)
			}
		} else if fragmented_buf.Len() > 0 {
			if bsize > next_package_at-fragmented_buf.Len() {
				next_sum := next_package_at - fragmented_buf.Len()
				fragmented_buf.Write(buf[0:next_sum])

				left_cmd := command.Command{}
				left_cmd.Parse(buf[next_sum:bsize])
				commands = append(commands, &left_cmd)
			} else {
				fragmented_buf.Write(buf[0:bsize])
			}
		}

		if fragmented_buf.Len() > 0 && next_package_at == fragmented_buf.Len() {
			fmt.Printf("Fragment size : %d\n", fragmented_buf.Len())
			next_package_at = -1
			cmd := command.Command{}
			cmd.Parse(fragmented_buf.Bytes())
			fragmented_buf.Reset()
			commands = append([]*command.Command{&cmd}, commands...)

		} else if fragmented_buf.Len() == 0 {
			commands = ParseCommands(buf[0:bsize])

		}

		// To disable parse packages, open it
		// commands := ParseCommands(buf[0:bsize])
		for i := range commands {
			HandleCommand(conn, &iam, commands[i])
		}
	}

	if iam.Type == "WORKER" {
		for i := range iam.Functions {
			workers.Unregister(iam.Functions[i], []byte(iam.ID))
		}
	}

	// fmt.Printf("Connection closed %s\n", conn.RemoteAddr())
}

func HandleCommand(conn net.Conn, iam *IAM, cmd *command.Command) {

	switch cmd.Task {
	case consts.SUBMIT_JOB_HIGH_BG, consts.SUBMIT_JOB_LOW_BG, consts.SUBMIT_JOB_BG:
		handler := utils.NextHandlerID()

		conn.Write(command.NewByteWithData(
			consts.RESPONSE,
			consts.JOB_CREATED,
			handler,
		))

		ID, Fn, Payload := cmd.ParsePayload()
		jobs.Add(&models.Job{
			Func:    Fn,
			ID:      ID,
			Payload: Payload,
		})

		workers.WakeUpAll(Fn)

		break

	case consts.SUBMIT_JOB, consts.SUBMIT_JOB_HIGH, consts.SUBMIT_JOB_LOW:
		handler := utils.NextHandlerID()

		conn.Write(command.NewByteWithData(
			consts.RESPONSE,
			consts.JOB_CREATED,
			handler,
		))

		_, Fn, Payload := cmd.ParsePayload()

		result := []byte(fmt.Sprintf("doNormal:: %s(%s) not available yet - use doBackground", Fn, Payload))

		conn.Write(command.NewByteWithData(
			consts.RESPONSE,
			consts.WORK_COMPLETE,
			handler,
			consts.NULLTERM,
			result,
		))
		break

	case consts.CAN_DO, consts.CAN_DO_TIMEOUT:
		// fmt.Printf("[worker] Registering for %s fn\n", cmd.Data)
		if iam.Type == "CLIENT" {
			iam.Type = "WORKER"
			iam.ID = string(utils.NextWorkerID())
		}
		iam.Functions = append(iam.Functions, string(cmd.Data))

		workers.Register(string(cmd.Data), []byte(iam.ID), conn)
		break

	case consts.RESET_ABILITIES: // TODO
		if iam.Type != "WORKER" {
			// TODO: ERR
		}
		for i := range iam.Functions {
			workers.Unregister(iam.Functions[i], []byte(iam.ID))
		}
		iam.Functions = []string{}

		break

	case consts.CANT_DO: // TODO
		if iam.Type != "WORKER" {
			// TODO: ERR
		}
		new_fns := []string{}
		for i := range iam.Functions {
			if iam.Functions[i] == string(cmd.Data) {
				workers.Unregister(string(cmd.Data), []byte(iam.ID))
			} else {
				new_fns = append(new_fns, iam.Functions[i])
			}
		}
		iam.Functions = new_fns

		break

	case consts.PRE_SLEEP:
		// fmt.Printf("[worker] Pre sleep requested\n")
		break

	case consts.GRAB_JOB, consts.GRAB_JOB_ALL:
		// fmt.Printf("[worker] Grab Job requested\n")

		// fmt.Printf("Hello IAM %s and able to do %#v\n", iam.Type, iam.Functions)

		var job *models.Job
		for _, fn := range iam.Functions {
			job = jobs.Get(fn)
			if job != nil {
				break
			}
		}

		if job == nil {
			// fmt.Printf("No job found\n")
			conn.Write(command.NewByteWithData(
				consts.RESPONSE,
				consts.NO_JOB,
			))
			return
		}

		// handler := NewHandler()
		conn.Write(command.NewByteWithData(
			consts.RESPONSE,
			consts.JOB_ASSIGN,
			job.ID, consts.NULLTERM,
			[]byte(job.Func), consts.NULLTERM,
			[]byte(job.Payload),
		))

		break

	case consts.WORK_COMPLETE:
		ID, _ := cmd.ParseResult()
		jobs.Remove(ID)

		// ID, Payload := cmd.ParseResult()
		// fmt.Printf("[worker] Work completed - Result : %s => %s\n", ID, Payload)

		break

	default:
		fmt.Printf("[unknown] %s requested ", consts.String(cmd.Task))
		if len(cmd.Data) > 0 {
			fmt.Printf("with %s", cmd.Data)
		}
		fmt.Println("")

	}
}

func isAdminOperation(conn net.Conn, buf []byte) bool {
	if bytes.HasPrefix(buf, []byte("status")) {
		admin.Status(conn)
		return true
	}

	if bytes.HasPrefix(buf, []byte("version")) {
		admin.Version(conn)
		return true
	}

	if bytes.HasPrefix(buf, []byte("shutdown")) {
		admin.Shutdown(conn)
		return true
	}

	if bytes.HasPrefix(buf, []byte("workers")) {
		admin.Workers(conn)
		return true
	}

	return false
}
