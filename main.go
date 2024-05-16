package main

import (
	"bytes"
	"flag"
	"fmt"
	"gearmanx/pkg/command"
	"gearmanx/pkg/consts"
	"gearmanx/pkg/http"
	"gearmanx/pkg/jobs"
	"gearmanx/pkg/models"
	"gearmanx/pkg/queue"
	"gearmanx/pkg/redis"
	"gearmanx/pkg/workers"

	"io"
	"net"
	"os"
	"sync/atomic"
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

	listen_port := flag.Int("p", 4730, "server")
	flag.Parse()

	fmt.Printf("# gearmanx\n")
	fmt.Printf("Listening port %d\n\n", *listen_port)

	sock, err := net.Listen("tcp", fmt.Sprintf(":%d", *listen_port))
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}

	defer sock.Close()

	for {
		var conn net.Conn

		conn, err = sock.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			continue
		}

		go Serve(conn)
	}
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

		if bytes.HasPrefix(buf[0:bsize], []byte("status")) {
			conn.Write([]byte(redis.Status()))
			continue
		}

		commands := ParseCommands(buf[0:bsize])

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

var jobID atomic.Int64

func NewHandler() []byte {
	jobID.Add(1)
	return []byte(fmt.Sprintf("H:gearmanx:%d", jobID.Load()))
}

var workerID atomic.Int64

func NewWorkerID() []byte {
	workerID.Add(1)
	return []byte(fmt.Sprintf("H:worker:%d", workerID.Load()))
}

func HandleCommand(conn net.Conn, iam *IAM, cmd *command.Command) {

	switch cmd.Task {
	case consts.SUBMIT_JOB_HIGH_BG, consts.SUBMIT_JOB_LOW_BG, consts.SUBMIT_JOB_BG:
		handler := NewHandler()
		JobCreated(conn, handler)

		ID, Fn, Payload := cmd.ParsePayload()
		jobs.Add(&models.Job{
			Func:    Fn,
			ID:      ID,
			Payload: Payload,
		})

		workers.WakeUpAll(Fn)

		// queue.Async(cmd.JobDetails.Func, cmd.JobDetails.Payload)
		break

	case consts.SUBMIT_JOB, consts.SUBMIT_JOB_HIGH, consts.SUBMIT_JOB_LOW:
		handler := NewHandler()

		JobCreated(conn, handler)

		_, Fn, Payload := cmd.ParsePayload()

		result := queue.Sync(Fn, Payload)

		WorkCompleted(conn, handler, result)
		break

	case consts.CAN_DO, consts.CAN_DO_TIMEOUT:
		// fmt.Printf("[worker] Registering for %s fn\n", cmd.Data)
		if iam.Type == "CLIENT" {
			iam.Type = "WORKER"
			iam.ID = string(NewWorkerID())
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

func JobCreated(conn net.Conn, handler []byte) {
	conn.Write(command.NewByteWithData(
		consts.RESPONSE,
		consts.JOB_CREATED,
		handler,
	))
}

func WorkCompleted(conn net.Conn, handler []byte, result []byte) {
	conn.Write(command.NewByteWithData(
		consts.RESPONSE,
		consts.WORK_COMPLETE,
		handler,
		consts.NULLTERM,
		result,
	))
}
