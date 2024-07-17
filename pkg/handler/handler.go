package handler

import (
	"fmt"
	"gearmanx/pkg/clients"
	"gearmanx/pkg/command"
	"gearmanx/pkg/consts"
	"gearmanx/pkg/debounce"
	"gearmanx/pkg/models"
	"gearmanx/pkg/storage"
	"gearmanx/pkg/utils"
	"gearmanx/pkg/workers"
	"net"
	"time"
)

var fn_router map[int]func(conn net.Conn, iam *models.IAM, cmd *command.Command)

var job_debounce_map map[string]func(f func())

func job_debounce(fn string, f func()) {
	if _, ok := job_debounce_map[fn]; !ok {
		job_debounce_map[fn] = debounce.New(200 * time.Millisecond)
	}
	job_debounce_map[fn](f)
}

func init() {
	job_debounce_map = make(map[string]func(f func()))

	fn_router = map[int]func(conn net.Conn, iam *models.IAM, cmd *command.Command){
		// General
		consts.ECHO_REQ:   EchoReq,
		consts.OPTION_REQ: OptionReq,

		// Worker
		consts.CAN_DO:          CanDo,
		consts.CAN_DO_TIMEOUT:  CanDo,
		consts.RESET_ABILITIES: ResetAbilities,
		consts.CANT_DO:         CanNotDo,
		consts.PRE_SLEEP:       PreSleep,
		consts.GRAB_JOB:        GrabJob,
		consts.GRAB_JOB_ALL:    GrabJob,
		consts.GRAB_JOB_UNIQ:   GrabJobUnique,
		consts.WORK_COMPLETE:   WorkComplete,
		consts.WORK_FAIL:       WorkFailed,
		consts.WORK_EXCEPTION:  WorkComplete,

		// Client
		consts.SUBMIT_JOB:      SubmitJob,
		consts.SUBMIT_JOB_HIGH: SubmitJob,
		consts.SUBMIT_JOB_LOW:  SubmitJob,

		consts.SUBMIT_JOB_HIGH_BG: SubmitJobBg,
		consts.SUBMIT_JOB_LOW_BG:  SubmitJobBg,
		consts.SUBMIT_JOB_BG:      SubmitJobBg,
	}
}

func Run(conn net.Conn, iam *models.IAM, cmd *command.Command) bool {
	if _, ok := fn_router[cmd.Task]; !ok {
		// TODO: Parse error?
		if consts.String(cmd.Task) == "" {
			return false
		}
		fmt.Printf("[unknown] %s requested with (%s)\n", consts.String(cmd.Task), cmd.Data)
		return false
	}

	fn_router[cmd.Task](conn, iam, cmd)
	return true
}

func EchoReq(conn net.Conn, iam *models.IAM, cmd *command.Command) {
	conn.Write(command.Response(
		consts.ECHO_RES,
		cmd.Data,
	))
}

func OptionReq(conn net.Conn, iam *models.IAM, cmd *command.Command) {
	conn.Write(command.Response(
		consts.OPTION_RES,
		cmd.Data,
	))
}

func CanDo(conn net.Conn, iam *models.IAM, cmd *command.Command) {
	// fmt.Printf("[worker] Registering for %s fn\n", cmd.Data)
	if iam.Role == consts.ROLE_CLIENT {
		iam.Role = consts.ROLE_WORKER
		clients.Unregister(iam)
	}
	iam.Functions = append(iam.Functions, string(cmd.Data))

	workers.Register(string(cmd.Data), []byte(iam.ID), conn)
}

func ResetAbilities(conn net.Conn, iam *models.IAM, cmd *command.Command) {
	if iam.Role != consts.ROLE_WORKER {
		conn.Write(command.Response(
			consts.ERROR,
			[]byte("not_available"), consts.NULLTERM,
			[]byte("worker method requested"),
		))
		return
	}
	for i := range iam.Functions {
		workers.Unregister(iam.Functions[i], []byte(iam.ID))
	}
	iam.Functions = []string{}
}

func CanNotDo(conn net.Conn, iam *models.IAM, cmd *command.Command) {
	if iam.Role != consts.ROLE_WORKER {
		conn.Write(command.Response(
			consts.ERROR,
			[]byte("not_available"), consts.NULLTERM,
			[]byte("worker method requested"),
		))
		return
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

}

func PreSleep(conn net.Conn, iam *models.IAM, cmd *command.Command) {
	workers.Sleep(iam.ID)
	// fmt.Printf("[worker] Pre sleep requested\n")
}

func SubmitJob(conn net.Conn, iam *models.IAM, cmd *command.Command) {
	handlerID := utils.NextHandlerID()

	conn.Write(command.Response(
		consts.JOB_CREATED,
		handlerID,
	))

	ID, Fn, Payload := cmd.ParsePayload()

	storage.AddJob(&models.Job{
		Func:    Fn,
		ID:      ID,
		Payload: Payload,
	})
	go storage.WakeUpAll(Fn)

	result := storage.WaitJob(ID)

	conn.Write(command.Response(
		consts.WORK_COMPLETE,
		handlerID,
		consts.NULLTERM,
		result,
	))

}

func WorkComplete(conn net.Conn, iam *models.IAM, cmd *command.Command) {
	ID, payload := cmd.ParseResult()
	storage.DeleteJob(ID)

	storage.UnassignJobFromWorker(iam.ID, string(ID), "all")
	// fmt.Printf("[worker] Work completed - Result : %s => %s\n", ID, Payload)

	storage.JobResult(ID, payload)
}

func GrabJob(conn net.Conn, iam *models.IAM, cmd *command.Command) {
	// fmt.Printf("[worker] Grab Job requested\n")
	// fmt.Printf("Hello IAM %s and able to do %#v\n", iam.Type, iam.Functions)

	var job *models.Job
	for _, fn := range iam.Functions {
		job = storage.GetJob(fn)
		if job != nil {
			break
		}
	}

	if job == nil {
		// fmt.Printf("No job found\n")
		conn.Write(command.Response(
			consts.NO_JOB,
		))
		return
	}

	conn.Write(command.Response(
		consts.JOB_ASSIGN,
		job.ID, consts.NULLTERM,
		[]byte(job.Func), consts.NULLTERM,
		[]byte(job.Payload),
	))

	storage.AssignJobToWorker(iam.ID, string(job.ID), job.Func)
}

func GrabJobUnique(conn net.Conn, iam *models.IAM, cmd *command.Command) {
	// fmt.Printf("[worker] Grab Job requested\n")
	// fmt.Printf("Hello IAM %s and able to do %#v\n", iam.Type, iam.Functions)

	var job *models.Job
	for _, fn := range iam.Functions {
		job = storage.GetJob(fn)
		if job != nil {
			break
		}
	}

	if job == nil {
		// fmt.Printf("No job found\n")
		conn.Write(command.Response(
			consts.NO_JOB,
		))
		return
	}

	conn.Write(command.Response(
		consts.JOB_ASSIGN_UNIQ,
		job.ID, consts.NULLTERM,
		[]byte(job.Func), consts.NULLTERM,
		[]byte(job.ID), consts.NULLTERM,
		[]byte(job.Payload),
	))

	storage.AssignJobToWorker(iam.ID, string(job.ID), job.Func)
}

func SubmitJobBg(conn net.Conn, iam *models.IAM, cmd *command.Command) {
	handlerID := utils.NextHandlerID()

	conn.Write(command.Response(
		consts.JOB_CREATED,
		handlerID,
	))

	ID, Fn, Payload := cmd.ParsePayload()
	storage.AddJob(&models.Job{
		Func:    Fn,
		ID:      ID,
		Payload: Payload,
	})

	job_debounce(Fn, func() {
		storage.WakeUpAll(Fn)
	})
}

func WorkException(conn net.Conn, iam *models.IAM, cmd *command.Command) {
	ID, payload := cmd.ParseResult()
	fmt.Printf("[worker] Work Exception - Result : %s => %s\n", ID, payload)

	storage.DeleteJob(cmd.Data)
	storage.UnassignJobFromWorker(iam.ID, string(cmd.Data), "all")

	storage.JobResult(cmd.Data, []byte("gearman: exception"))
}

func WorkFailed(conn net.Conn, iam *models.IAM, cmd *command.Command) {
	ID := cmd.Data
	fmt.Printf("[worker] Work failed - Result : %s => %s\n", ID, "failed")

	storage.DeleteJob(ID)
	storage.UnassignJobFromWorker(iam.ID, string(ID), "all")
	storage.JobResult(ID, []byte{})
}
