package handler

import (
	"fmt"
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

var debounced func(f func())
var fn_router map[int]func(conn net.Conn, iam *models.IAM, cmd *command.Command)

func init() {
	debounced = debounce.New(100 * time.Millisecond)

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
		consts.WORK_COMPLETE:   WorkComplete,
		consts.GRAB_JOB:        GrabJob,
		consts.GRAB_JOB_ALL:    GrabJob,

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
		fmt.Printf("[unknown] %s requested with (%s)\n", consts.String(cmd.Task), cmd.Data)
		return false
	}

	fn_router[cmd.Task](conn, iam, cmd)
	return true
}

func EchoReq(conn net.Conn, iam *models.IAM, cmd *command.Command) {
	conn.Write(command.NewByteWithData(
		consts.RESPONSE,
		consts.ECHO_RES,
		cmd.Data,
	))
}

func OptionReq(conn net.Conn, iam *models.IAM, cmd *command.Command) {
	conn.Write(command.NewByteWithData(
		consts.RESPONSE,
		consts.OPTION_RES,
		cmd.Data,
	))
}

func CanDo(conn net.Conn, iam *models.IAM, cmd *command.Command) {
	// fmt.Printf("[worker] Registering for %s fn\n", cmd.Data)
	if iam.Role == consts.ROLE_CLIENT {
		iam.Role = consts.ROLE_WORKER
		iam.ID = utils.NextWorkerID()
	}
	iam.Functions = append(iam.Functions, string(cmd.Data))

	workers.Register(string(cmd.Data), []byte(iam.ID), conn)
}

func ResetAbilities(conn net.Conn, iam *models.IAM, cmd *command.Command) {
	if iam.Role != consts.ROLE_WORKER {
		conn.Write(command.NewByteWithData(
			consts.RESPONSE,
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
		conn.Write(command.NewByteWithData(
			consts.RESPONSE,
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
	// fmt.Printf("[worker] Pre sleep requested\n")
}

func SubmitJob(conn net.Conn, iam *models.IAM, cmd *command.Command) {
	handlerID := utils.NextHandlerID()

	conn.Write(command.NewByteWithData(
		consts.RESPONSE,
		consts.JOB_CREATED,
		handlerID,
	))

	_, Fn, Payload := cmd.ParsePayload()

	result := []byte(fmt.Sprintf("doNormal:: %s(%s) not available yet - use doBackground", Fn, Payload))

	conn.Write(command.NewByteWithData(
		consts.RESPONSE,
		consts.WORK_COMPLETE,
		handlerID,
		consts.NULLTERM,
		result,
	))

}

func WorkComplete(conn net.Conn, iam *models.IAM, cmd *command.Command) {
	ID, _ := cmd.ParseResult()
	storage.DeleteJob(ID)

	storage.UnassignJobFromWorker(iam.ID, string(ID), "all")

	// ID, Payload := cmd.ParseResult()
	// fmt.Printf("[worker] Work completed - Result : %s => %s\n", ID, Payload)
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
		conn.Write(command.NewByteWithData(
			consts.RESPONSE,
			consts.NO_JOB,
		))
		return
	}

	conn.Write(command.NewByteWithData(
		consts.RESPONSE,
		consts.JOB_ASSIGN,
		job.ID, consts.NULLTERM,
		[]byte(job.Func), consts.NULLTERM,
		[]byte(job.Payload),
	))

	storage.AssignJobToWorker(iam.ID, string(job.ID), job.Func)
}

func SubmitJobBg(conn net.Conn, iam *models.IAM, cmd *command.Command) {
	handlerID := utils.NextHandlerID()

	conn.Write(command.NewByteWithData(
		consts.RESPONSE,
		consts.JOB_CREATED,
		handlerID,
	))

	ID, Fn, Payload := cmd.ParsePayload()
	storage.AddJob(&models.Job{
		Func:    Fn,
		ID:      ID,
		Payload: Payload,
	})

	debounced(func() {
		workers.WakeUpAll(Fn)
	})
}
