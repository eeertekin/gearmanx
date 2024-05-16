package consts

var (
	REQUEST  = 100
	RESPONSE = 101

	CAN_DO                       = 1  // Worker
	CANT_DO                      = 2  // Worker
	RESET_ABILITIES              = 3  //  REQ    Worker
	PRE_SLEEP                    = 4  //  REQ    Worker
	NOOP                         = 6  //  RES    Worker
	SUBMIT_JOB                   = 7  //  REQ    Client
	JOB_CREATED                  = 8  //  RES    Client
	GRAB_JOB                     = 9  //  REQ    Worker
	NO_JOB                       = 10 //  RES    Worker
	JOB_ASSIGN                   = 11 //  RES    Worker
	WORK_STATUS                  = 12 //  REQ    Worker/Client
	WORK_COMPLETE                = 13 //  REQ    Worker/Client
	WORK_FAIL                    = 14 //  REQ    Worker/Client
	GET_STATUS                   = 15 //  REQ    Client
	ECHO_REQ                     = 16 //  REQ    Client/Worker
	ECHO_RES                     = 17 //  RES    Client/Worker
	SUBMIT_JOB_BG                = 18 //  REQ    Client
	ERROR                        = 19 //  RES    Client/Worker
	STATUS_RES                   = 20 //  RES    Client
	SUBMIT_JOB_HIGH              = 21 //  REQ    Client
	SET_CLIENT_ID                = 22 //  REQ    Worker
	CAN_DO_TIMEOUT               = 23 //  REQ    Worker
	ALL_YOURS                    = 24 //  REQ    Worker
	WORK_EXCEPTION               = 25 //  REQ    Worker/Client
	OPTION_REQ                   = 26 //  REQ    Client/Worker
	OPTION_RES                   = 27 //  RES    Client/Worker
	WORK_DATA                    = 28 //  REQ    Worker/Client
	WORK_WARNING                 = 29 //  REQ    Worker/Client
	GRAB_JOB_UNIQ                = 30 //  REQ    Worker
	JOB_ASSIGN_UNIQ              = 31 //  RES    Worker
	SUBMIT_JOB_HIGH_BG           = 32 //  REQ    Client
	SUBMIT_JOB_LOW               = 33 //  REQ    Client
	SUBMIT_JOB_LOW_BG            = 34 //  REQ    Client
	SUBMIT_JOB_SCHED             = 35 //  REQ    Client
	SUBMIT_JOB_EPOCH             = 36 //  REQ    Client
	SUBMIT_REDUCE_JOB            = 37 //  REQ    Client
	SUBMIT_REDUCE_JOB_BACKGROUND = 38 // REQ    Client
	GRAB_JOB_ALL                 = 39 //  REQ    Worker
	JOB_ASSIGN_ALL               = 40 //  RES    Worker
	GET_STATUS_UNIQUE            = 41 //  REQ    Client
	STATUS_RES_UNIQUE            = 42 //  RES    Client

	NULLTERM = []byte("\x00")
)

var str_map = map[int]string{
	1:  "CAN_DO",
	2:  "CANT_DO",
	3:  "RESET_ABILITIES",
	4:  "PRE_SLEEP",
	6:  "NOOP",
	7:  "SUBMIT_JOB",
	8:  "JOB_CREATED",
	9:  "GRAB_JOB",
	10: "NO_JOB",
	11: "JOB_ASSIGN",
	12: "WORK_STATUS",
	13: "WORK_COMPLETE",
	14: "WORK_FAIL",
	15: "GET_STATUS",
	16: "ECHO_REQ",
	17: "ECHO_RES",
	18: "SUBMIT_JOB_BG",
	19: "ERROR",
	20: "STATUS_RES",
	21: "SUBMIT_JOB_HIGH",
	22: "SET_CLIENT_ID",
	23: "CAN_DO_TIMEOUT",
	24: "ALL_YOURS",
	25: "WORK_EXCEPTION",
	26: "OPTION_REQ",
	27: "OPTION_RES",
	28: "WORK_DATA",
	29: "WORK_WARNING",
	30: "GRAB_JOB_UNIQ",
	31: "JOB_ASSIGN_UNIQ",
	32: "SUBMIT_JOB_HIGH_BG",
	33: "SUBMIT_JOB_LOW",
	34: "SUBMIT_JOB_LOW_BG",
	35: "SUBMIT_JOB_SCHED",
	36: "SUBMIT_JOB_EPOCH",
	37: "SUBMIT_REDUCE_JOB",
	38: "SUBMIT_REDUCE_JOB_BACKGROUND",
	39: "GRAB_JOB_ALL",
	40: "JOB_ASSIGN_ALL",
	41: "GET_STATUS_UNIQUE",
	42: "STATUS_RES_UNIQUE",
}

func String(id int) string {
	return str_map[id]
}
