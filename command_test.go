package main

// func TestNewByteWithDataResponseNOOP(t *testing.T) {
// 	test_handler := "x:h:123"
// 	test_payload := "job-data-123"

// 	test_data := command.Response(
// 		consts.WORK_COMPLETE,
// 		[]byte(test_handler), consts.NULLTERM,
// 		[]byte(test_payload),
// 	)
// 	test_data = test_data[0:3]

// 	test_data = append(test_data, command.Response(
// 		consts.WORK_COMPLETE,
// 		[]byte(test_handler), consts.NULLTERM,
// 		[]byte(test_payload),
// 	)...)

// 	test_data = append(test_data, command.Request(
// 		consts.WORK_COMPLETE,
// 		[]byte(test_handler), consts.NULLTERM,
// 		[]byte(test_payload),
// 	)...)

// 	test_data = append(test_data, command.Response(
// 		consts.WORK_COMPLETE,
// 		[]byte(test_handler), consts.NULLTERM,
// 		[]byte(test_payload),
// 	)...)

// 	fragmented_buf := bytes.Buffer{}

// 	bsize := len(test_data)

// 	commands := parser.Parse(test_data, bsize, &fragmented_buf)
// 	fmt.Printf("commands: %v\n", commands)
// 	fmt.Printf("buf: %v\n", test_data)

// 	if commands == nil {
// 		fmt.Printf("[main] parser returned nil\n")
// 		return
// 	}

// 	for i := range commands {
// 		fmt.Printf("commands[%d] %v\n", i, commands[i])
// 	}

// 	// cmd := command.Parse(test_data)

// 	// fmt.Printf("cmd: %#v\n", cmd)

// 	// if cmd.Type != consts.RESPONSE {
// 	// 	t.Errorf("\nType %d wanted, got %d", consts.RESPONSE, cmd.Type)
// 	// }

// 	// if cmd.Task != consts.WORK_COMPLETE {
// 	// 	t.Errorf("\nTask %d wanted, got %d", consts.WORK_COMPLETE, cmd.Task)
// 	// }

// 	// if cmd.Size != 20 {
// 	// 	t.Errorf("\nSize %d wanted, got %d", 20, cmd.Size)
// 	// }

// 	// ID, Payload := cmd.ParseResult()

// 	// if !bytes.Equal(ID, []byte(test_handler)) {
// 	// 	t.Errorf("\nJobID %d wanted, got %d", 28, cmd.Size)
// 	// }

// 	// if !bytes.Equal(Payload, []byte(test_payload)) {
// 	// 	t.Errorf("\nPayload %v wanted,\n    got %v", []byte(test_payload), Payload)
// 	// }

// }
