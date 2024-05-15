package command

// func NewResponse(task int) *Command {
// 	return &Command{
// 		Type: consts.RESPONSE,
// 		Task: task,
// 	}
// }

// func NewRequest(task int) *Command {
// 	return &Command{
// 		Type: consts.REQUEST,
// 		Task: task,
// 	}
// }

// func (c *Command) Bytes() []byte {
// 	arr := []byte("\x00")
// 	if c.Type == consts.REQUEST {
// 		arr = append(arr, "REQ"...)
// 	} else if c.Type == consts.RESPONSE {
// 		arr = append(arr, "RES"...)
// 	}
// 	arr = append(arr, toByteArray(int32(c.Task))...)
// 	arr = append(arr, toByteArray(int32(len(c.Data)))...)
// 	arr = append(arr, []byte(c.Data)...)
// 	return arr
// }

// func (c *Command) Write(data []byte) {
// 	c.Data = append(c.Data, data...)
// }
