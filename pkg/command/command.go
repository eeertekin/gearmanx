package command

import (
	"bytes"
	"encoding/binary"
	"gearmanx/pkg/consts"
)

type Command struct {
	Type int
	Task int
	Size int
	Data []byte
}

func NewByteWithData(op_type int, task int, args ...[]byte) (arr []byte) {
	if op_type == consts.REQUEST {
		arr = []byte("\x00REQ")
	} else {
		arr = []byte("\x00RES")
	}
	arr = append(arr, toByteArray(int32(task))...)

	size := 0
	for i := range args {
		size += len(args[i])
	}
	arr = append(arr, toByteArray(int32(size))...)

	for i := range args {
		arr = append(arr, []byte(args[i])...)
	}

	return arr
}

func (c *Command) Parse(raw []byte) []byte {
	if !bytes.HasPrefix(raw, []byte("\x00")) {
		panic("not a command")
	}

	if bytes.Equal(raw[1:4], []byte("REQ")) {
		c.Type = consts.REQUEST
	} else if bytes.Equal(raw[1:4], []byte("RES")) {
		c.Type = consts.RESPONSE
	}

	c.Task = int(binary.BigEndian.Uint32(raw[4:8]))
	c.Size = int(binary.BigEndian.Uint32(raw[8:12]))
	// if cap(raw) < c.Size, it is partial request, we need more from socket
	c.Data = raw[12 : 12+c.Size]

	return raw[12+c.Size:]
}

func (c *Command) ParsePayload() (ID []byte, Fn string, Payload []byte) {
	tmp := bytes.Split([]byte(c.Data), []byte("\x00"))
	if len(tmp) != 3 {
		return
	}

	ID, Fn, Payload = tmp[1], string(tmp[0]), tmp[2]

	return ID, Fn, Payload
}

func (c *Command) ParseResult() (ID []byte, Payload []byte) {
	tmp := bytes.Split([]byte(c.Data), []byte("\x00"))
	if len(tmp) != 2 {
		return
	}
	return tmp[0], tmp[1]
}

func toByteArray(i int32) []byte {
	arr := make([]byte, 4)

	binary.BigEndian.PutUint32(arr, uint32(i))
	return arr
}
