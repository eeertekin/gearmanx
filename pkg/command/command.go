package command

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"gearmanx/pkg/consts"
	"os"
)

type Command struct {
	Type int
	Task int
	Size int
	Data []byte
}

func Response(task int, args ...[]byte) (arr []byte) {
	return createByteArray(consts.RESPONSE, task, args...)
}

func Request(task int, args ...[]byte) (arr []byte) {
	return createByteArray(consts.REQUEST, task, args...)
}

func createByteArray(op_type int, task int, args ...[]byte) (arr []byte) {
	if op_type == consts.REQUEST {
		arr = []byte("\x00REQ")
	} else {
		arr = []byte("\x00RES")
	}
	arr = append(arr, toByteArray(task)...)

	size := 0
	for i := range args {
		size += len(args[i])
	}
	arr = append(arr, toByteArray(size)...)

	for i := range args {
		arr = append(arr, []byte(args[i])...)
	}

	return arr
}

func Parse(raw []byte) *Command {
	if len(raw) == 0 {
		return nil
	}
	c := Command{}
	c.Parse(raw)

	return &c
}

func ParseHeader(raw []byte) *Command {
	c := &Command{}
	if !bytes.HasPrefix(raw, consts.NULLTERM) {
		fmt.Printf("raw: %s\n", raw)
		os.Exit(1)
		// panic("not a command")
		c.Type = consts.REQUEST
		c.Task = consts.NOOP
		return c
	}

	if bytes.Equal(raw[0:4], consts.REQ) {
		c.Type = consts.REQUEST
	} else if bytes.Equal(raw[0:4], consts.RES) {
		c.Type = consts.RESPONSE
	}

	c.Task = int(binary.BigEndian.Uint32(raw[4:8]))
	c.Size = int(binary.BigEndian.Uint32(raw[8:12]))

	return c
}

func (c *Command) Parse(raw []byte) []byte {
	if !bytes.HasPrefix(raw, consts.NULLTERM) {
		// panic("not a command")
		c.Type = consts.REQUEST
		c.Task = consts.NOOP
		return []byte{}
	}

	if bytes.Equal(raw[0:4], consts.REQ) {
		c.Type = consts.REQUEST
	} else if bytes.Equal(raw[0:4], consts.RES) {
		c.Type = consts.RESPONSE
	}

	c.Task = int(binary.BigEndian.Uint32(raw[4:8]))
	c.Size = int(binary.BigEndian.Uint32(raw[8:12]))
	// if cap(raw) < c.Size, it is partial request, we need more from socket
	c.Data = raw[12 : 12+c.Size]

	return raw[12+c.Size:]
}

func (c *Command) ParsePayload() (ID []byte, Fn string, Payload []byte) {
	tmp := bytes.SplitN(c.Data, consts.NULLTERM, 3)
	if len(tmp) != 3 {
		return
	}

	ID, Fn, Payload = tmp[1], string(tmp[0]), tmp[2]

	return ID, Fn, Payload
}

func (c *Command) ParseResult() (ID []byte, Payload []byte) {
	tmp := bytes.SplitN(c.Data, consts.NULLTERM, 2)
	if len(tmp) != 2 {
		return
	}
	return tmp[0], tmp[1]
}

func toByteArray(i int) []byte {
	arr := make([]byte, 4)

	binary.BigEndian.PutUint32(arr, uint32(i))
	return arr
}

func (c *Command) Bytes() []byte {
	return createByteArray(c.Type, c.Task, c.Data)
}
