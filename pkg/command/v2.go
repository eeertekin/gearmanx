package command

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"gearmanx/pkg/consts"
)

func Decode(raw []byte) (*Command, error) {
	c := Command{
		Type: consts.NOOP,
	}
	if bytes.Equal(raw[0:4], consts.REQ) {
		c.Type = consts.REQUEST
	} else if bytes.Equal(raw[0:4], consts.RES) {
		c.Type = consts.RESPONSE
	}

	c.Task = int(binary.BigEndian.Uint32(raw[4:8]))
	c.Size = int(binary.BigEndian.Uint32(raw[8:12]))
	if len(raw) < 12+c.Size {
		return &c, errors.New("not a package 2")
	}
	c.Data = raw[12 : 12+c.Size]

	return &c, nil
}

func (c *Command) Bytes2() (res []byte) {
	if c.Type == consts.REQUEST {
		res = append(res, consts.REQ...)
	} else if c.Type == consts.RESPONSE {
		res = append(res, consts.RES...)
	} else {
		fmt.Printf("WTF\n")
	}

	arr := make([]byte, 4)

	binary.BigEndian.PutUint32(arr, uint32(c.Task))
	res = append(res, arr...)

	binary.BigEndian.PutUint32(arr, uint32(len(c.Data)))
	res = append(res, arr...)

	res = append(res, c.Data...)

	return res
}
