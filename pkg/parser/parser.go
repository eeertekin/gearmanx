package parser

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"gearmanx/pkg/command"
	"gearmanx/pkg/consts"
)

var (
	fragmented_buf  = bytes.Buffer{}
	next_package_at = 0
)

func Parse(buf []byte, bsize int) []*command.Command {
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

	return commands
}

func ParseCommands(raw []byte) (commands []*command.Command) {
	com := command.Command{}
	raw = com.Parse(raw)
	commands = append(commands, &com)
	if len(raw) != 0 {
		commands = append(commands, ParseCommands(raw)...)
	}

	return commands
}
