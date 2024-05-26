package parser

import (
	"bytes"
	"encoding/binary"
	"gearmanx/pkg/command"
	"gearmanx/pkg/consts"
)

func Parse(buf []byte, bsize int, fragmented_buf *bytes.Buffer) []*command.Command {
	if bytes.HasPrefix(buf, consts.NULLTERM) {
		size := int(binary.BigEndian.Uint32(buf[8:12]))
		if cap(buf) >= size+12 {
			return ParseCommands(buf[0:bsize])
		}

		*fragmented_buf = *bytes.NewBuffer(make([]byte, 0, size+12))
		fragmented_buf.Write(buf)
		// fmt.Printf("Possible fragmented command %d > %d setting cap to %d => %d \n", size, cap(buf), fragmented_buf.Len(), fragmented_buf.Cap())
		return []*command.Command{}
	}

	commands := []*command.Command{}

	if fragmented_buf.Len() > 0 {
		if fragmented_buf.Available() >= bsize {
			fragmented_buf.Write(buf[0:bsize])
		} else {
			available := fragmented_buf.Available()
			fragmented_buf.Write(buf[0:available])

			commands = append(commands, command.Parse(buf[available:bsize]))
		}
	}

	if fragmented_buf.Available() == 0 {
		cmd := command.Parse(fragmented_buf.Bytes())
		fragmented_buf.Reset()
		if len(commands) > 0 {
			commands = append([]*command.Command{cmd}, commands...)
		} else {
			commands = append(commands, cmd)
		}
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
