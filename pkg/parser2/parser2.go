package parser2

import (
	"bytes"
	"gearmanx/pkg/command"
)

func Parse(raw []byte, part *bytes.Buffer) (cmds []*command.Command) {
	for {
		if len(raw) < 12 {
			part.Write(raw)
			break
		}
		if part.Len() > 0 {
			part.Write(raw)

			c, err := command.Decode(part.Bytes())
			if err != nil {
				break
			}
			part.Reset()
			cmds = append(cmds, c)
			break
		} else {
			c, err := command.Decode(raw)
			if err != nil {
				part.Write(raw)
				break
			}
			cmds = append(cmds, c)
			raw = raw[c.Size+12:]
		}
	}

	return cmds
}
