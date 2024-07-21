package parser2

import "gearmanx/pkg/command"

func Parse(raw []byte) (part []byte, cmds []*command.Command) {
	for {
		if len(raw) < 12 {
			part = make([]byte, len(raw))
			copy(part, raw[0:])
			break
		}
		c, err := command.Decode(raw)
		if err != nil {
			part = make([]byte, len(raw))
			copy(part, raw[0:])
			break
		}
		cmds = append(cmds, c)
		raw = raw[c.Size+12:]
	}

	return part, cmds
}
