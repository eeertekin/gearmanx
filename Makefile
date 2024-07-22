gearmandx:
	@builder -o gearmandx main.go production.go
	@rm gearmandx

cue-observer:
	@builder -o cue-observer cli/observer/main.go cli/observer/production.go
	@rm cue-observer