generate:
	rm -f protocol/protocol.go
	flatc --go --gen-onefile --go-namespace protocol -o protocol protocol/protocol.fbs
	mv protocol/protocol_generated.go protocol/protocol.go
