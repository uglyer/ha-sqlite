internal.pb.go: internal.proto
	protoc internal.proto --go_out=plugins=grpc:. --go_opt=paths=source_relative

force:
	rm -f internal.pb.go
	make internal.pb.go
