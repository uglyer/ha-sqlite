internal.pb.go: internal.proto
	protoc internal.proto --go_out=plugins=grpc:. --go_opt=paths=source_relative

service.pb.go: service.proto
	protoc service.proto --go_out=plugins=grpc:. --go_opt=paths=source_relative

force:
	rm -f internal.pb.go
	make internal.pb.go
	rm -f service.pb.go
	make service.pb.go
