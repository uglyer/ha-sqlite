// Code generated by protoc-gen-go. DO NOT EDIT.
// source: internal.proto

package proto

import (
	context "context"
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type JoinRequest struct {
	Id                   string   `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Address              string   `protobuf:"bytes,2,opt,name=address,proto3" json:"address,omitempty"`
	PreviousIndex        uint64   `protobuf:"varint,3,opt,name=previous_index,json=previousIndex,proto3" json:"previous_index,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *JoinRequest) Reset()         { *m = JoinRequest{} }
func (m *JoinRequest) String() string { return proto.CompactTextString(m) }
func (*JoinRequest) ProtoMessage()    {}
func (*JoinRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_41f4a519b878ee3b, []int{0}
}

func (m *JoinRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_JoinRequest.Unmarshal(m, b)
}
func (m *JoinRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_JoinRequest.Marshal(b, m, deterministic)
}
func (m *JoinRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_JoinRequest.Merge(m, src)
}
func (m *JoinRequest) XXX_Size() int {
	return xxx_messageInfo_JoinRequest.Size(m)
}
func (m *JoinRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_JoinRequest.DiscardUnknown(m)
}

var xxx_messageInfo_JoinRequest proto.InternalMessageInfo

func (m *JoinRequest) GetId() string {
	if m != nil {
		return m.Id
	}
	return ""
}

func (m *JoinRequest) GetAddress() string {
	if m != nil {
		return m.Address
	}
	return ""
}

func (m *JoinRequest) GetPreviousIndex() uint64 {
	if m != nil {
		return m.PreviousIndex
	}
	return 0
}

type JoinResponse struct {
	Index                uint64   `protobuf:"varint,1,opt,name=index,proto3" json:"index,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *JoinResponse) Reset()         { *m = JoinResponse{} }
func (m *JoinResponse) String() string { return proto.CompactTextString(m) }
func (*JoinResponse) ProtoMessage()    {}
func (*JoinResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_41f4a519b878ee3b, []int{1}
}

func (m *JoinResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_JoinResponse.Unmarshal(m, b)
}
func (m *JoinResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_JoinResponse.Marshal(b, m, deterministic)
}
func (m *JoinResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_JoinResponse.Merge(m, src)
}
func (m *JoinResponse) XXX_Size() int {
	return xxx_messageInfo_JoinResponse.Size(m)
}
func (m *JoinResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_JoinResponse.DiscardUnknown(m)
}

var xxx_messageInfo_JoinResponse proto.InternalMessageInfo

func (m *JoinResponse) GetIndex() uint64 {
	if m != nil {
		return m.Index
	}
	return 0
}

func init() {
	proto.RegisterType((*JoinRequest)(nil), "JoinRequest")
	proto.RegisterType((*JoinResponse)(nil), "JoinResponse")
}

func init() { proto.RegisterFile("internal.proto", fileDescriptor_41f4a519b878ee3b) }

var fileDescriptor_41f4a519b878ee3b = []byte{
	// 211 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0xcb, 0xcc, 0x2b, 0x49,
	0x2d, 0xca, 0x4b, 0xcc, 0xd1, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x57, 0x8a, 0xe3, 0xe2, 0xf6, 0xca,
	0xcf, 0xcc, 0x0b, 0x4a, 0x2d, 0x2c, 0x4d, 0x2d, 0x2e, 0x11, 0xe2, 0xe3, 0x62, 0xca, 0x4c, 0x91,
	0x60, 0x54, 0x60, 0xd4, 0xe0, 0x0c, 0x62, 0xca, 0x4c, 0x11, 0x92, 0xe0, 0x62, 0x4f, 0x4c, 0x49,
	0x29, 0x4a, 0x2d, 0x2e, 0x96, 0x60, 0x02, 0x0b, 0xc2, 0xb8, 0x42, 0xaa, 0x5c, 0x7c, 0x05, 0x45,
	0xa9, 0x65, 0x99, 0xf9, 0xa5, 0xc5, 0xf1, 0x99, 0x79, 0x29, 0xa9, 0x15, 0x12, 0xcc, 0x0a, 0x8c,
	0x1a, 0x2c, 0x41, 0xbc, 0x30, 0x51, 0x4f, 0x90, 0xa0, 0x92, 0x0a, 0x17, 0x0f, 0xc4, 0xfc, 0xe2,
	0x82, 0xfc, 0xbc, 0xe2, 0x54, 0x21, 0x11, 0x2e, 0x56, 0x88, 0x6a, 0x46, 0xb0, 0x6a, 0x08, 0xc7,
	0xc8, 0x92, 0x4b, 0xc0, 0x23, 0x31, 0xb8, 0x30, 0x27, 0xb3, 0x24, 0xd5, 0x13, 0xea, 0x3e, 0x21,
	0x55, 0x2e, 0x16, 0x90, 0x4e, 0x21, 0x1e, 0x3d, 0x24, 0x07, 0x4a, 0xf1, 0xea, 0x21, 0x1b, 0xa7,
	0xc4, 0xe0, 0xa4, 0x1c, 0xa5, 0x98, 0x9e, 0x59, 0x92, 0x51, 0x9a, 0xa4, 0x97, 0x9c, 0x9f, 0xab,
	0x5f, 0x9a, 0x9e, 0x53, 0x99, 0x5a, 0xa4, 0x9f, 0x91, 0xa8, 0x5b, 0x0c, 0x36, 0x4d, 0x1f, 0xec,
	0xcb, 0x24, 0x36, 0x30, 0x65, 0x0c, 0x08, 0x00, 0x00, 0xff, 0xff, 0x53, 0x39, 0xd0, 0x9f, 0xfe,
	0x00, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// HaSqliteInternalClient is the client API for HaSqliteInternal service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type HaSqliteInternalClient interface {
	Join(ctx context.Context, in *JoinRequest, opts ...grpc.CallOption) (*JoinResponse, error)
}

type haSqliteInternalClient struct {
	cc *grpc.ClientConn
}

func NewHaSqliteInternalClient(cc *grpc.ClientConn) HaSqliteInternalClient {
	return &haSqliteInternalClient{cc}
}

func (c *haSqliteInternalClient) Join(ctx context.Context, in *JoinRequest, opts ...grpc.CallOption) (*JoinResponse, error) {
	out := new(JoinResponse)
	err := c.cc.Invoke(ctx, "/HaSqliteInternal/Join", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// HaSqliteInternalServer is the server API for HaSqliteInternal service.
type HaSqliteInternalServer interface {
	Join(context.Context, *JoinRequest) (*JoinResponse, error)
}

// UnimplementedHaSqliteInternalServer can be embedded to have forward compatible implementations.
type UnimplementedHaSqliteInternalServer struct {
}

func (*UnimplementedHaSqliteInternalServer) Join(ctx context.Context, req *JoinRequest) (*JoinResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Join not implemented")
}

func RegisterHaSqliteInternalServer(s *grpc.Server, srv HaSqliteInternalServer) {
	s.RegisterService(&_HaSqliteInternal_serviceDesc, srv)
}

func _HaSqliteInternal_Join_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(JoinRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(HaSqliteInternalServer).Join(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/HaSqliteInternal/Join",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(HaSqliteInternalServer).Join(ctx, req.(*JoinRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _HaSqliteInternal_serviceDesc = grpc.ServiceDesc{
	ServiceName: "HaSqliteInternal",
	HandlerType: (*HaSqliteInternalServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Join",
			Handler:    _HaSqliteInternal_Join_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "internal.proto",
}