package cpeErrors

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	NoCpeTypes         = status.Error(codes.NotFound, "CPE types are not found")
	NoCpeType          = status.Error(codes.NotFound, "CPE type is not found")
	Internal           = status.Error(codes.Internal, "Internal server error")
	RequestTerminated  = status.Error(codes.Canceled, "Request was terminated")
	ServiceUnavailable = status.Error(codes.Unavailable, "Service unavailable")
)
