package uaerrors

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	NoUaTypes          = status.Error(codes.NotFound, "UA types are not found")
	NoUaType           = status.Error(codes.NotFound, "UA type is not found")
	Internal           = status.Error(codes.Internal, "Internal server error")
	RequestTerminated  = status.Error(codes.Canceled, "Request was terminated")
	ServiceUnavailable = status.Error(codes.Unavailable, "Service unavailable")
)
