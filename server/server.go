package uaProfileDispatcherImpl

import (
	"io"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/maxkondr/ba-proto/uaProfile"
	"github.com/maxkondr/ba-proto/uaProfileDispatcher"

	"github.com/maxkondr/ba-ua-profile-dispatcher/uaconfig"
	"github.com/maxkondr/ba-ua-profile-dispatcher/uaerrors"
	"github.com/sirupsen/logrus"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"

	context "golang.org/x/net/context"
	"google.golang.org/genproto/googleapis/api/httpbody"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var (
	grpcDialOptions []grpc.DialOption
)

func init() {
	grpcDialOptions = []grpc.DialOption{grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithTimeout(time.Second * 1),
		grpc.WithUnaryInterceptor(
			grpc_middleware.ChainUnaryClient(
				grpc_opentracing.UnaryClientInterceptor())),
		grpc.WithStreamInterceptor(
			grpc_middleware.ChainStreamClient(
				grpc_opentracing.StreamClientInterceptor(),
			)),
	}
}

func getLogger(context context.Context) *logrus.Entry {
	return ctxlogrus.Extract(context)
}

// typeInfoChCb struct for keeping info about UA Type
type typeInfoChCb struct {
	uaTypeConfig uaconfig.UaProfileServiceConfig
	uaTypeInfo   *uaProfile.UaTypeInfo
	err          error
}

// typeInfoChCb struct for keeping info about UA meta info
type metaInfoChCb struct {
	uaTypeConfig uaconfig.UaProfileServiceConfig
	uaMetaInfo   *uaProfile.UaProfileMetaInfo
	err          error
}

// Server implementation
type Server struct {
	config  uaconfig.UaProfileDispatcherConfig
	clients map[string]uaProfile.UaProfileClient
	uaProfileDispatcher.UaProfileDispatcherServer
}

// NewServer creates server
func NewServer(config uaconfig.UaProfileDispatcherConfig) *Server {
	s := Server{config: config}
	s.clients = make(map[string]uaProfile.UaProfileClient)
	return &s
}

func newRPCMetaDataContext(context context.Context) context.Context {
	headers, _ := metadata.FromIncomingContext(context)
	return metadata.NewOutgoingContext(context, headers)
}

/*
func merge(cs ...<-chan int) <-chan int {
	var wg sync.WaitGroup
	out := make(chan int)

	// Start an output goroutine for each input channel in cs.  output
	// copies values from c to out until c is closed, then calls wg.Done.
	output := func(c <-chan int) {
		for n := range c {
			out <- n
		}
		wg.Done()
	}
	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}

	// Start a goroutine to close out once all the output goroutines are
	// done.  This must start after the wg.Add call.
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}
*/
// -------------------------------------------------------------------------------------

// SetConfig sets new config to server
func (s *Server) SetConfig(newConfig uaconfig.UaProfileDispatcherConfig) {
	s.config = newConfig
}

func (s Server) createClient(addr string) (uaProfile.UaProfileClient, *grpc.ClientConn, error) {
	conn, err := grpc.Dial(addr, grpcDialOptions...)
	if err != nil {
		return nil, nil, err
	}
	myC := uaProfile.NewUaProfileClient(conn)
	return myC, conn, nil
}

// -------------------------------------------------------------------------------------
func (s Server) callUaProfileGetTypeInfo(ctx context.Context, uaTypeConfig uaconfig.UaProfileServiceConfig, ch chan typeInfoChCb, wg *sync.WaitGroup) {
	logger := getLogger(ctx)

	defer wg.Done()
	client, conn, err := s.createClient(uaTypeConfig.URL)

	if err != nil {
		logger.Warnf("Can't create client to service=%s, err=%s", uaTypeConfig.URL, err.Error())
		ch <- typeInfoChCb{uaTypeConfig, nil, err}
		return
	}
	defer conn.Close()

	logger.Debug("Sending 'GetUaTypeInfo' request to service=", uaTypeConfig.URL)
	resp, err := client.GetUaTypeInfo(ctx, &empty.Empty{})
	ch <- typeInfoChCb{uaTypeConfig, resp, err}
}

// GetUaTypeList interface
func (s Server) GetUaTypeList(ctx context.Context, _ *empty.Empty) (*uaProfileDispatcher.GetUaTypeListResponse, error) {
	logger := getLogger(ctx)
	logger.Info("Received request")

	uaProfileServiceList := s.config.GetUaProfileServiceList()
	ch := make(chan typeInfoChCb, len(uaProfileServiceList))
	defer close(ch)

	ctx = newRPCMetaDataContext(ctx)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	for _, uaProfileServiceConfig := range uaProfileServiceList {
		wg.Add(1)
		go s.callUaProfileGetTypeInfo(ctx, uaProfileServiceConfig, ch, &wg)
	}
	wg.Wait()

	toReturn := &uaProfileDispatcher.GetUaTypeListResponse{}
	for range uaProfileServiceList {
		select {
		case <-ctx.Done():
			// incoming request was canceled
			logger.Warn("Request was terminated. Reason: ", ctx.Err())
			return nil, uaerrors.RequestTerminated
		case callRes := <-ch:
			if callRes.err != nil {
				logger.Warnf("Error during getting info from service=%s, err=%s", callRes.uaTypeConfig.URL, callRes.err.Error())
			} else {
				logger.Debugf("Received response=%v on 'GetUaTypeInfo' request from service=%s", callRes.uaTypeInfo, callRes.uaTypeConfig.URL)
				toReturn.UaTypeList = append(toReturn.UaTypeList, callRes.uaTypeInfo)
			}
		}
	}

	if len(toReturn.UaTypeList) == 0 {
		logger.Warn("UA services are unavailable")
		return nil, uaerrors.ServiceUnavailable
	}
	return toReturn, nil
}

// -------------------------------------------------------------------------------------

// GetUaProfileMetaInfo interface
func (s Server) GetUaProfileMetaInfo(ctx context.Context, req *uaProfileDispatcher.GetUaProfileMetaInfoRequest) (*uaProfileDispatcher.GetUaProfileMetaInfoResponse, error) {
	logger := getLogger(ctx)
	logger.Infof("Received request=%v", req)

	uaProfileService, ok := s.config.GetUaProfileServiceByID(req.IUaType)
	if !ok {
		logger.Warnf("Service with i_ua_type=%d is not found", req.IUaType)
		return nil, uaerrors.NoUaType
	}

	client, conn, err := s.createClient(uaProfileService.URL)

	if err != nil {
		logger.Warnf("Can't create client to service=%s err=%s", uaProfileService.URL, err.Error())
		return nil, uaerrors.ServiceUnavailable
	}
	defer conn.Close()

	ctx = newRPCMetaDataContext(ctx)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	logger.Debug("Sending 'GetUaProfileMetaInfo' request to service=", uaProfileService.URL)
	resp, err := client.GetUaProfileMetaInfo(ctx, &empty.Empty{})

	if err != nil {
		logger.Warnf("Error during 'GetUaProfileMetaInfo' request processing from service=%s, err=%s", uaProfileService.URL, err.Error())
		return nil, uaerrors.Internal
	}

	return &uaProfileDispatcher.GetUaProfileMetaInfoResponse{
		UaProfileMetainfo: resp.UaProfileMetainfo,
	}, nil
}

// -------------------------------------------------------------------------------------

// GenerateUaProfile interface
func (s Server) GenerateUaProfile(req *uaProfileDispatcher.GenerateUaProfileRequest, stream uaProfileDispatcher.UaProfileDispatcher_GenerateUaProfileServer) error {
	logger := getLogger(stream.Context())
	logger.Infof("Received request=%v", req.Options.UaProfileData)

	uaProfileService, ok := s.config.GetUaProfileServiceByID(req.IUaType)
	if !ok {
		logger.Warnf("Service with i_ua_type=%d is not found", req.IUaType)
		return uaerrors.NoUaType
	}

	client, conn, err := s.createClient(uaProfileService.URL)
	if err != nil {
		logger.Warnf("Can't create client to service=%s err=%s", uaProfileService.URL, err.Error())
		return uaerrors.ServiceUnavailable
	}
	defer conn.Close()

	ctx := newRPCMetaDataContext(stream.Context())
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	logger.Debug("Sending 'GenerateUaProfile' request to service=", uaProfileService.URL)
	clientStream, err := client.GenerateUaProfile(ctx, &uaProfile.GenerateUaProfileRequest{UaProfileData: req.Options.UaProfileData})

	if err != nil {
		logger.Warnf("Error during 'GenerateUaProfile' request processing from service=%s, err=%s", uaProfileService.URL, err.Error())
		return uaerrors.Internal
	}

	ch := make(chan *httpbody.HttpBody, 100)

	go func(c chan<- *httpbody.HttpBody) {
		for {
			httpB, err := clientStream.Recv()
			if err != nil {
				if err == io.EOF {
					logger.Info("Transfer finished successfully")
					close(c)
					return
				}
				logger.Error("Error has appeared during receiving data, err=", err.Error())
				close(c)
				return
			}
			c <- httpB
		}
	}(ch)

	for {
		select {
		case <-ctx.Done():
			// incoming request was canceled
			logger.Warn("Request was terminated. Reason: ", ctx.Err())
			return uaerrors.RequestTerminated
		case httpB, ok := <-ch:
			if !ok {
				// no more data to transfer
				return nil
			}
			if err = stream.Send(httpB); err != nil {
				logger.Error("Error has appeared during profile transfer, err=", err.Error())
				return uaerrors.Internal
			}
		}
	}
}
