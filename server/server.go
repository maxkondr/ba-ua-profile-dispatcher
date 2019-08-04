package cpeProfileDispatcherImpl

import (
	"io"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/maxkondr/ba-proto/cpeProfile"
	"github.com/maxkondr/ba-proto/cpeProfileDispatcher"

	"github.com/maxkondr/ba-ua-profile-dispatcher/cpeconfig"
	"github.com/maxkondr/ba-ua-profile-dispatcher/cpeerrors"
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

// typeInfoChCb struct for keeping info about CPE Type
type typeInfoChCb struct {
	cpeTypeConfig cpeConfig.CpeProfileServiceConfig
	cpeTypeInfo   *cpeProfile.CpeTypeInfo
	err           error
}

// typeInfoChCb struct for keeping info about CPE meta info
type metaInfoChCb struct {
	cpeTypeConfig cpeConfig.CpeProfileServiceConfig
	cpeMetaInfo   *cpeProfile.CpeProfileMetaInfo
	err           error
}

// Server implementation
type Server struct {
	config  cpeConfig.CpeProfileDispatcherConfig
	clients map[string]cpeProfile.CpeProfileClient
	cpeProfileDispatcher.CpeProfileDispatcherServer
}

// NewServer creates server
func NewServer(config cpeConfig.CpeProfileDispatcherConfig) *Server {
	s := Server{config: config}
	s.clients = make(map[string]cpeProfile.CpeProfileClient)
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
func (s *Server) SetConfig(newConfig cpeConfig.CpeProfileDispatcherConfig) {
	s.config = newConfig
}

func (s Server) createClientToService(ctx context.Context, addr string) (cpeProfile.CpeProfileClient, *grpc.ClientConn, error) {
	conn, err := grpc.Dial(addr, grpcDialOptions...)
	if err != nil {
		return nil, nil, err
	}
	go func() {
		<-ctx.Done() // it means the initial request processing is finished
		if cerr := conn.Close(); cerr != nil {
			getLogger(ctx).Printf("Failed to close conn to %s: %v", addr, cerr)
		}
	}()
	myC := cpeProfile.NewCpeProfileClient(conn)
	return myC, conn, nil
}

// -------------------------------------------------------------------------------------
func (s Server) callCpeProfileGetTypeInfo(ctx context.Context, cpeTypeConfig cpeConfig.CpeProfileServiceConfig, dataCh chan typeInfoChCb, stopCh chan struct{}) {
	logger := getLogger(ctx)

	client, _, err := s.createClientToService(ctx, cpeTypeConfig.URL)

	if err != nil {
		logger.Warnf("Can't create client to service=%s, err=%s", cpeTypeConfig.URL, err.Error())
		select {
		case <-stopCh:
			// receiver can't accept result
			return
		case dataCh <- typeInfoChCb{cpeTypeConfig, nil, err}:
		}
		return
	}

	// defer conn.Close()

	logger.Debug("Sending 'GetCpeTypeInfo' request to service=", cpeTypeConfig.URL)
	resp, err := client.GetCpeTypeInfo(ctx, &empty.Empty{})
	select {
	case <-stopCh:
		// receiver can't accept result
		return
	case dataCh <- typeInfoChCb{cpeTypeConfig, resp, err}:
	}

}

// GetCpeTypeList interface
func (s Server) GetCpeTypeList(ctx context.Context, _ *empty.Empty) (*cpeProfileDispatcher.GetCpeTypeListResponse, error) {
	logger := getLogger(ctx)
	logger.Info("Received request")

	cpeProfileServiceList := s.config.GetCpeProfileServiceList()
	dataCh := make(chan typeInfoChCb, len(cpeProfileServiceList))
	stopCh := make(chan struct{})
	// defer close(ch)

	ctx = newRPCMetaDataContext(ctx)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, cpeProfileServiceConfig := range cpeProfileServiceList {
		go s.callCpeProfileGetTypeInfo(ctx, cpeProfileServiceConfig, dataCh, stopCh)
	}

	toReturn := &cpeProfileDispatcher.GetCpeTypeListResponse{}
	for range cpeProfileServiceList {
		select {
		case <-ctx.Done():
			// incoming request was canceled
			logger.Warn("Request was terminated. Reason: ", ctx.Err())
			// notify dataCh sender that we are no long accept results
			close(stopCh)
			return nil, cpeErrors.RequestTerminated
		case callRes := <-dataCh:
			if callRes.err != nil {
				logger.Warnf("Error during getting info from service=%s, err=%s", callRes.cpeTypeConfig.URL, callRes.err.Error())
			} else {
				logger.Debugf("Received response=%v on 'GetCpeTypeInfo' request from service=%s", callRes.cpeTypeInfo, callRes.cpeTypeConfig.URL)
				toReturn.CpeTypeList = append(toReturn.CpeTypeList, callRes.cpeTypeInfo)
			}
		}
	}

	if len(toReturn.CpeTypeList) == 0 {
		logger.Warn("CPE services are unavailable")
		return nil, cpeErrors.ServiceUnavailable
	}
	close(stopCh)
	return toReturn, nil
}

// -------------------------------------------------------------------------------------

// GetCpeProfileMetaInfo interface
func (s Server) GetCpeProfileMetaInfo(ctx context.Context, req *cpeProfileDispatcher.GetCpeProfileMetaInfoRequest) (*cpeProfileDispatcher.GetCpeProfileMetaInfoResponse, error) {
	logger := getLogger(ctx)
	logger.Infof("Received request=%v", req)

	cpeProfileService, ok := s.config.GetCpeProfileServiceByID(req.IUaType)
	if !ok {
		logger.Warnf("Service with i_cpe_type=%d is not found", req.IUaType)
		return nil, cpeErrors.NoCpeType
	}

	ctx = newRPCMetaDataContext(ctx)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	client, _, err := s.createClientToService(ctx, cpeProfileService.URL)

	if err != nil {
		logger.Warnf("Can't create client to service=%s err=%s", cpeProfileService.URL, err.Error())
		return nil, cpeErrors.ServiceUnavailable
	}
	// defer conn.Close()

	logger.Debug("Sending 'GetCpeProfileMetaInfo' request to service=", cpeProfileService.URL)
	resp, err := client.GetCpeProfileMetaInfo(ctx, &empty.Empty{})

	if err != nil {
		logger.Warnf("Error during 'GetCpeProfileMetaInfo' request processing from service=%s, err=%s", cpeProfileService.URL, err.Error())
		return nil, cpeErrors.Internal
	}

	return &cpeProfileDispatcher.GetCpeProfileMetaInfoResponse{
		CpeProfileMetainfo: resp.CpeProfileMetainfo,
	}, nil
}

// -------------------------------------------------------------------------------------

// GenerateCpeProfile interface
func (s Server) GenerateCpeProfile(req *cpeProfileDispatcher.GenerateCpeProfileRequest, stream cpeProfileDispatcher.CpeProfileDispatcher_GenerateCpeProfileServer) error {
	logger := getLogger(stream.Context())
	logger.Infof("Received request=%v", req.Options.CpeProfileData)

	cpeProfileService, ok := s.config.GetCpeProfileServiceByID(req.IUaType)
	if !ok {
		logger.Warnf("Service with i_cpe_type=%d is not found", req.IUaType)
		return cpeErrors.NoCpeType
	}

	ctx := newRPCMetaDataContext(stream.Context())
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	client, _, err := s.createClientToService(ctx, cpeProfileService.URL)
	if err != nil {
		logger.Warnf("Can't create client to service=%s err=%s", cpeProfileService.URL, err.Error())
		return cpeErrors.ServiceUnavailable
	}
	// defer conn.Close()

	logger.Debug("Sending 'GenerateCpeProfile' request to service=", cpeProfileService.URL)
	clientStream, err := client.GenerateCpeProfile(ctx, &cpeProfile.GenerateCpeProfileRequest{CpeProfileData: req.Options.CpeProfileData})

	if err != nil {
		logger.Warnf("Error during 'GenerateCpeProfile' request processing from service=%s, err=%s", cpeProfileService.URL, err.Error())
		return cpeErrors.Internal
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
			return cpeErrors.RequestTerminated
		case httpB, ok := <-ch:
			if !ok {
				// no more data to transfer
				return nil
			}
			if err = stream.Send(httpB); err != nil {
				logger.Error("Error has appeared during profile transfer, err=", err.Error())
				return cpeErrors.Internal
			}
		}
	}
}
