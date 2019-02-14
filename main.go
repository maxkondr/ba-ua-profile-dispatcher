package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/maxkondr/ba-proto/uaProfileDispatcher"
	"github.com/maxkondr/ba-ua-profile-dispatcher/server"
	"github.com/maxkondr/ba-ua-profile-dispatcher/uaconfig"

	"github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/openzipkin/zipkin-go-opentracing"
	zipkinot "github.com/openzipkin/zipkin-go-opentracing"
	"github.com/sirupsen/logrus"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var (
	zipkinCollector zipkintracer.Collector
	config          uaconfig.UaProfileDispatcherConfig
	configPath      = flag.String("config", "./config.json", "Path to config file")
	logger          logrus.Entry
)

func initTracerZipkin(zipkinURL string) {
	var err error
	zipkinCollector, err = zipkinot.NewHTTPCollector(zipkinURL)
	if err != nil {
		logger.Error("Error during zipkin HTTP Collection creation, err=", err.Error())
		os.Exit(1)
	}

	myName := os.Getenv("MY_POD_NAME")
	myIP := os.Getenv("MY_POD_IP")
	myNS := os.Getenv("MY_POD_NAMESPACE")

	var (
		debug       = false
		hostPort    = fmt.Sprintf("%s:%d", myIP, config.ListenPort)
		serviceName = fmt.Sprintf("%s.%s(%s)", myName, myNS, myIP)
	)

	recorder := zipkinot.NewRecorder(zipkinCollector, debug, hostPort, serviceName)
	zipkinTracer, err := zipkinot.NewTracer(recorder)
	if err != nil {
		logger.Error("Error during zipkin tracer creation, err=", err.Error())
		os.Exit(1)
	}
	opentracing.SetGlobalTracer(zipkinTracer)
}

func initLogger() {
	l := logrus.New()
	l.SetOutput(os.Stdout)
	l.Formatter = &logrus.JSONFormatter{DisableTimestamp: true}
	log.SetOutput(l.Writer())

	hostName, err := os.Hostname()
	if err != nil {
		hostName = "UNKNOWN"
	}

	logger = *logrus.NewEntry(l).WithFields(logrus.Fields{
		"process": os.Getenv("PORTA_APP_NAME"),
		"version": os.Getenv("PORTA_GIT_TAG") + ":" + os.Getenv("PORTA_GIT_COMMIT"),
		"host":    hostName,
	})
	// Make sure that log statements internal to gRPC library are logged using the logrus Logger as well.
	grpc_logrus.ReplaceGrpcLogger(&logger)
}

func grpcRequestIDPropagatorUnaryServerInterceptor(entry *logrus.Entry) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		headers, _ := metadata.FromIncomingContext(ctx)

		reqIDs, ok := headers["x-b3-traceid"]
		var reqID string
		if ok {
			reqID = reqIDs[0]
		}

		newCtx := ctxlogrus.ToContext(ctx, entry.WithFields(logrus.Fields{"request_id": reqID}))
		resp, err := handler(newCtx, req)
		return resp, err
	}
}

func init() {
	initLogger()
}

// main start a gRPC server and waits for connection
func main() {
	flag.Parse()

	config = uaconfig.LoadConfiguration(*configPath)
	uaProfileDispatcherServer := uaProfileDispatcherImpl.NewServer(config)
	stopCh := make(chan bool)
	defer close(stopCh)
	// ---------------------------------
	// Config re-reading
	go func() {
		for {
			select {
			case <-time.After(10 * time.Second):
				newConfig, err := uaconfig.Reconfig(*configPath)
				if err == nil && newConfig.MD5 != config.MD5 {
					logger.Infoln("Applying new config", newConfig)
					config = newConfig
					uaProfileDispatcherServer.SetConfig(newConfig)
				}
			case <-stopCh:
				// need to stop loop
				break
			}
		}
	}()

	// ---------------------------------
	initTracerZipkin(config.ZipkinURL)
	defer zipkinCollector.Close()

	// create a listener on TCP port 7777
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", config.ListenPort))
	if err != nil {
		logger.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer(grpc.StreamInterceptor(
		grpc_middleware.ChainStreamServer(
			grpc_opentracing.StreamServerInterceptor(),
			grpc_logrus.StreamServerInterceptor(&logger),
		),
	),
		grpc.UnaryInterceptor(
			grpc_middleware.ChainUnaryServer(
				grpc_opentracing.UnaryServerInterceptor(),
				grpcRequestIDPropagatorUnaryServerInterceptor(&logger),
				grpc_logrus.UnaryServerInterceptor(&logger)),
		),
	)

	uaProfileDispatcher.RegisterUaProfileDispatcherServer(grpcServer, uaProfileDispatcherServer)
	// start the server
	logger.Infof("Start listening on port %d", config.ListenPort)

	for _, val := range config.GetUaProfileServiceList() {
		logger.Info("Using ua service=", val)
	}

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			logger.Fatalf("failed to serve: %s", err)
		}
	}()

	// Listen for OS signals
	ch := make(chan os.Signal, 10)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	close(ch)
	stopCh <- true
	grpcServer.GracefulStop()

	logger.Info("Finished")
}
