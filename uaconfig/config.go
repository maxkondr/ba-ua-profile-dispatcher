package uaconfig

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"google.golang.org/grpc/grpclog"
)

// The structure of config is following:
// {
//     "listen_port" : 7780,
//     "zipkin":
//     {
//         "url": "http://zipkin.tracing:9411/api/v1/spans"
//     },
//     "ua_profile_services": [
//         {
//             "i_ua_type": 1,
//             "url": "ba-ua-profile-audio-codes.default:7787"
//         },
//         {
//             "i_ua_type": 2,
//             "url": "ba-ua-profile-htek.default:7788"
//         }
//     ]
// }

// UaProfileServiceConfig stores particular UA profile params
type UaProfileServiceConfig struct {
	IUaType uint32 `json:"i_ua_type"` // mandatory
	URL     string `json:"url"`       // mandatory
}

// UaProfileDispatcherConfig config
type UaProfileDispatcherConfig struct {
	MD5                  string
	ListenPort           int                      `json:"listen_port"`
	ZipkinURL            string                   `json:"zipkinURL"`
	UaProfileServiceList []UaProfileServiceConfig `json:"ua_profile_services"`
}

// GetUaProfileServiceByID return UaTypeConfig struct if found
func (c UaProfileDispatcherConfig) GetUaProfileServiceByID(id uint32) (UaProfileServiceConfig, bool) {
	for _, uaType := range c.UaProfileServiceList {
		if uaType.IUaType == id {
			return uaType, true
		}
	}
	return UaProfileServiceConfig{}, false
}

func getMd5Sum(file string) (string, error) {
	content, err := ioutil.ReadFile(file)
	if err != nil {
		return "", fmt.Errorf("Can't calculate md5 sum, err:%s", err)
	}
	return fmt.Sprintf("%x", md5.Sum(content)), nil
}

// GetUaProfileServiceList return list of payment processors
func (c UaProfileDispatcherConfig) GetUaProfileServiceList() []UaProfileServiceConfig {
	return c.UaProfileServiceList
}

func validateConfig(c UaProfileDispatcherConfig) error {
	if c.ListenPort == 0 {
		return fmt.Errorf("mandatory param 'listen_port' is missed in config")
	}

	for _, uaProfileServiceConfig := range c.UaProfileServiceList {
		if uaProfileServiceConfig.IUaType == 0 {
			return fmt.Errorf("mandatory param 'i_ua_type' for ua profile(%v) is missed in config", uaProfileServiceConfig)
		}
		if len(uaProfileServiceConfig.URL) == 0 {
			return fmt.Errorf("mandatory param 'url' for ua profile(%v) is missed in config", uaProfileServiceConfig)
		}
	}
	return nil
}

// LoadConfiguration reads config from file
func LoadConfiguration(file string) UaProfileDispatcherConfig {
	grpclog.Infoln("Reading config", file)
	var config UaProfileDispatcherConfig
	f, err := os.Open(file)
	defer f.Close()

	if err != nil {
		panic(err.Error())
	}
	jsonParser := json.NewDecoder(f)

	if err := jsonParser.Decode(&config); err != nil {
		panic(err.Error())
	}

	if err := validateConfig(config); err != nil {
		panic(err.Error())
	}

	if md5, err := getMd5Sum(file); err == nil {
		config.MD5 = md5
	}
	return config
}

// Reconfig re-reads config file
func Reconfig(file string) (UaProfileDispatcherConfig, error) {
	var config UaProfileDispatcherConfig
	f, err := os.Open(file)
	defer f.Close()

	if err != nil {
		return config, err
	}
	jsonParser := json.NewDecoder(f)

	if err := jsonParser.Decode(&config); err != nil {
		return config, err
	}

	if err := validateConfig(config); err != nil {
		return config, err
	}

	if md5, err := getMd5Sum(file); err == nil {
		config.MD5 = md5
	}

	return config, nil
}
