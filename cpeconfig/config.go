package cpeConfig

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
//     "cpe_profile_services": [
//         {
//             "i_cpe_type": 1,
//             "url": "ba-cpe-profile-audio-codes.default:7787"
//         },
//         {
//             "i_cpe_type": 2,
//             "url": "ba-cpe-profile-htek.default:7788"
//         }
//     ]
// }

// CpeProfileServiceConfig stores particular CPE profile params
type CpeProfileServiceConfig struct {
	ICpeType uint32 `json:"i_cpe_type"` // mandatory
	URL      string `json:"url"`        // mandatory
}

// CpeProfileDispatcherConfig config
type CpeProfileDispatcherConfig struct {
	MD5                   string
	ListenPort            int                       `json:"listen_port"`
	ZipkinURL             string                    `json:"zipkinURL"`
	CpeProfileServiceList []CpeProfileServiceConfig `json:"cpe_profile_services"`
}

// GetCpeProfileServiceByID return CpeTypeConfig struct if found
func (c CpeProfileDispatcherConfig) GetCpeProfileServiceByID(id uint32) (CpeProfileServiceConfig, bool) {
	for _, cpeType := range c.CpeProfileServiceList {
		if cpeType.ICpeType == id {
			return cpeType, true
		}
	}
	return CpeProfileServiceConfig{}, false
}

func getMd5Sum(file string) (string, error) {
	content, err := ioutil.ReadFile(file)
	if err != nil {
		return "", fmt.Errorf("Can't calculate md5 sum, err:%s", err)
	}
	return fmt.Sprintf("%x", md5.Sum(content)), nil
}

// GetCpeProfileServiceList return list of CPE profiles
func (c CpeProfileDispatcherConfig) GetCpeProfileServiceList() []CpeProfileServiceConfig {
	return c.CpeProfileServiceList
}

func validateConfig(c CpeProfileDispatcherConfig) error {
	if c.ListenPort == 0 {
		return fmt.Errorf("mandatory param 'listen_port' is missed in config")
	}

	for _, cpeProfileServiceConfig := range c.CpeProfileServiceList {
		if cpeProfileServiceConfig.ICpeType == 0 {
			return fmt.Errorf("mandatory param 'i_cpe_type' for cpe profile(%v) is missed in config", cpeProfileServiceConfig)
		}
		if len(cpeProfileServiceConfig.URL) == 0 {
			return fmt.Errorf("mandatory param 'url' for cpe profile(%v) is missed in config", cpeProfileServiceConfig)
		}
	}
	return nil
}

// LoadConfiguration reads config from file
func LoadConfiguration(file string) CpeProfileDispatcherConfig {
	grpclog.Infoln("Reading config", file)
	var config CpeProfileDispatcherConfig
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
func Reconfig(file string) (CpeProfileDispatcherConfig, error) {
	var config CpeProfileDispatcherConfig
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
