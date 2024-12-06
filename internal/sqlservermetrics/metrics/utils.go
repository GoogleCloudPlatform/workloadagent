/*
Copyright 2024 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/commandlineexecutor"
	"github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/log"
)

// DiskTypeEnum enum used for disktypes to keep linux and windows collection consistent .
type DiskTypeEnum int

const (
	// LocalSSD - local disk
	LocalSSD DiskTypeEnum = iota
	// PersistentSSD - persistent disk
	PersistentSSD
	// Other - not local or persistent disk but still a valid disk type
	Other
)

func (disk DiskTypeEnum) String() string {
	return []string{"LOCAL-SSD", "PERSISTENT-SSD", "OTHER"}[disk]
}

func convertHexStringToBoolean(value string) (bool, error) {
	value = strings.TrimSpace(strings.Replace(value, "0x", "", -1))
	output, err := strconv.ParseUint(value, 16, 64)
	if err != nil {
		return false, err
	}
	return output == 1, nil
}

// handleNilString converts generic string to the desired string output,
// or returns 'unknown' if desired type if nil.
func handleNilString(data any) string {
	if data == nil {
		return "unknown"
	}
	return fmt.Sprintf("%v", data.(string))
}

// handleNilInt converts generic int64 to desired string output,
// or returns 'unknown' if desired type if nil.
func handleNilInt(data any) string {
	if data == nil {
		return "unknown"
	}
	// The passed in data might not be int64 so we need to handle the conversion from
	// all possible integer types to string.
	res, err := integerToString(data)
	if err != nil {
		log.Logger.Error(err)
		return "unknown"
	}

	return res
}

// handleNilFloat64 converts generic float64 to desired string output,
// or returns 'unknown' if desired type if nil.
func handleNilFloat64(data any) string {
	if data == nil {
		return "unknown"
	}
	return fmt.Sprintf("%f", data.(float64))
}

// handleNilBool converts generic bool to desired string output,
// or returns 'unknown' if desired type if nil.
func handleNilBool(data any) string {
	if data == nil {
		return "unknown"
	}
	return fmt.Sprintf("%v", data.(bool))
}

// SaveToFile saves data to given path.
func SaveToFile(path string, data []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(data)
	if err != nil {
		return err
	}
	return nil
}

// PrettyStruct converts the passed in struct into a pretty json format.
func PrettyStruct(data any) (string, error) {
	val, err := json.MarshalIndent(data, "", "    ")
	if err != nil {
		return "", err
	}
	return string(val), nil
}

// CommandLineExecutorWrapper executes a windows or linux command with arguments given
func CommandLineExecutorWrapper(ctx context.Context, executable string, argsToSplit string, exec commandlineexecutor.Execute) (string, error) {
	result := exec(ctx, commandlineexecutor.Params{
		Executable:  executable,
		ArgsToSplit: argsToSplit,
	})
	if result.Error != nil {
		return "", fmt.Errorf("Error when running CommandLineExecutor: %s", result.StdErr)
	}
	return strings.TrimSuffix(result.StdOut, "\n"), nil
}

// GetPhysicalDriveFromPath gets the physical drive associated with a file path for linux and windows env
func GetPhysicalDriveFromPath(ctx context.Context, path string, windows bool, exec commandlineexecutor.Execute) string {

	if path == "" {
		return "unknown"
	} else if windows {
		mapping := strings.Split(path, `:`)
		if len(mapping) <= 1 {
			log.Logger.Warn("Couldn't find windows drive associated with the physical path name.")
			return "unknown"
		}
		return mapping[0]
	}

	dir, filename := filepath.Split(path)
	filePath, filePathErr := CommandLineExecutorWrapper(ctx, "/bin/sh", fmt.Sprintf(" -c 'find %s -type f -iname \"%s\" -print'", dir, filename), exec)
	if filePathErr != nil {
		log.Logger.Warn(filePathErr)
		return "unknown"
	}

	physicalPathMount, physicalPathErr := CommandLineExecutorWrapper(ctx, "/bin/sh", fmt.Sprintf(" -c 'df --output=target %s| tail -n 1'", filePath), exec)
	if physicalPathErr != nil {
		log.Logger.Warn(physicalPathErr)
		return "unknown"
	}

	resultMount, mountErr := CommandLineExecutorWrapper(ctx, "/bin/sh", fmt.Sprintf(" -c ' mount |grep sd'"), exec)
	if mountErr != nil {
		log.Logger.Warn(mountErr)
		return "unknown"
	}

	allMounts := strings.TrimSuffix(resultMount, "\n")
	physicalDriveHelper := regexp.MustCompile(` `+physicalPathMount+` `).Split(allMounts, -1)

	physicalDrives := []string{}
	for i := 0; i < len(physicalDriveHelper)-1; i++ {
		splitStr := regexp.MustCompile("\n| |/").Split(physicalDriveHelper[i], -1)
		physicalDrives = append(physicalDrives, splitStr[len(splitStr)-2])
	}
	physicalDrive := strings.Join(physicalDrives, ", ")

	if physicalDrive == "" {
		return "unknown"
	}
	return physicalDrive
}

// integerToString converts any valid integer type to a string representation.
func integerToString(num any) (string, error) {
	switch v := num.(type) {
	case int:
		return strconv.Itoa(v), nil
	case int8:
		return strconv.FormatInt(int64(v), 10), nil
	case int16:
		return strconv.FormatInt(int64(v), 10), nil
	case int32:
		return strconv.FormatInt(int64(v), 10), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case uint:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case []uint8:
		return string([]uint8(v)), nil
	default:
		return "", fmt.Errorf("unsupported number type: %T", num)
	}
}
