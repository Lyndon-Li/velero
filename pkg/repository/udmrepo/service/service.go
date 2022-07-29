/*
Copyright 2020 the Velero contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package service

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/vmware-tanzu/velero/pkg/repository/udmrepo"
)

func CreateService(logger logrus.FieldLogger) udmrepo.BackupRepoService {
	///TODO: create from kopiaLib
	return nil
}

func GetRepoConfigFile(workPath string, repoID string) string {
	if workPath == "" {
		workPath = getDefaultWorkPath()
	}

	name := "repo-" + strings.ToLower(repoID) + ".conf"

	return filepath.Join(workPath, name)
}

func GetOpenOption(password string, workPath string, repoID string, description string) udmrepo.RepoOptions {
	repoOption := udmrepo.RepoOptions{
		RepoPassword:   password,
		ConfigFilePath: GetRepoConfigFile(workPath, repoID),
		Description:    description,
	}

	return repoOption
}

func getDefaultWorkPath() string {
	return filepath.Join(os.Getenv("HOME"), ".config", "udmrepo")
}
