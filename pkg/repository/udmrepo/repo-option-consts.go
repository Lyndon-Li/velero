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

package udmrepo

const (
	UNIFIED_REPO_OPTION_STORAGE_TYPE_S3    = "s3"
	UNIFIED_REPO_OPTION_STORAGE_TYPE_AZURE = "azure"
	UNIFIED_REPO_OPTION_STORAGE_TYPE_FS    = "filesystem"
	UNIFIED_REPO_OPTION_STORAGE_TYPE_GCS   = "gcs"

	UNIFIED_REPO_GEN_OPTION_MAINTAIN_MODE  = "mode"
	UNIFIED_REPO_GEN_OPTION_MAINTAIN_FULL  = "full"
	UNIFIED_REPO_GEN_OPTION_MAINTAIN_QUICK = "quick"

	UNIFIED_REPO_GEN_OPTION_OWNER_NAME  = "username"
	UNFIED_REPO_GEN_OPTION_OWNER_DOMAIN = "domainname"

	UNIFIED_REPO_STORE_OPTION_S3_KEY_ID             = "accessKeyID"
	UNIFIED_REPO_STORE_OPTION_S3_PROVIDER           = "providerName"
	UNIFIED_REPO_STORE_OPTION_S3_SECRET_KEY         = "secretAccessKey"
	UNIFIED_REPO_STORE_OPTION_S3_TOKEN              = "sessionToken"
	UNIFIED_REPO_STORE_OPTION_S3_ENDPOINT           = "endpoint"
	UNIFIED_REPO_STORE_OPTION_S3_DISABLE_TLS        = "doNotUseTLS"
	UNIFIED_REPO_STORE_OPTION_S3_DISABLE_TLS_VERIFY = "skipTLSVerify"

	UNIFIED_REPO_STORE_OPTION_AZ_KEY             = "storageKey"
	UNIFIED_REPO_STORE_OPTION_AZ_DOMAIN          = "storageDomain"
	UNIFIED_REPO_STORE_OPTION_AZ_STORAGE_ACCOUNT = "storageAccount"
	UNIFIED_REPO_STORE_OPTION_AZ_TOKEN           = "sasToken"

	UNIFIED_REPO_STORE_OPTION_FS_PATH = "fspath"

	UNIFIED_REPO_STORE_OPTION_GCP_READONLY = "readonly"

	UNIFIED_REPO_STORE_OPTION_OSS_BUCKET = "bucket"
	UNIFIED_REPO_STORE_OPTION_OSS_REGION = "region"

	UNIFIED_REPO_STORE_OPTION_CRED_FILE   = "credFile"
	UNIFIED_REPO_STORE_OPTION_PREFIX      = "prefix"
	UNIFIED_REPO_STORE_OPTION_PREFIX_NAME = "unified-repo"

	UNIFIED_REPO_THROTTLE_OPTION_READ_OPS       = "readOPS"
	UNIFIED_REPO_THROTTLE_OPTION_WRITE_OPS      = "writeOPS"
	UNIFIED_REPO_THROTTLE_OPTION_LIST_OPS       = "listOPS"
	UNIFIED_REPO_THROTTLE_OPTION_UPLOAD_BYTES   = "uploadBytes"
	UNIFIED_REPO_THROTTLE_OPTION_DOWNLOAD_BYTES = "downloadBytes"
)
