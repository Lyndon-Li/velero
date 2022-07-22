package ownership

import "github.com/vmware-tanzu/velero/pkg/repository/udmrepo"

const (
	defaultOwnerUsername = "default"
	defaultOwnerDomain   = "default"
)

// GetBackupOwner returns the owner used by uploaders when saving a snapshot or
// opening the unified repository. At present, use the default owner only
func GetBackupOwner() udmrepo.OwnershipOptions {
	return udmrepo.OwnershipOptions{
		Username:   defaultOwnerUsername,
		DomainName: defaultOwnerDomain,
	}
}

// GetBackupOwner returns the owner used to create/connect the unified repository.
//At present, use the default owner only
func GetRepositoryOwner() udmrepo.OwnershipOptions {
	return udmrepo.OwnershipOptions{
		Username:   defaultOwnerUsername,
		DomainName: defaultOwnerDomain,
	}
}
