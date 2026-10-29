## v1.19

### Download
https://github.com/vmware-tanzu/velero/releases/tag/v1.19.0

### Container Image
`velero/velero:v1.19.0`

### Documentation
https://velero.io/docs/v1.19/

### Upgrading
https://velero.io/docs/v1.19/upgrade-to-1.19/

### Highlights

#### Block Data Mover

Velero 1.19 introduces a block data mover (velero-block) for CSI snapshot data movement, enabling block-level backup and restore for both file system and block volumes backed by block storage. It integrates with the Kubernetes CSI Changed Block Tracking (CBT) API (https://kubernetes.io/blog/2025/09/25/csi-changed-block-tracking/) to transfer only changed blocks during incremental backups and restores with compatible CSI drivers, avoiding file system scanning overhead and improving backup and restore efficiency. For more information, check Velero document https://velero.io/docs/main/csi-snapshot-data-movement/.    

#### Volume Data In-place Restore

Velero 1.19 introduces volume data in-place restore, allowing backup data to be restored directly into existing volumes. It supports both full and incremental restores for CSI snapshot data movement (velero-fs and velero-block) and File System Backup, with incremental restores skipping unchanged data to reduce data transfer and recovery time. For more information, check Velero document https://velero.io/docs/main/volume-data-inplace-restore/.  

#### Fine-Grained Filters

Velero 1.19 introduces fine-grained filters for backups and restores through ResourcePolicy ConfigMaps. Users can define per-namespace and per-resource-kind rules with independent label selectors and resource name patterns, providing more precise control over which namespaced and cluster-scoped resources are backed up or restored. For more information, check Velero document https://velero.io/docs/main/fine-grained-backup-filters/ and https://velero.io/docs/main/fine-grained-restore-filters/.  

#### Namespace Selection By Labels

Velero 1.19 supports dynamically including or excluding namespaces from backups based on namespace labels through ResourcePolicy ConfigMaps. This simplifies backup and schedule management by automatically selecting matching namespaces when each backup starts, without maintaining explicit namespace lists. For more information, check Velero document https://velero.io/docs/main/resource-filtering/#namespace-selection-by-label-backup-only.   

#### Global Volume Policy

Velero 1.19 introduces global volume policies, allowing administrators to configure default volume backup rules for all backups through a shared ConfigMap. Backup-specific volume policies take precedence over global rules, providing consistent defaults while allowing customization for individual backups. For more information, check Velero document https://velero.io/docs/main/resource-filtering/#global-backup-volume-policies.  

#### Custom Action Type In Volume Policy

Velero 1.19 adds a `custom` action type to volume policies, allowing third-party BackupItemAction plugins to handle volume backups using custom methods. Users can select volumes for custom handling and pass plugin-specific parameters alongside existing snapshot and file system backup policies. For more information, check the Velero document <TBD>.  

#### Default Restore Resource Modifier

Velero 1.19 allows administrators to configure a default restore resource modifier through a shared ConfigMap, automatically applying common resource transformations during restores. Individual restores can override the default with their own modifier or explicitly skip it, simplifying consistent restore customization. For more information, check the Velero document [<TBD>](https://velero.io/docs/main/restore-resource-modifiers/#default-resource-modifiers).  

### Runtime And Dependencies
<TBD>

### Limitations/Known Issues

#### Block Data Mover Gap On Snapshot Preservation

Some CSI drivers require the parent backup's snapshot to survive through the incremental backup, so that CBT could be retrieved successfully.  
For 1.19.0, Velero block data mover always delete the snapshot after the backup completes. Consequently, for volumes provisioned by these CSI drivers, Velero block data mover makes full backup & restore only.  
An enhancement is ongoing to migitate this gap and will be available soon.  

### Breaking Changes

#### Restic Deprecation

Restic path for fs-backup is completed removed in 1.19, Velero ignores backup & restore requests under Restic path in all workflows, make sure there is no backups or restores under Restic path before upgrading to 1.19.  

#### Default Prepare Queue Length

A default prepare queue length (5) is set for data movers, so that pending data mover pods won't occupy large amount of resources from the cluster. In the previous versions there is no limit. For more information about data mover prepare queue length, see Velero document https://velero.io/docs/v1.19/node-agent-prepare-queue-length/.

#### Hashing Algorithm For Kopia Repository

Kopia repository's hashing algorithm is changed from `BLAKE2B-256-128` to `HMAC-SHA256-128` for security compliance. This changes apply to new repositories only, existing repostories still use `BLAKE2B-256-128`.  

### All Changes
<TBD>