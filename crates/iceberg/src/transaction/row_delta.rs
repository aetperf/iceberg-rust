// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Row-level changes committed as one snapshot.
//!
//! aetperf fork addition. A thin action over [`MergingSnapshotProducer`], which
//! already handles added and removed delete files; upstream has no action that
//! exposes them yet (apache/iceberg-rust#2203).

use std::collections::HashMap;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::{Result, invalid_data};
use crate::spec::{DataContentType, DataFile, Operation};
use crate::table::Table;
use crate::transaction::action::CommitStatus;
use crate::transaction::snapshot::merging::{MergingIntent, MergingSnapshotProducer};
use crate::transaction::snapshot::validate_files_not_live;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind};

/// Adds data files and delete files, and removes data files and delete files,
/// in one snapshot.
///
/// Covers merge-on-read changes (new rows plus position deletes or deletion
/// vectors, with superseded deletion vectors removed) and copy-on-write changes
/// (rewritten data files replacing the originals).
///
/// The operation is `Append` when only data files are added, `Delete` when
/// nothing is added but deletes or removals exist, and `Overwrite` otherwise.
/// Files committed concurrently are not checked.
#[derive(Clone, Default)]
pub struct RowDeltaAction {
    commit_uuid: Option<Uuid>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
    removed_data_files: Vec<DataFile>,
    removed_delete_files: Vec<DataFile>,
}

impl RowDeltaAction {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Data files holding new rows.
    pub fn add_data_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(files);
        self
    }

    /// Position delete files or deletion vectors to add.
    pub fn add_delete_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_delete_files.extend(files);
        self
    }

    /// Data files to remove, matched by path against the current snapshot.
    pub fn remove_data_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.removed_data_files.extend(files);
        self
    }

    /// Delete files to remove, such as a deletion vector replaced by a new one.
    pub fn remove_delete_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.removed_delete_files.extend(files);
        self
    }

    /// Set commit UUID for the snapshot.
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set snapshot summary properties.
    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }

    fn validate(&self) -> Result<()> {
        if self.added_data_files.is_empty()
            && self.added_delete_files.is_empty()
            && self.removed_data_files.is_empty()
            && self.removed_delete_files.is_empty()
        {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "Row delta has no file to add or remove",
            ));
        }
        if let Some(f) = self
            .added_data_files
            .iter()
            .chain(&self.removed_data_files)
            .find(|f| f.content_type() != DataContentType::Data)
        {
            return Err(invalid_data!(
                "Expected a data file, got {:?} for {}",
                f.content_type(),
                f.file_path()
            ));
        }
        if let Some(f) = self
            .added_delete_files
            .iter()
            .chain(&self.removed_delete_files)
            .find(|f| f.content_type() == DataContentType::Data)
        {
            return Err(invalid_data!(
                "Expected a delete file, got a data file: {}",
                f.file_path()
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl TransactionAction for RowDeltaAction {
    type State = MergingSnapshotProducer;

    fn new_state(&self) -> MergingSnapshotProducer {
        let adds_rows = !self.added_data_files.is_empty();
        let removes = !self.added_delete_files.is_empty()
            || !self.removed_data_files.is_empty()
            || !self.removed_delete_files.is_empty();
        let operation = match (adds_rows, removes) {
            (true, true) => Operation::Overwrite,
            (false, true) => Operation::Delete,
            _ => Operation::Append,
        };
        MergingSnapshotProducer::new(MergingIntent {
            commit_uuid: self.commit_uuid,
            operation,
            snapshot_properties: self.snapshot_properties.clone(),
            added_data_files: self.added_data_files.clone(),
            added_delete_files: self.added_delete_files.clone(),
            removed_data_files: self.removed_data_files.clone(),
            removed_delete_files: self.removed_delete_files.clone(),
            fail_missing_delete_paths: true,
            truncate_full_table: false,
        })
    }

    async fn commit(
        &self,
        state: &mut MergingSnapshotProducer,
        table: &Table,
    ) -> Result<ActionCommit> {
        self.validate()?;
        validate_files_not_live(table, &self.added_data_files).await?;
        state.apply(table).await
    }

    async fn cleanup(self, _state: MergingSnapshotProducer, _table: &Table, _status: CommitStatus) {
    }
}

/// Returns the live data files and the live delete files of the current snapshot
/// (aetperf fork addition).
///
/// A row-level writer needs the [`DataFile`] entries themselves to remove a data
/// file or to replace a deletion vector; the scan API only exposes paths.
pub async fn live_files(table: &Table) -> Result<(Vec<DataFile>, Vec<DataFile>)> {
    let mut data = Vec::new();
    let mut deletes = Vec::new();
    for manifest_file in crate::transaction::snapshot::current_manifests(table).await? {
        let manifest = table.object_cache().get_manifest(&manifest_file).await?;
        for entry in manifest.entries().iter().filter(|e| e.is_alive()) {
            let file = entry.data_file().clone();
            if file.content_type() == DataContentType::Data {
                data.push(file);
            } else {
                deletes.push(file);
            }
        }
    }
    Ok((data, deletes))
}
