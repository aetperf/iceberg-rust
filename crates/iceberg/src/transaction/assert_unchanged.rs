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

//! Asserts that the schema and the partition spec are those the transaction
//! was planned on.
//!
//! aetperf fork addition. A snapshot commit asserts the snapshot it was
//! planned on, not the schema: a concurrent schema change that adds no
//! snapshot goes unnoticed. A writer whose data files were checked against a
//! schema needs the catalog to refuse the commit when that schema is no longer
//! the current one.

use async_trait::async_trait;

use crate::table::Table;
use crate::transaction::action::{ActionCommit, StatelessAction};
use crate::{Result, TableRequirement};

/// Adds no update, and the requirements that the current schema and the
/// default partition spec of the table are unchanged.
#[derive(Clone, Default)]
pub struct AssertUnchangedAction {}

impl AssertUnchangedAction {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl StatelessAction for AssertUnchangedAction {
    async fn commit(&self, table: &Table) -> Result<ActionCommit> {
        let metadata = table.metadata();
        Ok(ActionCommit::new(vec![], vec![
            TableRequirement::CurrentSchemaIdMatch { current_schema_id: metadata.current_schema().schema_id() },
            TableRequirement::DefaultSpecIdMatch { default_spec_id: metadata.default_partition_spec_id() },
        ]))
    }
}
