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

//! Replaces the current schema by one the caller built.
//!
//! aetperf fork addition. `UpdateSchemaAction` can add and delete columns; it
//! cannot widen a type or make a column optional, and it assigns field ids
//! itself. A loader that reconciles a table with incoming data needs both, and
//! needs to know the field ids before it commits, to compute the metrics of the
//! data files it adds in the same commit.

use async_trait::async_trait;

use crate::spec::Schema;
use crate::table::Table;
use crate::transaction::action::{ActionCommit, StatelessAction};
use crate::{Result, TableRequirement, TableUpdate};

/// Adds a schema and makes it current. The commit asserts that the table's
/// current schema and last assigned field id are those the new schema was built
/// from, so that a concurrent schema change fails the commit.
#[derive(Clone)]
pub struct SetSchemaAction {
    schema: Schema,
}

impl SetSchemaAction {
    pub(crate) fn new(schema: Schema) -> Self {
        Self { schema }
    }
}

#[async_trait]
impl StatelessAction for SetSchemaAction {
    async fn commit(&self, table: &Table) -> Result<ActionCommit> {
        let metadata = table.metadata();
        let updates = vec![
            TableUpdate::AddSchema { schema: self.schema.clone() },
            TableUpdate::SetCurrentSchema { schema_id: -1 },
        ];
        let requirements = vec![
            TableRequirement::CurrentSchemaIdMatch { current_schema_id: metadata.current_schema().schema_id() },
            TableRequirement::LastAssignedFieldIdMatch { last_assigned_field_id: metadata.last_column_id() },
        ];
        Ok(ActionCommit::new(updates, requirements))
    }
}
