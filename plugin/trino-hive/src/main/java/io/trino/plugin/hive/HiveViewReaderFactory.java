/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hive;

import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;

import java.util.Optional;
import java.util.function.BiFunction;

import static io.trino.plugin.hive.ViewReaderUtil.ViewReader;

public interface HiveViewReaderFactory
{
    ViewReader createViewReader(
            ConnectorSession session,
            SemiTransactionalHiveMetastore metastore,
            Table table,
            BiFunction<ConnectorSession, SchemaTableName, Optional<CatalogSchemaTableName>> tableRedirectionResolver);
}
