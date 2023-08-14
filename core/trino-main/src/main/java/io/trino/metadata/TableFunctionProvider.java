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
package io.trino.metadata;

import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorProvider;

import java.util.Optional;

/**
 * Provides specific ConnectorTableFunction, TableFunctionProcessorProvider and ConnectorSplitSource implementation
 * for concrete function handle
 */
public interface TableFunctionProvider
{
    ConnectorTableFunction getConnectorTableFunction();

    default Optional<TableFunctionProcessorProvider> getTableFunctionProcessorProvider(ConnectorTableFunctionHandle functionHandle)
    {
        return Optional.empty();
    }

    default Optional<ConnectorSplitSource> getSplits(ConnectorTableFunctionHandle functionHandle)
    {
        return Optional.empty();
    }
}
