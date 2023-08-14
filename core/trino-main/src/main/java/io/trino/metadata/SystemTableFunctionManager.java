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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorProvider;

import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static java.util.Objects.requireNonNull;

public class SystemTableFunctionManager
        implements TableFunctionManager
{
    private final Set<TableFunctionProvider> tableFunctionProviders;

    @Inject
    public SystemTableFunctionManager(Set<TableFunctionProvider> tableFunctionProviders)
    {
        this.tableFunctionProviders = ImmutableSet.copyOf(requireNonNull(tableFunctionProviders, "tableFunctionProviders is null"));
    }

    @Override
    public Set<ConnectorTableFunction> getTableFunctions()
    {
        return tableFunctionProviders.stream()
                .map(TableFunctionProvider::getConnectorTableFunction)
                .collect(toImmutableSet());
    }

    @Override
    public ConnectorSplitSource getSplitSource(ConnectorTableFunctionHandle functionHandle)
    {
        Set<ConnectorSplitSource> connectorSplitSources = tableFunctionProviders.stream()
                .map(tableFunctionProvider -> tableFunctionProvider.getSplits(functionHandle))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableSet());

        if (connectorSplitSources.size() > 1) {
            throw new TrinoException(FUNCTION_IMPLEMENTATION_ERROR, "Multiple get splits implementations found for a function handle");
        }

        return getOnlyElement(connectorSplitSources);
    }

    @Override
    public TableFunctionProcessorProvider getTableFunctionProcessorProvider(ConnectorTableFunctionHandle functionHandle)
    {
        Set<TableFunctionProcessorProvider> tableFunctionProcessorProviders = tableFunctionProviders.stream()
                .map(tableFunctionProvider -> tableFunctionProvider.getTableFunctionProcessorProvider(functionHandle))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableSet());

        if (tableFunctionProcessorProviders.size() > 1) {
            throw new TrinoException(FUNCTION_IMPLEMENTATION_ERROR, "Multiple tableFunctionProcessorProvider implementations found for a function handle");
        }

        return getOnlyElement(tableFunctionProcessorProviders);
    }
}
