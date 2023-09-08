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
package io.trino.filesystem;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

public abstract class ForwardingTrinoFileSystem
        implements TrinoFileSystem
{
    public static ForwardingTrinoFileSystem of(Supplier<TrinoFileSystem> fileSystemSupplier)
    {
        return new ForwardingTrinoFileSystem()
        {
            @Override
            protected TrinoFileSystem delegate()
            {
                return fileSystemSupplier.get();
            }
        };
    }

    protected abstract TrinoFileSystem delegate();

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        return delegate().newInputFile(location);
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length)
    {
        return delegate().newInputFile(location, length);
    }

    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        return delegate().newOutputFile(location);
    }

    @Override
    public void deleteFile(Location location)
            throws IOException
    {
        delegate().deleteFile(location);
    }

    @Override
    public void deleteFiles(Collection<Location> locations)
            throws IOException
    {
        delegate().deleteFiles(locations);
    }

    @Override
    public void deleteDirectory(Location location)
            throws IOException
    {
        delegate().deleteDirectory(location);
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        delegate().renameFile(source, target);
    }

    @Override
    public FileIterator listFiles(Location location)
            throws IOException
    {
        return delegate().listFiles(location);
    }

    @Override
    public Optional<Boolean> directoryExists(Location location)
            throws IOException
    {
        return delegate().directoryExists(location);
    }

    @Override
    public void createDirectory(Location location)
            throws IOException
    {
        delegate().createDirectory(location);
    }

    @Override
    public void renameDirectory(Location source, Location target)
            throws IOException
    {
        delegate().renameDirectory(source, target);
    }

    @Override
    public Set<Location> listDirectories(Location location)
            throws IOException
    {
        return delegate().listDirectories(location);
    }
}
