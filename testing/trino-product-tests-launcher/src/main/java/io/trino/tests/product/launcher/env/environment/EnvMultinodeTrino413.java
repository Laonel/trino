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
package io.trino.tests.product.launcher.env.environment;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import java.time.Duration;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.configureTempto;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.wait.strategy.Wait.forHealthcheck;
import static org.testcontainers.containers.wait.strategy.Wait.forLogMessage;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public class EnvMultinodeTrino413
        extends EnvironmentProvider
{
    private final PortBinder portBinder;
    private final DockerFiles dockerFiles;

    @Inject
    public EnvMultinodeTrino413(PortBinder portBinder, DockerFiles dockerFiles, StandardMultinode standardMultinode)
    {
        super(ImmutableList.of(standardMultinode));
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        DockerFiles.ResourceProvider configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-trino-413");

        builder
                .addConnector("hive", forHostPath(configDir.getPath("hive.properties")))
                .addConnector("iceberg", forHostPath(configDir.getPath("iceberg.properties")))
                .addConnector("delta", forHostPath(configDir.getPath("delta.properties")));

        // This container acts as a separate singlenode cluster since the discovery url is set to localhost:8080 and the node version and environment are different
        DockerContainer container = new DockerContainer("trinodb/trino:413", "trino-413")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingForAll(forLogMessage(".*======== SERVER STARTED ========.*", 1), forHealthcheck())
                .withStartupTimeout(Duration.ofMinutes(5))
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("conf/presto/etc/jvm.config")), "/etc/trino/jvm.config")
                .withCopyFileToContainer(forHostPath(configDir.getPath("config-413.properties")), "/etc/trino/config.properties")
                .withCopyFileToContainer(forHostPath(configDir.getPath("hive-413.properties")), "/etc/trino/catalog/hive.properties")
                .withCopyFileToContainer(forHostPath(configDir.getPath("iceberg-413.properties")), "/etc/trino/catalog/iceberg.properties")
                .withCopyFileToContainer(forHostPath(configDir.getPath("delta-413.properties")), "/etc/trino/catalog/delta.properties")
                .withExposedPorts(8080);
        portBinder.exposePort(container, 8070, 8080);
        builder.addContainer(container);

        builder.configureContainers(EnvMultinodeTrino413::exportAWSCredentials);

        configureTempto(builder, configDir);
    }

    private static void exportAWSCredentials(DockerContainer container)
    {
        exportAWSCredential(container, "TRINO_AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID", true);
        exportAWSCredential(container, "TRINO_AWS_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY", true);
        exportAWSCredential(container, "TRINO_AWS_SESSION_TOKEN", "AWS_SESSION_TOKEN", false);
        exportAWSCredential(container, "AWS_REGION", "AWS_REGION", true);
        exportAWSCredential(container, "S3_BUCKET", "S3_BUCKET", true);
    }

    private static void exportAWSCredential(DockerContainer container, String credentialEnvVariable, String containerEnvVariable, boolean required)
    {
        String credentialValue = System.getenv(credentialEnvVariable);
        if (credentialValue == null) {
            if (required) {
                throw new IllegalStateException(format("Environment variable %s not set", credentialEnvVariable));
            }
            return;
        }
        container.withEnv(containerEnvVariable, credentialValue);
    }
}
