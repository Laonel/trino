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
package io.trino.tests.product.s3;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import io.airlift.log.Logger;
import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert;
import io.trino.tempto.query.QueryExecutionException;
import io.trino.tempto.query.QueryExecutor;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TestGroups.S3_BACKWARDS_COMPATIBILITY;
import static io.trino.tests.product.utils.QueryExecutors.connectToTrino;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.util.Objects.requireNonNull;

public class TestS3BackwardsCompatibilityDoubleSlashes
        extends ProductTest
{
    private static final Logger log = Logger.get(TestS3BackwardsCompatibilityDoubleSlashes.class);

    private Closer closer;
    private AmazonS3 s3;
    private String schemaName;
    private String bucketName;

    @BeforeMethodWithContext
    public void setup()
    {
        bucketName = requireNonNull(System.getenv("S3_BUCKET"), "Environment variable not set: S3_BUCKET");
        if (bucketName.contains("/")) {
            // if the bucket name itself contains/ends in slashes it takes away our control on the test conditions
            throw new IllegalStateException("Expected S3_BUCKET to not contain any slashes");
        }

        closer = Closer.create();
        // Force us-east-2 since that's where the S3_BUCKET lives instead of using AWS_REGION env var
        // Ideally there should be no AWS_REGION env var set in which case everything gets auto-configured correctly
        s3 = AmazonS3Client.builder()
                .withRegion(Regions.US_EAST_2)
                .build();
        closer.register(s3::shutdown);

        List<String> catalogs = Arrays.stream(tableFormats())
                .flatMap(Arrays::stream)
                .map(String.class::cast)
                .toList();
        schemaName = "test_s3_backwards_compatibility_" + randomNameSuffix();
        // Create schema with double slashes in the location. This will get inherited by the table locations.
        // This is the case that used to be erroneously handled by previous Trino versions, leading to table corruption.
        // This test verifies we can still read from and operate on such tables.
        onTrino().executeQuery("CREATE SCHEMA delta." + schemaName + " WITH (location = 's3://" + bucketName + "/temp_files//" + schemaName + "')");
        closer.register(() -> onTrino().executeQuery("DROP SCHEMA delta." + schemaName));
        closer.register(() -> {
            onTrino().executeQuery("SHOW TABLES FROM delta." + schemaName).column(1).stream()
                    .map(String.class::cast)
                    .forEach(tableName -> {
                        log.warn("Table '%s' left behind after test method execution, trying to remove it", tableName);
                        for (String catalogName : catalogs) {
                            try {
                                onTrino().executeQuery("DROP TABLE %s.%s.%s".formatted(catalogName, schemaName, tableName));
                                return;
                            }
                            catch (QueryExecutionException ignored) {
                            }
                        }
                        log.error("Failed to remove table '%s' left behind after test method execution", tableName);
                    });
        });
    }

    @AfterMethodWithContext
    public void tearDown()
            throws Exception
    {
        if (closer != null) {
            closer.close();
            closer = null;
        }
        s3 = null;
    }

    @Test(groups = {PROFILE_SPECIFIC_TESTS, S3_BACKWARDS_COMPATIBILITY})
    public void testContainerVersions()
    {
        assertThat(onTrino413().executeQuery("SELECT version()"))
                .containsOnly(row("413"));
    }

    @Test(groups = {PROFILE_SPECIFIC_TESTS, S3_BACKWARDS_COMPATIBILITY}, dataProvider = "tableFormats")
    public void testReadsOnCorruptedTable(String tableFormat)
    {
        String tableName = tableFormat + "_reads_on_corrupted_table_" + randomNameSuffix();
        String qualifiedTableName = "%s.%s.%s".formatted(tableFormat, schemaName, tableName);
        onTrino413().executeQuery("CREATE TABLE " + qualifiedTableName + "(a INT, b INT)");
        try {
            // current version can read inserts done by 413
            onTrino413().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (0, 1)");
            assertThat(onTrino413().executeQuery("TABLE " + qualifiedTableName))
                    .containsOnly(row(0, 1));
            assertThat(onTrino().executeQuery("TABLE " + qualifiedTableName))
                    .containsOnly(row(0, 1));
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + qualifiedTableName);
        }
    }

    @Test(groups = {PROFILE_SPECIFIC_TESTS, S3_BACKWARDS_COMPATIBILITY}, dataProvider = "tableFormats")
    public void testInsertsOnCorruptedTable(String tableFormat)
    {
        String tableName = tableFormat + "_writes_on_corrupted_table_" + randomNameSuffix();
        String qualifiedTableName = "%s.%s.%s".formatted(tableFormat, schemaName, tableName);
        onTrino413().executeQuery("CREATE TABLE " + qualifiedTableName + "(a, b) AS VALUES (1, 2)");
        try {
            // current version can insert into tables created/inserted to by 413
            onTrino413().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (3, 4)");
            onTrino().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (5, 6)");
            assertThat(onTrino().executeQuery("TABLE " + qualifiedTableName))
                    .containsOnly(row(1, 2), row(3, 4), row(5, 6));
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + qualifiedTableName);
        }
    }

    @Test(groups = {PROFILE_SPECIFIC_TESTS, S3_BACKWARDS_COMPATIBILITY})
    public void testFileLevelDeletesOnCorruptedIcebergTable()
    {
        String tableName = "iceberg_deletes_on_corrupted_table_" + randomNameSuffix();
        String qualifiedTableName = "iceberg.%s.%s".formatted(schemaName, tableName);
        onTrino413().executeQuery("CREATE TABLE " + qualifiedTableName + "(a INT, b INT)");
        try {
            onTrino413().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (0, 1)");
            onTrino413().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (2, 3)");
            onTrino413().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (4, 5)");

            // current version can read file level deletes done by 413
            onTrino413().executeQuery("DELETE FROM " + qualifiedTableName + " WHERE a = 0");
            assertThat(onTrino().executeQuery("TABLE " + qualifiedTableName))
                    .containsOnly(row(2, 3), row(4, 5));

            // current version can perform file level deletes on files written by 413
            onTrino().executeQuery("DELETE FROM " + qualifiedTableName + " WHERE a = 2");
            assertThat(onTrino().executeQuery("TABLE " + qualifiedTableName))
                    .containsOnly(row(4, 5));

            onTrino().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (10, 11), (12, 13)");
            onTrino().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (14, 15)");

            // current version can perform file level deletes on uncorrupted files
            onTrino().executeQuery("DELETE FROM " + qualifiedTableName + " WHERE a = 14");
            assertThat(onTrino().executeQuery("TABLE " + qualifiedTableName))
                    .containsOnly(row(4, 5), row(10, 11), row(12, 13));
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + qualifiedTableName);
        }
    }

    @Test(groups = {PROFILE_SPECIFIC_TESTS, S3_BACKWARDS_COMPATIBILITY})
    public void testFileLevelDeletesOnCorruptedDeltaTable()
    {
        String tableName = "delta_deletes_on_corrupted_table_" + randomNameSuffix();
        String qualifiedTableName = "delta.%s.%s".formatted(schemaName, tableName);
        onTrino413().executeQuery("CREATE TABLE " + qualifiedTableName + "(a INT, b INT)");
        try {
            onTrino413().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (0, 1)");
            onTrino413().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (2, 3)");

            // 413 could not perform file level deletes on files created by 413
            assertQueryFailure(() -> onTrino413().executeQuery("DELETE FROM " + qualifiedTableName + " WHERE a = 0"))
                    .hasMessageContaining("Unable to rewrite Parquet file");

            // current version can perform file level deletes on files written by 413
            onTrino().executeQuery("DELETE FROM " + qualifiedTableName + " WHERE a = 2");
            assertThat(onTrino().executeQuery("TABLE " + qualifiedTableName))
                    .containsOnly(row(0, 1));

            onTrino().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (10, 11), (12, 13)");
            onTrino().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (14, 15)");

            // current version can perform file level deletes on uncorrupted files
            onTrino().executeQuery("DELETE FROM " + qualifiedTableName + " WHERE a = 14");
            assertThat(onTrino().executeQuery("TABLE " + qualifiedTableName))
                    .containsOnly(row(0, 1), row(10, 11), row(12, 13));
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + qualifiedTableName);
        }
    }

    @Test(groups = {PROFILE_SPECIFIC_TESTS, S3_BACKWARDS_COMPATIBILITY})
    public void testPartitionDeletesOnCorruptedHiveTable()
    {
        String tableName = "hive_deletes_on_corrupted_table_" + randomNameSuffix();
        String qualifiedTableName = "hive.%s.%s".formatted(schemaName, tableName);
        onTrino413().executeQuery("CREATE TABLE " + qualifiedTableName + "(a INT, b INT) WITH (partitioned_by = ARRAY['b'])");
        try {
            onTrino413().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (0, 1), (2, 3), (4, 5)");

            // current version can read partition deletes done by 413
            onTrino413().executeQuery("DELETE FROM " + qualifiedTableName + " WHERE b = 1");
            assertThat(onTrino().executeQuery("TABLE " + qualifiedTableName))
                    .containsOnly(row(2, 3), row(4, 5));

            // current version can perform partition deletes on partitions written by 413
            onTrino().executeQuery("DELETE FROM " + qualifiedTableName + " WHERE b = 5");
            assertThat(onTrino().executeQuery("TABLE " + qualifiedTableName))
                    .containsOnly(row(2, 3));

            onTrino().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (10, 11), (12, 13)");

            // current version can perform partition deletes on uncorrupted partitions
            onTrino().executeQuery("DELETE FROM " + qualifiedTableName + " WHERE b = 11");
            assertThat(onTrino().executeQuery("TABLE " + qualifiedTableName))
                    .containsOnly(row(2, 3), row(12, 13));
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + qualifiedTableName);
        }
    }

    @Test(groups = {PROFILE_SPECIFIC_TESTS, S3_BACKWARDS_COMPATIBILITY})
    public void testRowLevelDeletesOnCorruptedDeltaTable()
    {
        String tableName = "delta_row_level_deletes_" + randomNameSuffix();
        String qualifiedTableName = "delta.%s.%s".formatted(schemaName, tableName);
        onTrino413().executeQuery("CREATE TABLE " + qualifiedTableName + "(a INT, b INT)");
        try {
            onTrino413().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (0, 1), (2, 3), (4, 5)");

            // 413 could not perform row level deletes on files created by 413
            assertQueryFailure(() -> onTrino413().executeQuery("DELETE FROM " + qualifiedTableName + " WHERE a = 4"))
                    .hasMessageContaining("Unable to rewrite Parquet file");

            // current version can perform row level deletes on files created by 413
            onTrino().executeQuery("DELETE FROM " + qualifiedTableName + " WHERE a = 4");
            assertThat(onTrino().executeQuery("TABLE " + qualifiedTableName))
                    .containsOnly(row(0, 1), row(2, 3));

            onTrino().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (10, 11), (12, 13), (14, 15)");

            // current version can perform row level deletes on uncorrupted files
            onTrino().executeQuery("DELETE FROM " + qualifiedTableName + " WHERE a = 14");
            assertThat(onTrino().executeQuery("TABLE " + qualifiedTableName))
                    .containsOnly(row(0, 1), row(2, 3), row(10, 11), row(12, 13));
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + qualifiedTableName);
        }
    }

    @Test(groups = {PROFILE_SPECIFIC_TESTS, S3_BACKWARDS_COMPATIBILITY})
    public void testRowLevelDeletesOnCorruptedIcebergTable()
    {
        String tableName = "iceberg_row_level_deletes_" + randomNameSuffix();
        String qualifiedTableName = "iceberg.%s.%s".formatted(schemaName, tableName);
        onTrino413().executeQuery("CREATE TABLE " + qualifiedTableName + "(a INT, b INT)");
        try {
            onTrino413().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (0, 1), (2, 3), (4, 5)");

            // current version can read row level deletes done by 413
            onTrino413().executeQuery("DELETE FROM " + qualifiedTableName + " WHERE a = 4");
            assertThat(onTrino().executeQuery("TABLE " + qualifiedTableName))
                    .containsOnly(row(0, 1), row(2, 3));

            // current version can perform row level deletes on files created by 413
            onTrino().executeQuery("DELETE FROM " + qualifiedTableName + " WHERE a = 2");
            assertThat(onTrino().executeQuery("TABLE " + qualifiedTableName))
                    .containsOnly(row(0, 1));

            onTrino().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (10, 11), (12, 13), (14, 15)");

            // current version can perform row level deletes on uncorrupted files
            onTrino().executeQuery("DELETE FROM " + qualifiedTableName + " WHERE a = 14");
            assertThat(onTrino().executeQuery("TABLE " + qualifiedTableName))
                    .containsOnly(row(0, 1), row(10, 11), row(12, 13));
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + qualifiedTableName);
        }
    }

    @Test(groups = {PROFILE_SPECIFIC_TESTS, S3_BACKWARDS_COMPATIBILITY})
    public void testUpdatesOnCorruptedDeltaTable()
    {
        String tableName = "delta_update_" + randomNameSuffix();
        String qualifiedTableName = "delta.%s.%s".formatted(schemaName, tableName);
        onTrino413().executeQuery("CREATE TABLE " + qualifiedTableName + "(a INT, b INT)");
        try {
            onTrino413().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (0, 1), (2, 3), (4, 5)");

            // 413 could not perform updates on files created by 413
            assertQueryFailure(() -> onTrino413().executeQuery("UPDATE " + qualifiedTableName + " SET b = 42 WHERE a = 4"))
                    .hasMessageContaining("Unable to rewrite Parquet file");

            // current version can perform updates on files created by 413
            onTrino().executeQuery("UPDATE " + qualifiedTableName + " SET b = 42 WHERE a = 2");
            assertThat(onTrino().executeQuery("TABLE " + qualifiedTableName))
                    .containsOnly(row(0, 1), row(2, 42), row(4, 5));

            onTrino().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (10, 11), (12, 13), (14, 15)");

            // current version can perform updates on uncorrupted files
            onTrino().executeQuery("UPDATE " + qualifiedTableName + " SET b = 42 WHERE a = 14");
            assertThat(onTrino().executeQuery("TABLE " + qualifiedTableName))
                    .containsOnly(row(0, 1), row(2, 42), row(4, 5), row(10, 11), row(12, 13), row(14, 42));
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + qualifiedTableName);
        }
    }

    @Test(groups = {PROFILE_SPECIFIC_TESTS, S3_BACKWARDS_COMPATIBILITY})
    public void testUpdatesOnCorruptedIcebergTable()
    {
        String tableName = "iceberg_update_" + randomNameSuffix();
        String qualifiedTableName = "iceberg.%s.%s".formatted(schemaName, tableName);
        onTrino413().executeQuery("CREATE TABLE " + qualifiedTableName + "(a INT, b INT)");
        try {
            onTrino413().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (0, 1), (2, 3), (4, 5)");

            // current version can read updates done by 413
            onTrino413().executeQuery("UPDATE " + qualifiedTableName + " SET b = 42 WHERE a = 4");
            assertThat(onTrino().executeQuery("TABLE " + qualifiedTableName))
                    .containsOnly(row(0, 1), row(2, 3), row(4, 42));

            // current version can perform updates on files created by 413
            onTrino().executeQuery("UPDATE " + qualifiedTableName + " SET b = 42 WHERE a = 2");
            assertThat(onTrino().executeQuery("TABLE " + qualifiedTableName))
                    .containsOnly(row(0, 1), row(2, 42), row(4, 42));

            onTrino().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (10, 11), (12, 13), (14, 15)");

            // current version can perform updates on uncorrupted files
            onTrino().executeQuery("UPDATE " + qualifiedTableName + " SET b = 42 WHERE a = 14");
            assertThat(onTrino().executeQuery("TABLE " + qualifiedTableName))
                    .containsOnly(row(0, 1), row(2, 42), row(4, 42), row(10, 11), row(12, 13), row(14, 42));
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + qualifiedTableName);
        }
    }

    @Test(groups = {PROFILE_SPECIFIC_TESTS, S3_BACKWARDS_COMPATIBILITY}, dataProvider = "tableFormats")
    public void testDropTableRemovesCorruptedFiles(String tableFormat)
    {
        String tableName = tableFormat + "_drop_corrupted_table_" + randomNameSuffix();
        String qualifiedTableName = "%s.%s.%s".formatted(tableFormat, schemaName, tableName);
        onTrino413().executeQuery("CREATE TABLE " + qualifiedTableName + "(a, b) AS VALUES (1, 2), (3, 4)");
        try {
            onTrino().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (5, 6)");

            // The actual table location may be randomized, and then this would be just a prefix. In any case, it is unique for this table since table name is randomized as well.
            String tableLocationPrefixWithDoubleSlash = "temp_files//" + schemaName + "/" + tableName;
            String tableLocationPrefixWithoutDoubleSlash = "temp_files/" + schemaName + "/" + tableName;
            ListObjectsV2Request listObjectsRequestWithDoubleSlash = new ListObjectsV2Request()
                    .withBucketName(bucketName)
                    .withPrefix(tableLocationPrefixWithDoubleSlash);
            ListObjectsV2Request listObjectsRequestWithoutDoubleSlash = new ListObjectsV2Request()
                    .withBucketName(bucketName)
                    .withPrefix(tableLocationPrefixWithoutDoubleSlash);

            if ("hive".equals(tableFormat)) {
                // Hive tables don't contain corrupted files
                Assertions.assertThat(s3.listObjectsV2(listObjectsRequestWithDoubleSlash).getObjectSummaries()).isEmpty();
            }
            else {
                Assertions.assertThat(s3.listObjectsV2(listObjectsRequestWithDoubleSlash).getObjectSummaries()).isNotEmpty();
            }
            Assertions.assertThat(s3.listObjectsV2(listObjectsRequestWithoutDoubleSlash).getObjectSummaries()).isNotEmpty();
            onTrino().executeQuery("DROP TABLE " + qualifiedTableName);
            Assertions.assertThat(s3.listObjectsV2(listObjectsRequestWithDoubleSlash).getObjectSummaries()).isEmpty();
            if ("iceberg".equals(tableFormat)) {
                // TODO Iceberg with Glue catalog leaves stats file behind on DROP TABLE.
                // As seen in testVerifyLegacyDropTableBehavior, this is at least not a regression.
                Assertions.assertThat(s3.listObjectsV2(listObjectsRequestWithoutDoubleSlash).getObjectSummaries())
                        .hasSize(1)
                        .singleElement().extracting(S3ObjectSummary::getKey, InstanceOfAssertFactories.STRING)
                        .matches("(temp_files)/(" + schemaName + ")/(" + tableName + "(?:-[a-z0-9]+)?)/(metadata)/([-a-z0-9_]+\\.stats)" +
                                "#%2F\\1%2F%2F\\2%2F\\3%2F\\4%2F\\5");
            }
            else {
                Assertions.assertThat(s3.listObjectsV2(listObjectsRequestWithoutDoubleSlash).getObjectSummaries()).isEmpty();
            }
        }
        finally {
            onTrino().executeQuery("DROP TABLE IF EXISTS " + qualifiedTableName);
        }
    }

    /**
     * Like {@link #testDropTableRemovesCorruptedFiles} but doesn't write with new version & drops with {@link #onTrino413()}.
     * This serves for documentation purposes.
     */
    @Test(groups = {PROFILE_SPECIFIC_TESTS, S3_BACKWARDS_COMPATIBILITY}, dataProvider = "tableFormats")
    public void testVerifyLegacyDropTableBehavior(String tableFormat)
    {
        String tableName = tableFormat + "_legacy_drop_corrupted_table_" + randomNameSuffix();
        String qualifiedTableName = "%s.%s.%s".formatted(tableFormat, schemaName, tableName);
        onTrino413().executeQuery("CREATE TABLE " + qualifiedTableName + "(a, b) AS VALUES (1, 2), (3, 4)");
        try {
            // The actual table location may be randomized, and then this would be just a prefix. In any case, it is unique for this table since table name is randomized as well.
            String tableLocationPrefixWithDoubleSlash = "temp_files//" + schemaName + "/" + tableName;
            String tableLocationPrefixWithoutDoubleSlash = "temp_files/" + schemaName + "/" + tableName;
            ListObjectsV2Request listObjectsRequestWithDoubleSlash = new ListObjectsV2Request()
                    .withBucketName(bucketName)
                    .withPrefix(tableLocationPrefixWithDoubleSlash);
            ListObjectsV2Request listObjectsRequestWithoutDoubleSlash = new ListObjectsV2Request()
                    .withBucketName(bucketName)
                    .withPrefix(tableLocationPrefixWithoutDoubleSlash);

            Assertions.assertThat(s3.listObjectsV2(listObjectsRequestWithDoubleSlash).getObjectSummaries()).isEmpty(); // old version didn't write anything here
            Assertions.assertThat(s3.listObjectsV2(listObjectsRequestWithoutDoubleSlash).getObjectSummaries()).isNotEmpty();
            onTrino413().executeQuery("DROP TABLE " + qualifiedTableName);
            Assertions.assertThat(s3.listObjectsV2(listObjectsRequestWithDoubleSlash).getObjectSummaries()).isEmpty();
            if ("iceberg".equals(tableFormat)) {
                // Trino 413's Iceberg with Glue catalog would leave stats file behind on DROP TABLE
                Assertions.assertThat(s3.listObjectsV2(listObjectsRequestWithoutDoubleSlash).getObjectSummaries())
                        .hasSize(1)
                        .singleElement().extracting(S3ObjectSummary::getKey, InstanceOfAssertFactories.STRING)
                        .matches("(temp_files)/(" + schemaName + ")/(" + tableName + "(?:-[a-z0-9]+)?)/(metadata)/([-a-z0-9_]+\\.stats)" +
                                "#%2F\\1%2F%2F\\2%2F\\3%2F\\4%2F\\5");
            }
            else {
                Assertions.assertThat(s3.listObjectsV2(listObjectsRequestWithoutDoubleSlash).getObjectSummaries()).isEmpty();
            }
        }
        finally {
            onTrino413().executeQuery("DROP TABLE IF EXISTS " + qualifiedTableName);
        }
    }

    @Test(groups = {PROFILE_SPECIFIC_TESTS, S3_BACKWARDS_COMPATIBILITY})
    public void testProceduresOnDelta()
    {
        String tableName = "delta_table_procedures_" + randomNameSuffix();
        String qualifiedTableName = "delta.%s.%s".formatted(schemaName, tableName);
        onTrino413().executeQuery("CREATE TABLE " + qualifiedTableName + "(a INT, b INT)");
        try {
            onTrino413().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (0, 1)");
            onTrino413().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (2, 3)");
            onTrino413().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (4, 5)");

            List<QueryAssert.Row> expected = ImmutableList.of(row(0, 1), row(2, 3), row(4, 5));
            // For optimize we need to set task_writer_count to 1, otherwise it will create more than one file.
            onTrino().executeQuery("SET SESSION task_writer_count = 1");
            onTrino().executeQuery("ALTER TABLE " + qualifiedTableName + " EXECUTE optimize");
            onTrino().executeQuery("RESET SESSION task_writer_count");
            assertThat(onTrino().executeQuery("TABLE " + qualifiedTableName)).containsOnly(expected);

            onTrino().executeQuery("SET SESSION delta.vacuum_min_retention = '0s'");
            onTrino().executeQuery("CALL delta.system.vacuum('%s', '%s', '0s')".formatted(schemaName, tableName));

            String tableLocationPrefixWithDoubleSlash = "temp_files//" + schemaName + "/" + tableName;
            String tableLocationPrefixWithoutDoubleSlash = "temp_files/" + schemaName + "/" + tableName;

            List<S3ObjectSummary> dataFilesWithDoubleSlash = listPrefix(tableLocationPrefixWithDoubleSlash).stream()
                    .filter(summary -> !summary.getKey().contains("_delta_log"))
                    .collect(toImmutableList());
            List<S3ObjectSummary> dataFilesWithoutDoubleSlash = listPrefix(tableLocationPrefixWithoutDoubleSlash).stream()
                    .filter(summary -> !summary.getKey().contains("_delta_log"))
                    .collect(toImmutableList());

            // TODO: Vacuum does not clean up corrupted files
            Assertions.assertThat(dataFilesWithDoubleSlash).hasSize(1);
            Assertions.assertThat(dataFilesWithoutDoubleSlash).hasSize(3);
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + qualifiedTableName);
        }
    }

    @Test(groups = {PROFILE_SPECIFIC_TESTS, S3_BACKWARDS_COMPATIBILITY})
    public void testProceduresOnIceberg()
    {
        String tableName = "iceberg_table_procedures_" + randomNameSuffix();
        String qualifiedTableName = "iceberg.%s.%s".formatted(schemaName, tableName);
        onTrino413().executeQuery("CREATE TABLE " + qualifiedTableName + "(a INT, b INT)");
        try {
            onTrino413().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (0, 1)");
            onTrino413().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (2, 3)");
            onTrino413().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (4, 5)");

            List<QueryAssert.Row> expected = ImmutableList.of(row(0, 1), row(2, 3), row(4, 5));
            // For optimize we need to set task_writer_count to 1, otherwise it will create more than one file.
            onTrino().executeQuery("SET SESSION task_writer_count = 1");
            onTrino().executeQuery("ALTER TABLE " + qualifiedTableName + " EXECUTE optimize");
            onTrino().executeQuery("RESET SESSION task_writer_count");
            assertThat(onTrino().executeQuery("TABLE " + qualifiedTableName)).containsOnly(expected);

            onTrino().executeQuery("SET SESSION iceberg.expire_snapshots_min_retention = '0s'");
            onTrino().executeQuery("SET SESSION iceberg.remove_orphan_files_min_retention = '0s'");
            onTrino().executeQuery("ALTER TABLE %s EXECUTE expire_snapshots(retention_threshold => '0s')".formatted(qualifiedTableName));
            onTrino().executeQuery("ALTER TABLE %s EXECUTE remove_orphan_files(retention_threshold => '0s')".formatted(qualifiedTableName));

            String tableLocationPrefixWithDoubleSlash = "temp_files//%s/%s/data/".formatted(schemaName, tableName);
            String tableLocationPrefixWithoutDoubleSlash = "temp_files/%s/%s/data/".formatted(schemaName, tableName);

            // TODO: remove_orphan_files and expire_snapshots do not clean up corrupted files
            Assertions.assertThat(listPrefix(tableLocationPrefixWithDoubleSlash)).hasSize(1);
            Assertions.assertThat(listPrefix(tableLocationPrefixWithoutDoubleSlash)).hasSize(3);
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + qualifiedTableName);
        }
    }

    @Test(groups = {PROFILE_SPECIFIC_TESTS, S3_BACKWARDS_COMPATIBILITY})
    public void testCtasWithIncorrectLocationOnHive()
    {
        String tableName = "hive_test_create_table_with_incorrect_location_" + randomNameSuffix();
        String qualifiedTableName = "hive.%s.%s".formatted(schemaName, tableName);
        String location = "s3://%s/%s/a#hash/%s".formatted(bucketName, schemaName, tableName);

        String createSql = "CREATE TABLE " + qualifiedTableName + " WITH (external_location = '" + location + "') AS SELECT * FROM tpch.tiny.nation";
        assertQueryFailure(() -> onTrino().executeQuery(createSql))
                .hasMessageContaining("External location is not a valid file system URI");
    }

    @Test(groups = {PROFILE_SPECIFIC_TESTS, S3_BACKWARDS_COMPATIBILITY})
    public void testCreateSchemaWithIncorrectLocationOnHive()
    {
        String schemaName = "hive_test_create_schema_with_incorrect_location_" + randomNameSuffix();
        String qualifiedTableName = "hive." + schemaName + ".test";
        String location = "s3://%s/a#hash/%s".formatted(bucketName, schemaName);

        onTrino413().executeQuery("CREATE SCHEMA " + schemaName + " WITH (location = '" + location + "')");
        try {
            Assertions.assertThat(getSchemaLocation(schemaName)).isEqualTo(location);

            assertQueryFailure(() -> onTrino().executeQuery("CREATE TABLE " + qualifiedTableName + " (a INT, b INT)"))
                    .hasMessageContaining("Fragment is not allowed in a file system location");
        }
        finally {
            onTrino().executeQuery("DROP SCHEMA " + schemaName);
        }
    }

    @Test(groups = {PROFILE_SPECIFIC_TESTS, S3_BACKWARDS_COMPATIBILITY})
    public void testCtasWithIncorrectLocationOnDelta()
    {
        String tableName = "delta_test_create_table_with_incorrect_location_" + randomNameSuffix();
        String qualifiedTableName = "delta.%s.%s".formatted(schemaName, tableName);
        String location = "s3://%s/%s/a#hash/%s".formatted(bucketName, schemaName, tableName);

        assertQueryFailure(() -> onTrino().executeQuery("CREATE TABLE " + qualifiedTableName + " WITH (location = '" + location + "') AS SELECT * FROM tpch.tiny.nation"))
                .hasMessageContaining("Error reading statistics from cache")
                .cause().cause().hasMessageContaining("Fragment is not allowed in a file system location");
    }

    @Test(groups = {PROFILE_SPECIFIC_TESTS, S3_BACKWARDS_COMPATIBILITY})
    public void testCreateSchemaWithIncorrectLocationOnDelta()
    {
        String schemaName = "delta_test_create_schema_with_incorrect_location_" + randomNameSuffix();
        String qualifiedTableName = "delta." + schemaName + ".test";
        String location = "s3://%s/a#hash/%s".formatted(bucketName, schemaName);

        onTrino413().executeQuery("CREATE SCHEMA " + schemaName + " WITH (location = '" + location + "')");
        try {
            Assertions.assertThat(getSchemaLocation(schemaName)).isEqualTo(location);

            assertQueryFailure(() -> onTrino().executeQuery("CREATE TABLE " + qualifiedTableName + " (a INT, b INT)"))
                    .hasMessageContaining("location contains a fragment");
            assertQueryFailure(() -> onTrino().executeQuery("CREATE TABLE " + qualifiedTableName + " AS SELECT * FROM tpch.tiny.nation"))
                    .hasMessageContaining("location contains a fragment");
            assertQueryFailure(() -> onTrino413().executeQuery("CREATE TABLE " + qualifiedTableName + " (a INT, b INT)"))
                    .hasMessageContaining("location contains a fragment");
            assertQueryFailure(() -> onTrino413().executeQuery("CREATE TABLE " + qualifiedTableName + " AS SELECT * FROM tpch.tiny.nation"))
                    .hasMessageContaining("location contains a fragment");
        }
        finally {
            onTrino().executeQuery("DROP SCHEMA " + schemaName);
        }
    }

    @Test(groups = {PROFILE_SPECIFIC_TESTS, S3_BACKWARDS_COMPATIBILITY})
    public void testCtasWithIncorrectLocationOnIceberg()
    {
        String tableName = "iceberg_test_create_table_with_incorrect_location_" + randomNameSuffix();
        String qualifiedTableName = "iceberg.%s.%s".formatted(schemaName, tableName);
        String location = "s3://%s/%s/a#hash/%s".formatted(bucketName, schemaName, tableName);

        String createSql = "CREATE TABLE " + qualifiedTableName + " WITH (location = '" + location + "') AS SELECT * FROM tpch.tiny.nation";
        assertQueryFailure(() -> onTrino().executeQuery(createSql))
                .hasMessageContaining("Fragment is not allowed in a file system location");
    }

    @Test(groups = {PROFILE_SPECIFIC_TESTS, S3_BACKWARDS_COMPATIBILITY})
    public void testCreateSchemaWithIncorrectLocationOnIceberg()
    {
        String schemaName = "iceberg_test_create_schema_with_incorrect_location_" + randomNameSuffix();
        String qualifiedTableName = "iceberg." + schemaName + ".test";
        String location = "s3://%s/a#hash/%s".formatted(bucketName, schemaName);

        onTrino413().executeQuery("CREATE SCHEMA " + schemaName + " WITH (location = '" + location + "')");
        try {
            Assertions.assertThat(getSchemaLocation(schemaName)).isEqualTo(location);

            assertQueryFailure(() -> onTrino().executeQuery("CREATE TABLE " + qualifiedTableName + " (a INT, b INT)"))
                    .hasMessageContaining("location contains a fragment");
            assertQueryFailure(() -> onTrino().executeQuery("CREATE TABLE " + qualifiedTableName + " AS SELECT * FROM tpch.tiny.nation"))
                    .hasMessageContaining("location contains a fragment");
            assertQueryFailure(() -> onTrino413().executeQuery("CREATE TABLE " + qualifiedTableName + " (a INT, b INT)"))
                    .hasMessageContaining("location contains a fragment");
            assertQueryFailure(() -> onTrino413().executeQuery("CREATE TABLE " + qualifiedTableName + " AS SELECT * FROM tpch.tiny.nation"))
                    .hasMessageContaining("location contains a fragment");
        }
        finally {
            onTrino().executeQuery("DROP SCHEMA " + schemaName);
        }
    }

    private List<S3ObjectSummary> listPrefix(String prefix)
    {
        ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
                .withBucketName(bucketName)
                .withPrefix(prefix);
        return s3.listObjectsV2(listObjectsRequest).getObjectSummaries();
    }

    protected String getSchemaLocation(String schemaName)
    {
        return findLocationInQuery("SHOW CREATE SCHEMA " + schemaName);
    }

    private String findLocationInQuery(String query)
    {
        Pattern locationPattern = Pattern.compile(".*location = '(.*?)'.*", Pattern.DOTALL);
        Matcher m = locationPattern.matcher((String) onTrino413().executeQuery(query).getOnlyValue());
        if (m.find()) {
            String location = m.group(1);
            verify(!m.find(), "Unexpected second match");
            return location;
        }
        throw new IllegalStateException("Location not found in" + query + " result");
    }

    @DataProvider
    public static Object[][] tableFormats()
    {
        return new Object[][] {
                // Hive not covered, since Trino 413 used normalized paths for Hive table location (no double slashes)
                {"hive"},
                {"iceberg"},
                {"delta"},
        };
    }

    private static QueryExecutor onTrino413()
    {
        return connectToTrino("trino-413");
    }
}
