package com.aerospike.comparator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.aerospike.client.Key;
import com.aerospike.client.command.Buffer;
import com.aerospike.comparator.ClusterComparatorOptions.CompareMode;

public class FileLineTest {

    @Test
    void quotedFieldsAndMetadataColumnsRoundTrip(@TempDir File tempDir) throws Exception {
        File outputFile = new File(tempDir, "diffs.csv");
        ClusterComparatorOptions options = new ClusterComparatorOptions(new String[] {
                "--hosts1", "h1:3000",
                "--hosts2", "h2:3000",
                "--namespaces", "test",
                "--action", "scan",
                "--file", outputFile.getAbsolutePath()
        }, true);
        options.validateAndCollectErrors();

        Key key = new Key("test", "us,ers", "ke,y");
        CsvDifferenceHandler handler = new CsvDifferenceHandler(outputFile.getAbsolutePath(), options);
        handler.handle(CompareMode.MISSING_RECORDS, 7, key, Collections.singletonList(1), false, null);
        handler.close();

        List<String> lines = Files.readAllLines(outputFile.toPath());
        FileLine parsed = new FileLine(lines.get(1), options, false);
        assertEquals("us,ers", parsed.getSetName());
        assertEquals("ke,y", parsed.getUserKey());
        String digest = Buffer.bytesToHexString(key.digest);
        assertEquals(digest, parsed.getDigest(0));
        assertEquals("", parsed.getDigest(1));
        assertArrayEquals(key.digest, parsed.getKey(0).digest);
    }

    @Test
    void commaInAThreeClusterDifferenceDoesNotShiftDigestColumns(@TempDir File tempDir) throws Exception {
        File outputFile = new File(tempDir, "three.csv");
        java.nio.file.Path config = java.nio.file.Files.createTempFile("three-clusters", ".yaml");
        java.nio.file.Files.writeString(config, "clusters:\n"
                + "  - hostName: h1:3000\n"
                + "  - hostName: h2:3000\n"
                + "  - hostName: h3:3000\n");
        ClusterComparatorOptions options = new ClusterComparatorOptions(new String[] {
                "--configFile", config.toString(),
                "--namespaces", "test",
                "--action", "scan",
                "--file", outputFile.getAbsolutePath()
        }, true);
        options.validateAndCollectErrors();

        Key key = new Key("test", "users", "user-1");
        CsvDifferenceHandler handler = new CsvDifferenceHandler(outputFile.getAbsolutePath(), options);
        handler.handle(CompareMode.MISSING_RECORDS, 7, key, java.util.Arrays.asList(1, 2), false, null);
        handler.close();

        String line = Files.readAllLines(outputFile.toPath()).get(1);
        assertTrue(line.contains("\"Missing from clusters: [2, 3]\""));
        FileLine parsed = new FileLine(line, options, false);
        String digest = Buffer.bytesToHexString(key.digest);
        assertEquals(digest, parsed.getDigest(0));
        assertEquals("", parsed.getDigest(1));
        assertEquals("", parsed.getDigest(2));
    }

    @Test
    void keylessRowUsesDigestInsteadOfTheWordNull() throws Exception {
        ClusterComparatorOptions options = new ClusterComparatorOptions(new String[] {
                "--hosts1", "h1:3000",
                "--hosts2", "h2:3000",
                "--namespaces", "test"
        }, true);
        Key source = new Key("test", "users", "stored");
        String digest = Buffer.bytesToHexString(source.digest);
        String line = "test,users,4,null," + digest + ",2024/01/01,10,1,0," + digest + ",2024/01/01,10,1,0,diff";
        FileLine parsed = new FileLine(line, options, true);
        assertEquals(digest, parsed.getDigest(0));
        assertEquals(digest, parsed.getDigest(1));
        assertArrayEquals(source.digest, parsed.getKey().digest);
        assertNull(parsed.getKey().userKey);
    }
}
