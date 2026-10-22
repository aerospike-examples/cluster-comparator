package com.aerospike.comparator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class CsvDifferenceHandlerTest {

    @Test
    void quickCompareWritesPartitionRows(@TempDir File tempDir) throws Exception {
        File outputFile = new File(tempDir, "quick-compare.csv");
        ClusterComparatorOptions options = new ClusterComparatorOptions(new String[] {
                "--hosts1", "h1:3000",
                "--hosts2", "h2:3000",
                "--namespaces", "production",
                "--action", "scan",
                "--compareMode", "QUICK_NAMESPACE",
                "--file", outputFile.getAbsolutePath()
        });

        CsvDifferenceHandler handler = new CsvDifferenceHandler(outputFile.getAbsolutePath(), options);
        handler.handle("production", 4091, new long[] {1005, 1002}, new long[] {5, 7}, new long[] {1000, 995});
        handler.close();

        List<String> lines = Files.readAllLines(outputFile.toPath());
        assertEquals(2, lines.size());
        assertEquals(handler.getFileHeader(), lines.get(0));
        assertTrue(lines.get(1).contains("production,,4091,,"));
        assertTrue(lines.get(1).contains("\"Partition net object count mismatch:"));
        assertTrue(lines.get(1).contains("PARTITION_COUNT"));
        assertTrue(lines.get(1).contains("1005"));
        assertTrue(lines.get(1).contains("1002"));
        assertTrue(lines.get(1).contains("1000"));
        assertTrue(lines.get(1).contains("995"));
    }
}
