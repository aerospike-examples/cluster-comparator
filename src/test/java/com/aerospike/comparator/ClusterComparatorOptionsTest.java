package com.aerospike.comparator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import com.aerospike.comparator.ClusterComparatorOptions.CustomActions;

import org.junit.jupiter.api.Test;

public class ClusterComparatorOptionsTest {

    @Test
    void resolveThreadsToUse_capsDefaultAutoThreads() {
        assertEquals(32, ClusterComparatorOptions.resolveThreadsToUse(0, 112));
        assertEquals(16, ClusterComparatorOptions.resolveThreadsToUse(0, 16));
    }

    @Test
    void resolveThreadsToUse_usesAllCoresWhenMinusOne() {
        assertEquals(112, ClusterComparatorOptions.resolveThreadsToUse(-1, 112));
    }

    @Test
    void resolveThreadsToUse_honorsExplicitPositiveCount() {
        assertEquals(150, ClusterComparatorOptions.resolveThreadsToUse(150, 112));
    }

    @Test
    void isValidThreadsValue_rejectsInvalidNegativeValues() {
        assertTrue(ClusterComparatorOptions.isValidThreadsValue(-1));
        assertTrue(ClusterComparatorOptions.isValidThreadsValue(0));
        assertTrue(ClusterComparatorOptions.isValidThreadsValue(32));
        assertFalse(ClusterComparatorOptions.isValidThreadsValue(-2));
    }

    @Test
    void clusterIdToName_usesOneBasedOrdinalsWhenClusterNameNotSet() throws Exception {
        ClusterComparatorOptions options = new ClusterComparatorOptions(new String[] {
                "--hosts1", "h1:3000",
                "--hosts2", "h2:3000",
                "--namespaces", "test",
                "--action", "scan"
        });

        assertEquals("1", options.clusterIdToName(0));
        assertEquals("2", options.clusterIdToName(1));
    }

    @Test
    void clusterIdToName_usesConfiguredClusterNamesWhenSet() throws Exception {
        ClusterComparatorOptions options = new ClusterComparatorOptions(new String[] {
                "--hosts1", "h1:3000",
                "--hosts2", "h2:3000",
                "--clusterName1", "source",
                "--clusterName2", "target",
                "--namespaces", "test",
                "--action", "scan"
        });

        assertEquals("\"source\"", options.clusterIdToName(0));
        assertEquals("\"target\"", options.clusterIdToName(1));
    }

    @Test
    void sourceCluster_acceptsOneBasedOrdinalOnCommandLine() throws Exception {
        ClusterComparatorOptions options = new ClusterComparatorOptions(new String[] {
                "--hosts1", "h1:3000",
                "--hosts2", "h2:3000",
                "--namespaces", "test",
                "--action", "scan",
                "--sourceCluster", "1"
        });
        assertEquals(0, options.getSourceCluster());
        assertTrue(options.hasSourceCluster());
    }

    @Test
    void sourceCluster_acceptsShortFlagAndClusterName() throws Exception {
        ClusterComparatorOptions options = new ClusterComparatorOptions(new String[] {
                "--hosts1", "h1:3000",
                "--hosts2", "h2:3000",
                "--clusterName1", "primary",
                "--clusterName2", "replica",
                "--namespaces", "test",
                "--action", "scan",
                "-sc", "primary"
        });
        assertEquals(0, options.getSourceCluster());
    }

    @Test
    void skipDateRangeVerify_isAcceptedOnCommandLine() throws Exception {
        String[] base = {
                "--hosts1", "h1:3000",
                "--hosts2", "h2:3000",
                "--namespaces", "test",
                "--action", "scan",
                "--beginDate", "1"
        };
        ClusterComparatorOptions verifying = new ClusterComparatorOptions(base);
        assertTrue(verifying.isDateRangeVerify());

        String[] skipped = new String[base.length + 1];
        System.arraycopy(base, 0, skipped, 0, base.length);
        skipped[base.length] = "--skipDateRangeVerify";
        ClusterComparatorOptions skippedOpts = new ClusterComparatorOptions(skipped);
        assertFalse(skippedOpts.isDateRangeVerify());

        skipped[base.length] = "-sdv";
        ClusterComparatorOptions shortFlag = new ClusterComparatorOptions(skipped);
        assertFalse(shortFlag.isDateRangeVerify());
    }

    @Test
    void rerun_rejectsInputAndOutputThatAreTheSameFile() throws Exception {
        Path dir = Files.createTempDirectory("cc-same-file");
        Path file = dir.resolve("results.csv");
        Path viaParent = dir.resolve("..").resolve(dir.getFileName().toString()).resolve("results.csv");

        ClusterComparatorOptions options = rerunOptions(file.toString(), viaParent.toString());
        assertTrue(options.validateAndCollectErrors().containsKey("inputFile"));
    }

    @Test
    void rerun_allowsDistinctInputAndOutputFiles() throws Exception {
        ClusterComparatorOptions options = rerunOptions("previous.csv", "next.csv");
        assertFalse(options.validateAndCollectErrors().containsKey("inputFile"));
    }

    @Test
    void touch_ignoresAnOutputFileEvenWhenItMatchesTheInput() throws Exception {
        ClusterComparatorOptions options = new ClusterComparatorOptions(new String[] {
                "--hosts1", "h1:3000",
                "--hosts2", "h2:3000",
                "--namespaces", "test",
                "--action", "touch",
                "--inputFile", "records.csv",
                "--file", "records.csv"
        }, true);
        assertFalse(options.validateAndCollectErrors().containsKey("inputFile"));
    }

    @Test
    void customActions_resolveClusterNamesAfterClustersExist() throws Exception {
        ClusterComparatorOptions options = new ClusterComparatorOptions(new String[] {
                "--hosts1", "h1:3000",
                "--hosts2", "h2:3000",
                "--clusterName1", "source",
                "--clusterName2", "target",
                "--namespaces", "test",
                "--action", "custom",
                "--customActions", "source:touch,target:none",
                "--inputFile", "in.csv"
        }, true);
        assertFalse(options.validateAndCollectErrors().containsKey("customActions"));
        assertEquals(CustomActions.TOUCH, options.getCustomActions().get(0));
        assertEquals(CustomActions.NONE, options.getCustomActions().get(1));
    }

    @Test
    void setMapping_requiresSetNames() throws Exception {
        Path config = Files.createTempFile("set-mapping", ".yaml");
        Files.writeString(config, "clusters:\n"
                + "  - hostName: h1:3000\n"
                + "    clusterName: source\n"
                + "  - hostName: h2:3000\n"
                + "    clusterName: target\n"
                + "setMapping:\n"
                + "  - set: users\n"
                + "    mappings:\n"
                + "      - clusterIndex: 2\n"
                + "        name: accounts\n");
        ClusterComparatorOptions options = new ClusterComparatorOptions(new String[] {
                "--configFile", config.toString(),
                "--namespaces", "test",
                "--action", "scan",
                "--sourceCluster", "source"
        }, true);
        Map<String, String> errors = options.validateAndCollectErrors();
        assertTrue(errors.containsKey("setNames"));
        assertFalse(errors.containsKey("sourceCluster"));
    }

    @Test
    void webValidation_allowsPartitionListWhenTheRangeIsLeftAtTheDefaults() throws Exception {
        ClusterComparatorOptions options = new ClusterComparatorOptions(new String[] {
                "--hosts1", "h1:3000",
                "--hosts2", "h2:3000",
                "--namespaces", "test",
                "--partitionList", "1,2",
                "--startPartition", "0",
                "--endPartition", "4096"
        }, true);
        assertFalse(options.validateAndCollectErrors().containsKey("partitionList"));
    }

    @Test
    void webValidation_rejectsSourceClusterWithQuickCompareAndAnUnknownCluster() throws Exception {
        ClusterComparatorOptions quick = new ClusterComparatorOptions(new String[] {
                "--hosts1", "h1:3000",
                "--hosts2", "h2:3000",
                "--namespaces", "test",
                "--compareMode", "QUICK_NAMESPACE",
                "--sourceCluster", "1"
        }, true);
        assertTrue(quick.validateAndCollectErrors().containsKey("sourceCluster"));

        ClusterComparatorOptions unknown = new ClusterComparatorOptions(new String[] {
                "--hosts1", "h1:3000",
                "--hosts2", "h2:3000",
                "--namespaces", "test",
                "--sourceCluster", "9"
        }, true);
        assertTrue(unknown.validateAndCollectErrors().containsKey("sourceCluster"));
    }

    @Test
    void webValidation_requiresCustomActionsOnlyForCustomActions() throws Exception {
        ClusterComparatorOptions missing = new ClusterComparatorOptions(new String[] {
                "--hosts1", "h1:3000",
                "--hosts2", "h2:3000",
                "--namespaces", "test",
                "--action", "scan_custom"
        }, true);
        assertTrue(missing.validateAndCollectErrors().containsKey("customActions"));

        ClusterComparatorOptions forbidden = new ClusterComparatorOptions(new String[] {
                "--hosts1", "h1:3000",
                "--hosts2", "h2:3000",
                "--namespaces", "test",
                "--action", "scan",
                "--customActions", "1:touch"
        }, true);
        assertTrue(forbidden.validateAndCollectErrors().containsKey("customActions"));
    }

    @Test
    void webValidation_rejectsBatchSizeAndNonDefaultRangeWithPartitionList() throws Exception {
        ClusterComparatorOptions options = new ClusterComparatorOptions(new String[] {
                "--hosts1", "h1:3000",
                "--hosts2", "h2:3000",
                "--namespaces", "test",
                "--lookupBatchSize", "0",
                "--partitionList", "1,2",
                "--endPartition", "100"
        }, true);
        Map<String, String> errors = options.validateAndCollectErrors();
        assertTrue(errors.containsKey("lookupBatchSize"));
        assertTrue(errors.containsKey("partitionList"));
    }

    private static ClusterComparatorOptions rerunOptions(String input, String output) throws Exception {
        return new ClusterComparatorOptions(new String[] {
                "--hosts1", "h1:3000",
                "--hosts2", "h2:3000",
                "--namespaces", "test",
                "--action", "rerun",
                "--inputFile", input,
                "--file", output
        }, true);
    }
}
