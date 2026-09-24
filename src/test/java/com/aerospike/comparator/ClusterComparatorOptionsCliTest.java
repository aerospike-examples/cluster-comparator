package com.aerospike.comparator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.junit.jupiter.api.Test;

/**
 * Guards the Commons CLI catalog against the failure mode that dropped
 * {@code --sourceCluster} and {@code --skipDateRangeVerify}: values were still
 * read from {@code CommandLine}, but {@code formOptions()} no longer registered them.
 */
public class ClusterComparatorOptionsCliTest {

    private static final Pattern PARSED_OPTION_NAME = Pattern.compile(
            "cl\\.(?:getOptionValue|hasOption)\\(\"([^\"]+)\"");

    /** Parsed in the constructor but deliberately not advertised on the CLI. */
    private static final Set<String> INTENTIONALLY_UNREGISTERED = Set.of("masterCluster");

    private static final Set<String> OPTIONS_THAT_LOAD_FILES = Set.of("configFile", "pathOptionsFile");

    @Test
    void everyOptionReadFromCommandLineIsRegistered() throws Exception {
        Set<String> registered = registeredLongNames();
        Set<String> parsed = optionNamesReadFromCommandLine();

        List<String> missing = new ArrayList<>();
        for (String name : parsed) {
            if (INTENTIONALLY_UNREGISTERED.contains(name)) {
                continue;
            }
            if (!registered.contains(name)) {
                missing.add(name);
            }
        }
        assertTrue(missing.isEmpty(),
                "These options are read from CommandLine but missing from formOptions(): " + missing);
    }

    @Test
    void intentionallyUnregisteredOptionsAreNotOnTheCliCatalog() {
        Set<String> registered = registeredLongNames();
        for (String name : INTENTIONALLY_UNREGISTERED) {
            assertFalse(registered.contains(name),
                    name + " is marked as intentionally unregistered but is in formOptions()");
        }
    }

    @Test
    void eachRegisteredOptionCanBeParsed() throws Exception {
        Path configFile = Files.createTempFile("cc-config", ".yaml");
        Path pathOptionsFile = Files.createTempFile("cc-paths", ".yaml");
        Files.writeString(configFile, "{}\n");
        Files.writeString(pathOptionsFile, "paths: []\n");

        try {
            Options catalog = ClusterComparatorOptions.formOptions();
            for (Option option : catalog.getOptions()) {
                String longName = option.getLongOpt();
                List<String> args = new ArrayList<>(List.of(
                        "--hosts1", "h1:3000",
                        "--hosts2", "h2:3000",
                        "--namespaces", "test",
                        "--action", "scan"));
                args.add("--" + longName);
                if (option.hasArg()) {
                    args.add(dummyValue(longName, configFile, pathOptionsFile));
                }
                new ClusterComparatorOptions(args.toArray(new String[0]), true);
            }
        }
        finally {
            Files.deleteIfExists(configFile);
            Files.deleteIfExists(pathOptionsFile);
        }
    }

    @Test
    void unrecognizedOptionIsRejected() {
        RuntimeException thrown = assertThrows(RuntimeException.class, () ->
                new ClusterComparatorOptions(new String[] {
                        "--hosts1", "h1:3000",
                        "--hosts2", "h2:3000",
                        "--namespaces", "test",
                        "--notARealFlag"
                }, true));
        assertTrue(thrown.getMessage().contains("notARealFlag"), thrown.getMessage());
    }

    private static Set<String> registeredLongNames() {
        Set<String> names = new LinkedHashSet<>();
        for (Option option : ClusterComparatorOptions.formOptions().getOptions()) {
            names.add(option.getLongOpt());
        }
        return names;
    }

    private static Set<String> optionNamesReadFromCommandLine() throws Exception {
        Path source = Path.of("src/main/java/com/aerospike/comparator/ClusterComparatorOptions.java");
        String text = Files.readString(source);
        Matcher matcher = PARSED_OPTION_NAME.matcher(text);
        Set<String> names = new LinkedHashSet<>();
        while (matcher.find()) {
            names.add(matcher.group(1));
        }
        assertFalse(names.isEmpty(), "Did not find any cl.getOptionValue/hasOption calls in ClusterComparatorOptions");
        return names;
    }

    private static String dummyValue(String longName, Path configFile, Path pathOptionsFile) {
        switch (longName) {
        case "configFile":
            return configFile.toString();
        case "pathOptionsFile":
            return pathOptionsFile.toString();
        case "compareMode":
            return "MISSING_RECORDS";
        case "action":
            return "scan";
        case "authMode1":
        case "authMode2":
            return "INTERNAL";
        case "remoteServerHashes":
        case "sortMaps":
            return "true";
        case "customActions":
            return "1:none";
        case "tls1":
        case "tls2":
        case "remoteServerTls":
            return "{}";
        case "dateFormat":
            return "yyyy-MM-dd";
        case "beginDate":
        case "endDate":
            return "1";
        case "partitionList":
            return "0";
        case "remoteServer":
            return "-1";
        case "webInterfacePort":
            return "8080";
        case "threads":
        case "startPartition":
        case "endPartition":
        case "rps":
        case "limit":
        case "recordLimit":
        case "lookupBatchSize":
        case "remoteCacheSize":
        case "sourceCluster":
            return "1";
        default:
            return OPTIONS_THAT_LOAD_FILES.contains(longName) ? configFile.toString() : "dummy";
        }
    }
}
