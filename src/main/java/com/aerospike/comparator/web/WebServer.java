package com.aerospike.comparator.web;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.AuthMode;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.comparator.ClusterComparatorOptions;
import com.aerospike.comparator.dbaccess.AerospikeClientAccess;
import com.aerospike.comparator.dbaccess.LocalAerospikeClient;
import com.aerospike.comparator.dbaccess.RemoteAerospikeClient;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.javalin.Javalin;
import io.javalin.http.staticfiles.Location;
import io.javalin.util.JavalinLogger;

public class WebServer {
    private final ClusterComparatorOptions initialOptions;
    private final ComparisonSession session = new ComparisonSession();
    private final Map<String, Object> currentOptions;
    private final ConcurrentHashMap<String, Boolean> validTokens = new ConcurrentHashMap<>();
    private final ObjectMapper mapper = new ObjectMapper();
    private final AtomicInteger populateCurrent = new AtomicInteger(0);
    private final AtomicInteger populateTotal = new AtomicInteger(0);
    private final AtomicBoolean populateCancelRequested = new AtomicBoolean(false);

    private static final String[] FIRST_NAMES = {"Alice","Bob","Charlie","Diana","Eve","Frank","Grace","Hank","Ivy","Jack",
            "Karen","Leo","Mia","Nick","Olivia","Paul","Quinn","Rosa","Sam","Tina"};
    private static final String[] LAST_NAMES = {"Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Wilson","Moore",
            "Taylor","Anderson","Thomas","Jackson","White","Harris","Martin","Clark","Lewis","Walker"};
    private static final String[] TAGS = {"vip","newsletter","beta-tester","premium","early-adopter","referral","loyalty","seasonal","corporate","student"};

    /** Deterministic sample record so two clusters populated with the same options match byte-for-byte on bins. */
    private static void fillCustomerBins(int keyNum, Random rng, boolean constant,
            String[] outFirst, String[] outLast, List<String> outTags, Map<String, String> outPrefs,
            int[] outAge, double[] outBalance, boolean[] outActive, long[] outCreatedAt, int[] outLoyalty) {
        if (constant) {
            int kn = keyNum;
            outFirst[0] = FIRST_NAMES[Math.floorMod(kn, FIRST_NAMES.length)];
            outLast[0] = LAST_NAMES[Math.floorMod(kn * 3 + 5, LAST_NAMES.length)];
            outTags.clear();
            int tagCount = 2 + Math.floorMod(kn, 3);
            for (int t = 0; t < tagCount; t++) {
                String tag = TAGS[Math.floorMod(kn + t * 2, TAGS.length)];
                if (!outTags.contains(tag)) {
                    outTags.add(tag);
                }
            }
            outPrefs.clear();
            outPrefs.put("theme", Math.floorMod(kn, 2) == 0 ? "dark" : "light");
            outPrefs.put("language", Math.floorMod(kn, 2) == 0 ? "en" : "es");
            outPrefs.put("notifications", Math.floorMod(kn, 2) == 0 ? "email" : "sms");
            outAge[0] = 18 + Math.floorMod(kn, 62);
            outBalance[0] = Math.round((kn * 997L % 10000000L) / 100.0) / 100.0;
            outActive[0] = Math.floorMod(kn, 10) < 8;
            outCreatedAt[0] = 1704067200000L + (Math.floorMod(kn, 365 * 5) * 86400000L);
            outLoyalty[0] = Math.floorMod(kn * 991, 50000);
        } else {
            outFirst[0] = FIRST_NAMES[rng.nextInt(FIRST_NAMES.length)];
            outLast[0] = LAST_NAMES[rng.nextInt(LAST_NAMES.length)];
            outTags.clear();
            int tagCount = 1 + rng.nextInt(4);
            for (int t = 0; t < tagCount; t++) {
                String tag = TAGS[rng.nextInt(TAGS.length)];
                if (!outTags.contains(tag)) {
                    outTags.add(tag);
                }
            }
            outPrefs.clear();
            outPrefs.put("theme", rng.nextBoolean() ? "dark" : "light");
            outPrefs.put("language", rng.nextBoolean() ? "en" : "es");
            outPrefs.put("notifications", rng.nextBoolean() ? "email" : "sms");
            outAge[0] = 18 + rng.nextInt(62);
            outBalance[0] = Math.round(rng.nextDouble() * 100000.0) / 100.0;
            outActive[0] = rng.nextInt(10) < 8;
            outCreatedAt[0] = System.currentTimeMillis() - rng.nextInt(365 * 24 * 3600) * 1000L;
            outLoyalty[0] = rng.nextInt(50000);
        }
    }

    private static String customerUserKey(int keyNum) {
        return "customer-" + keyNum;
    }

    public WebServer(ClusterComparatorOptions options) {
        this.initialOptions = options;
        this.currentOptions = new ConcurrentHashMap<>(options.toOptionsMap());
    }

    public void start() {
        Javalin app = Javalin.create(config -> {
            config.showJavalinBanner = false;
            config.staticFiles.add(staticFiles -> {
                staticFiles.hostedPath = "/";
                staticFiles.directory = "/webapp";
                staticFiles.location = Location.CLASSPATH;
            });
            config.spaRoot.addFile("/", "/webapp/index.html", Location.CLASSPATH);
        });

        if (initialOptions.getWebPassword() != null) {
            app.before("/api/*", ctx -> {
                if (ctx.path().equals("/api/auth") || ctx.path().equals("/api/auth-required")) {
                    return;
                }
                String authHeader = ctx.header("Authorization");
                if (authHeader == null || !authHeader.startsWith("Bearer ")) {
                    ctx.status(401).json(Map.of("error", "Authentication required"));
                    ctx.skipRemainingHandlers();
                    return;
                }
                String token = authHeader.substring(7);
                if (!validTokens.containsKey(token)) {
                    ctx.status(401).json(Map.of("error", "Invalid token"));
                    ctx.skipRemainingHandlers();
                }
            });
        }

        app.get("/api/auth-required", ctx -> {
            ctx.json(Map.of("required", initialOptions.getWebPassword() != null));
        });

        app.get("/api/jvm-tls-params", ctx -> {
            try {
                javax.net.ssl.SSLParameters params = javax.net.ssl.SSLContext.getDefault().getSupportedSSLParameters();
                Map<String, Object> body = new HashMap<>();
                body.put("protocols", Arrays.asList(params.getProtocols()));
                body.put("ciphers", Arrays.asList(params.getCipherSuites()));
                ctx.json(body);
            } catch (Exception e) {
                ctx.status(500).json(Map.of("error", e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()));
            }
        });

        app.post("/api/auth", ctx -> {
            if (initialOptions.getWebPassword() == null) {
                ctx.json(Map.of("token", "none"));
                return;
            }
            Map<?, ?> body = ctx.bodyAsClass(Map.class);
            String password = (String) body.get("password");
            if (initialOptions.getWebPassword().equals(password)) {
                String token = UUID.randomUUID().toString();
                validTokens.put(token, true);
                ctx.json(Map.of("token", token));
            } else {
                ctx.status(401).json(Map.of("error", "Invalid password"));
            }
        });

        app.get("/api/options", ctx -> {
            ctx.json(currentOptions);
        });

        app.put("/api/options", ctx -> {
            @SuppressWarnings("unchecked")
            Map<String, Object> newOptions = ctx.bodyAsClass(Map.class);
            currentOptions.clear();
            currentOptions.putAll(newOptions);
            ctx.json(Map.of("status", "ok"));
        });

        app.post("/api/start", ctx -> {
            List<String> args = buildArgsFromOptions(currentOptions);
            String[] argsArray = args.toArray(new String[0]);
            try {
                ClusterComparatorOptions tempOpts = new ClusterComparatorOptions(argsArray, true);
                Map<String, String> fieldErrors = tempOpts.validateAndCollectErrors();
                if (!fieldErrors.isEmpty()) {
                    Map<String, Object> response = new HashMap<>();
                    response.put("error", "Validation failed");
                    response.put("fieldErrors", fieldErrors);
                    ctx.status(400).json(response);
                    return;
                }
            } catch (Exception e) {
                ctx.status(400).json(Map.of("error", "Invalid options: " + e.getMessage()));
                return;
            }
            String error = session.start(argsArray);
            if (error != null) {
                ctx.status(400).json(Map.of("error", error));
            } else {
                ctx.json(Map.of("status", "started"));
            }
        });

        app.post("/api/stop", ctx -> {
            session.stop();
            ctx.json(Map.of("status", "stopped"));
        });

        app.post("/api/reset", ctx -> {
            session.reset();
            ctx.json(Map.of("status", "reset"));
        });

        app.get("/api/results-history", ctx -> {
            ctx.json(session.getCompletedRuns());
        });

        app.delete("/api/results-history/{index}", ctx -> {
            int index = Integer.parseInt(ctx.pathParam("index"));
            session.removeCompletedRun(index);
            ctx.json(Map.of("status", "deleted"));
        });

        app.get("/api/status", ctx -> {
            Map<String, Object> status = new HashMap<>();
            status.put("state", session.getState().name());
            if (session.getErrorMessage() != null) {
                status.put("error", session.getErrorMessage());
            }
            ProgressSnapshot progress = session.getProgress();
            if (progress != null) {
                status.put("progress", progress);
            }
            ctx.json(status);
        });

        app.post("/api/test-connection", ctx -> {
            @SuppressWarnings("unchecked")
            Map<String, Object> body = ctx.bodyAsClass(Map.class);
            try {
                Map<String, Object> result = testConnection(body);
                ctx.json(result);
            } catch (Exception e) {
                ctx.status(400).json(Map.of("success", false, "error", e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()));
            }
        });

        app.post("/api/cluster-metadata", ctx -> {
            @SuppressWarnings("unchecked")
            Map<String, Object> body = ctx.bodyAsClass(Map.class);
            try {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> clusterList = (List<Map<String, Object>>) body.get("clusters");
                Map<String, Object> result = fetchClusterMetadata(clusterList);
                ctx.json(result);
            } catch (Exception e) {
                ctx.status(400).json(Map.of("error", e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()));
            }
        });

        app.post("/api/populate-cancel", ctx -> {
            populateCancelRequested.set(true);
            ctx.json(Map.of("status", "cancelRequested"));
        });

        app.post("/api/populate-data", ctx -> {
            @SuppressWarnings("unchecked")
            Map<String, Object> body = ctx.bodyAsClass(Map.class);
            try {
                populateCurrent.set(0);
                populateTotal.set(0);
                populateCancelRequested.set(false);
                Map<String, Object> result = populateData(body);
                ctx.json(result);
            } catch (Exception e) {
                ctx.status(400).json(Map.of("success", false, "error", e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()));
            } finally {
                populateCurrent.set(0);
                populateTotal.set(0);
                populateCancelRequested.set(false);
            }
        });

        app.get("/api/populate-progress", ctx -> {
            ctx.json(Map.of("current", populateCurrent.get(), "total", populateTotal.get()));
        });

        app.sse("/api/progress", client -> {
            client.keepAlive();
            Thread sseThread = new Thread(() -> {
                try {
                    while (true) {
                        ProgressSnapshot progress = session.getProgress();
                        if (progress != null) {
                            String json = mapper.writeValueAsString(progress);
                            client.sendEvent("progress", json);
                        } else {
                            client.sendEvent("progress", "{\"state\":\"IDLE\"}");
                        }
                        Thread.sleep(1000);
                    }
                } catch (Exception e) {
                    // SSE connection closed or error; thread exits
                }
            }, "SSE-Progress");
            sseThread.setDaemon(true);
            sseThread.start();
            client.onClose(sseThread::interrupt);
        });

        int port = initialOptions.getWebInterfacePort();
        // Javalin logs a "your version is N days old" INFO line once the JAR is >120 days past its
        // build timestamp (not a runtime error). Disable startup INFO to avoid alarming operators.
        JavalinLogger.startupInfo = false;
        app.start(port);
        System.out.printf("Web interface started on http://localhost:%d\n", port);

        Runtime.getRuntime().addShutdownHook(new Thread(app::stop));
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            app.stop();
        }
    }

    private AerospikeClientAccess connectFromMap(Map<String, Object> clusterMap) {
        String hostName = (String) clusterMap.get("hostName");
        if (hostName == null || hostName.isEmpty()) {
            throw new IllegalArgumentException("Host name is required");
        }
        if (hostName.startsWith("remote:")) {
            String[] parts = hostName.split(":");
            if (parts.length != 3) {
                throw new IllegalArgumentException(
                    "Remote proxy address must be 'remote:<host>:<port>', but received '" + hostName + "'");
            }
            try {
                return new RemoteAerospikeClient(parts[1], Integer.parseInt(parts[2]), null);
            } catch (Exception e) {
                throw new RuntimeException("Failed to connect to remote proxy: " + e.getMessage(), e);
            }
        }

        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.user = (String) clusterMap.get("userName");
        clientPolicy.password = (String) clusterMap.get("password");
        String authModeStr = (String) clusterMap.get("authMode");
        if (authModeStr != null && !authModeStr.isEmpty()) {
            clientPolicy.authMode = AuthMode.valueOf(authModeStr.toUpperCase());
        }
        String cn = (String) clusterMap.get("clusterName");
        clientPolicy.clusterName = (cn != null && !cn.trim().isEmpty()) ? cn : null;
        Object saObj = clusterMap.get("useServicesAlternate");
        if (saObj instanceof Boolean) {
            clientPolicy.useServicesAlternate = (Boolean) saObj;
        }
        clientPolicy.timeout = 5000;
        clientPolicy.loginTimeout = 5000;
        Host[] hosts = Host.parseHosts(hostName, 3000);
        return new LocalAerospikeClient(new AerospikeClient(clientPolicy, hosts));
    }

    private Map<String, Object> testConnection(Map<String, Object> clusterMap) {
        AerospikeClientAccess client = connectFromMap(clusterMap);
        try {
            List<String> nodeNames = client.getNodeNames();
            Map<String, String> nsResults = client.invokeInfoCommandOnAllNodes("namespaces");
            Map<String, String> buildResults = client.invokeInfoCommandOnAllNodes("build");
            Map<String, String> cnResults = client.invokeInfoCommandOnAllNodes("cluster-name");

            Set<String> namespaces = new HashSet<>();
            Set<String> versions = new HashSet<>();
            Set<String> clusterNames = new HashSet<>();

            for (String nsStr : nsResults.values()) {
                if (nsStr != null && !nsStr.isEmpty()) {
                    namespaces.addAll(Arrays.asList(nsStr.split(";")));
                }
            }
            for (String build : buildResults.values()) {
                if (build != null && !build.isEmpty()) {
                    versions.add(build);
                }
            }
            for (String cn : cnResults.values()) {
                if (cn != null && !cn.isEmpty()) {
                    clusterNames.add(cn);
                }
            }

            Map<String, Object> result = new HashMap<>();
            result.put("success", true);
            result.put("nodes", nodeNames.size());
            result.put("namespaces", new ArrayList<>(namespaces));
            result.put("versions", new ArrayList<>(versions));
            if (!clusterNames.isEmpty()) {
                result.put("clusterName", clusterNames.iterator().next());
            }
            return result;
        } finally {
            client.close();
        }
    }

    private Map<String, Object> fetchClusterMetadata(List<Map<String, Object>> clusterList) {
        Set<String> allNamespaces = new HashSet<>();
        Set<String> allSets = new HashSet<>();
        for (Map<String, Object> clusterMap : clusterList) {
            String hostName = (String) clusterMap.get("hostName");
            if (hostName == null || hostName.trim().isEmpty()) continue;
            AerospikeClientAccess client = null;
            try {
                client = connectFromMap(clusterMap);
                String nsStr = client.invokeInfoCommandOnANode("namespaces");
                if (nsStr != null && !nsStr.isEmpty()) {
                    for (String ns : nsStr.split(";")) {
                        allNamespaces.add(ns);
                        String setsStr = client.invokeInfoCommandOnANode("sets/" + ns);
                        if (setsStr != null && !setsStr.isEmpty()) {
                            for (String setEntry : setsStr.split(";")) {
                                for (String field : setEntry.split(":")) {
                                    if (field.startsWith("set_name=") || field.startsWith("set=")) {
                                        allSets.add(field.substring(field.indexOf('=') + 1));
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                // skip unreachable clusters
            } finally {
                if (client != null) client.close();
            }
        }
        Map<String, Object> result = new HashMap<>();
        result.put("namespaces", new ArrayList<>(allNamespaces));
        result.put("sets", new ArrayList<>(allSets));
        return result;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> populateData(Map<String, Object> body) {
        Map<String, Object> clusterMap = (Map<String, Object>) body.get("cluster");
        String namespace = (String) body.get("namespace");
        String setName = (String) body.get("set");
        int count = body.get("count") != null ? ((Number) body.get("count")).intValue() : 100;
        int startKey = body.get("startKey") != null ? ((Number) body.get("startKey")).intValue() : 1;
        String modeRaw = body.get("generationMode") != null ? body.get("generationMode").toString().trim() : "random";
        boolean constant = modeRaw.equalsIgnoreCase("constant");

        if (clusterMap == null) throw new IllegalArgumentException("cluster config is required");
        if (namespace == null || namespace.isEmpty()) throw new IllegalArgumentException("namespace is required");
        if (count <= 0 || count > 100000) throw new IllegalArgumentException("count must be between 1 and 100000");
        if (startKey < 0) throw new IllegalArgumentException("startKey must be >= 0");
        if ((long) startKey + (long) count - 1L > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("startKey + count - 1 overflows int");
        }
        if (!modeRaw.equalsIgnoreCase("constant") && !modeRaw.equalsIgnoreCase("random")) {
            throw new IllegalArgumentException("generationMode must be 'constant' or 'random'");
        }

        AerospikeClientAccess client = connectFromMap(clusterMap);
        try {
            WritePolicy wp = new WritePolicy();
            wp.totalTimeout = 5000;
            wp.sendKey = true;
            Random rng = new Random();
            int lastKey = startKey + count - 1;
            populateTotal.set(count);

            String[] firstHolder = new String[1];
            String[] lastHolder = new String[1];
            int[] ageHolder = new int[1];
            double[] balanceHolder = new double[1];
            boolean[] activeHolder = new boolean[1];
            long[] createdHolder = new long[1];
            int[] loyaltyHolder = new int[1];

            for (int n = 0; n < count; n++) {
                if (populateCancelRequested.get()) {
                    int written = n;
                    Map<String, Object> result = new HashMap<>();
                    result.put("success", false);
                    result.put("cancelled", true);
                    result.put("recordsWritten", written);
                    if (written > 0) {
                        result.put("firstKey", customerUserKey(startKey));
                        result.put("lastKey", customerUserKey(startKey + written - 1));
                    }
                    return result;
                }
                int keyNum = startKey + n;
                String keyStr = customerUserKey(keyNum);
                Key key = new Key(namespace, setName, keyStr);

                List<String> tagList = new ArrayList<>();
                Map<String, String> prefs = new HashMap<>();
                fillCustomerBins(keyNum, rng, constant, firstHolder, lastHolder, tagList, prefs,
                        ageHolder, balanceHolder, activeHolder, createdHolder, loyaltyHolder);
                String firstName = firstHolder[0];
                String lastName = lastHolder[0];

                client.put(wp, key,
                    new Bin("firstName", firstName),
                    new Bin("lastName", lastName),
                    new Bin("email", firstName.toLowerCase() + "." + lastName.toLowerCase() + keyNum + "@example.com"),
                    new Bin("age", ageHolder[0]),
                    new Bin("balance", balanceHolder[0]),
                    new Bin("active", activeHolder[0]),
                    new Bin("createdAt", createdHolder[0]),
                    new Bin("tags", Value.get(tagList)),
                    new Bin("preferences", Value.get(prefs)),
                    new Bin("loyaltyPoints", loyaltyHolder[0])
                );
                populateCurrent.set(n + 1);
            }
            Map<String, Object> result = new HashMap<>();
            result.put("success", true);
            result.put("recordsWritten", count);
            result.put("cancelled", false);
            result.put("firstKey", customerUserKey(startKey));
            result.put("lastKey", customerUserKey(lastKey));
            return result;
        } finally {
            client.close();
        }
    }

    @SuppressWarnings("unchecked")
    private List<String> buildArgsFromOptions(Map<String, Object> opts) {
        List<String> args = new ArrayList<>();
        Object clustersObj = opts.get("clusters");

        if (clustersObj instanceof List) {
            List<Map<String, Object>> clusters = (List<Map<String, Object>>) clustersObj;
            if (!clusters.isEmpty()) {
                try {
                    File tempFile = File.createTempFile("cc-config-", ".yaml");
                    tempFile.deleteOnExit();
                    StringBuilder yaml = new StringBuilder();
                    yaml.append("clusters:\n");
                    for (Map<String, Object> c : clusters) {
                        yaml.append("  - hostName: \"").append(nullSafe(c.get("hostName"))).append("\"\n");
                        if (c.get("clusterName") != null && !c.get("clusterName").toString().isEmpty()) {
                            yaml.append("    clusterName: \"").append(c.get("clusterName")).append("\"\n");
                        }
                        if (c.get("userName") != null && !c.get("userName").toString().isEmpty()) {
                            yaml.append("    userName: \"").append(c.get("userName")).append("\"\n");
                        }
                        if (c.get("password") != null && !c.get("password").toString().isEmpty()) {
                            yaml.append("    password: \"").append(c.get("password")).append("\"\n");
                        }
                        if (c.get("authMode") != null && !c.get("authMode").toString().isEmpty()) {
                            yaml.append("    authMode: ").append(c.get("authMode")).append("\n");
                        }
                        Object sa = c.get("useServicesAlternate");
                        if (sa instanceof Boolean && (Boolean) sa) {
                            yaml.append("    useServicesAlternate: true\n");
                        }
                    }
                    try (FileWriter fw = new FileWriter(tempFile)) {
                        fw.write(yaml.toString());
                    }
                    args.add("--configFile");
                    args.add(tempFile.getAbsolutePath());
                } catch (Exception e) {
                    throw new RuntimeException("Failed to write temp config: " + e.getMessage(), e);
                }
            }
        }

        for (Map.Entry<String, Object> entry : opts.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if ("clusters".equals(key)) continue;
            if (value == null || "".equals(value.toString())) {
                continue;
            }
            if (value instanceof Boolean) {
                if ((Boolean) value) {
                    args.add("--" + key);
                }
            } else {
                args.add("--" + key);
                args.add(value.toString());
            }
        }
        return args;
    }

    private static String nullSafe(Object obj) {
        return obj == null ? "" : obj.toString();
    }
}
