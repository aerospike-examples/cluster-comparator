package com.aerospike.comparator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.policy.AuthMode;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.util.Util;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import nl.altindag.ssl.SSLFactory;
import nl.altindag.ssl.pem.util.PemUtils;

/**
 * Options parser and storage for controlling the cluster comparator.  
 * @author tfaulkes
 *
 */
public class ClusterComparatorOptions {
    private static final String DEFAULT_DATE_FORMAT = "yyyy/MM/dd-hh:mm:ssZ";
    public static enum Action {
        SCAN(false, true),
        TOUCH(true, false),
        READ(true, false),
        RERUN(true, true),
        SCAN_TOUCH(false, true),
        SCAN_ASK(false, true),
        SCAN_READ(false, true);
        
        private final boolean needsInputFile;
        private final boolean canUseOutputFile;
        private Action(boolean needsInputFile, boolean canUseOutputFile) {
            this.needsInputFile = needsInputFile;
            this.canUseOutputFile = canUseOutputFile;
        }
        public boolean needsInputFile() {
            return this.needsInputFile;
        }
        public boolean canUseOutputFile() {
            return this.canUseOutputFile;
        }
    }
    
    public static enum CompareMode {
        QUICK_NAMESPACE,
        MISSING_RECORDS,
        RECORDS_DIFFERENT,
        RECORD_DIFFERENCES
    }
    
    private boolean console = false;
    private boolean silent = false;
    private CompareMode compareMode = CompareMode.MISSING_RECORDS;
    private int threads = 1;
    private int startPartition = 0;
    private int endPartition = 4096;
    private String[] namespaces;
    private String[] setNames;
    private String outputFileName;
    private String inputFileName;
    private Action action;
    private int rps;
    private String clusterName1;
    private String clusterName2;
    private String hosts1;
    private String hosts2;
    private String userName1;
    private String userName2;
    private String password1;
    private String password2;
    private TlsPolicy tlsPolicy1;
    private TlsPolicy tlsPolicy2;
    private AuthMode authMode1;
    private AuthMode authMode2;
    private boolean servicesAlternate1;
    private boolean servicesAlternate2;
    private long missingRecordsLimit;
    private Date beginDate = null;
    private Date endDate = null;
    private SimpleDateFormat dateFormat = new SimpleDateFormat(DEFAULT_DATE_FORMAT);
    private PathOptions pathOptions = null; 
    private long recordCompareLimit;
    private boolean metadataCompare = false;
    private int remoteServerPort = -1;
    private int remoteServerHeartbeatPort = -1;
    private TlsPolicy remoteServerTls = null;
    private int remoteCacheSize;
    private boolean remoteServerHashes = true;
    private boolean verbose = false;
    private boolean debug = false;
    private boolean sortMaps = false;
    
    static class ParseException extends RuntimeException {
        private static final long serialVersionUID = 5652947902453765251L;

        public ParseException(String message) {
            super(message);
        }
    }
    private SSLFactory parseTlsContext(String tlsContext) {
        String certChain = null;
        String privateKey = null;
        String caCertChain = null;
        String keyPassword = null;
        
        StringWithOffset stringData = new StringWithOffset(tlsContext);
        stringData.checkAndConsumeSymbol('{');
        while (!stringData.isSymbol('}', false)) {
            String subkey = stringData.getString();
            stringData.checkAndConsumeSymbol(':');
            String subValue = stringData.getString();
            if (!stringData.isSymbol('}', false)) {
                stringData.checkAndConsumeSymbol(',');
            }
            switch (subkey) {
            case "certChain":
                certChain = subValue;
                break;
            case "privateKey":
                privateKey = subValue;
                break;
            case "caCertChain":
                caCertChain = subValue;
                break;
            case "keyPassword":
                keyPassword = subValue;
                break;
            default: 
                throw new ParseException("Unexpected key '" + subkey + "' in TLS Context. Valid keys are: 'certChain', 'privateKey', 'caCertChain', and 'keyPassword'");
            }
        }
        
        InputStream certFile = null;
        InputStream keyFile = null;
        InputStream caFile = null;
        try {
            try {
                certFile = new FileInputStream(new File(certChain));
            } catch (FileNotFoundException e) {
                throw new AerospikeException(String.format("certChain file '%s' not found", certChain));
            }
            try {
                keyFile = new FileInputStream(new File(privateKey));
            } catch (FileNotFoundException e) {
                throw new AerospikeException(String.format("privateKey file '%s' not found", certChain));
            }
            try {
                caFile = new FileInputStream(new File(caCertChain));
            } catch (FileNotFoundException e) {
                throw new AerospikeException(String.format("caCertChain file '%s' not found", certChain));
            }
            X509ExtendedKeyManager keyManager = PemUtils.loadIdentityMaterial(certFile, keyFile, keyPassword == null ? null : keyPassword.toCharArray());
            X509ExtendedTrustManager trustManager = PemUtils.loadTrustMaterial(caFile);
    
            SSLFactory sslFactory = SSLFactory.builder()
                    .withIdentityMaterial(keyManager)
                    .withTrustMaterial(trustManager)
                    .build();
            return sslFactory;
        }
        finally {
            if (certFile != null) {
                try {
                    certFile.close();
                } catch (IOException ignored) {}
            }
            if (keyFile != null) {
                try {
                    keyFile.close();
                } catch (IOException ignored) {}
            }
            if (caFile != null) {
                try {
                    caFile.close();
                } catch (IOException ignored) {}
            }
        }
    }
    
    private void setPropertyOnTlsPolicy(TlsPolicy tlsPolicy, String key, String value) {
        switch (key) {
        case "protocols":
            tlsPolicy.protocols = value.split(",");
            break;
        case "ciphers":
            tlsPolicy.ciphers = value.split(",");
            break;
        case "revokeCerts":
            tlsPolicy.revokeCertificates = Util.toBigIntegerArray(value);
            break;
        case "loginOnly":
            tlsPolicy.forLoginOnly = Boolean.parseBoolean(value);
            break;
        case "context":
            tlsPolicy.context = parseTlsContext(value).getSslContext();
            break;
        default: 
            throw new ParseException("Unexpected key '" + key + "' in TLS policy. Valid keys are: 'protocols', 'ciphers', 'revokeCerts', 'context' and 'loginOnly'");
        }
    }
    
    private TlsPolicy parseTlsPolicy(String tlsPolicy) {
        if (tlsPolicy != null) {
            TlsPolicy policy = new TlsPolicy();
            StringWithOffset stringData = new StringWithOffset(tlsPolicy);
            if (stringData.isSymbol('{')) {
                while (true) {
                    String key = stringData.getString();
                    if (key != null) {
                        stringData.checkAndConsumeSymbol(':');
                        String value = stringData.getString();
                        setPropertyOnTlsPolicy(policy, key, value);
                    }
                    if (stringData.isSymbol('}')) {
                        break;
                    }
                    else {
                        stringData.checkAndConsumeSymbol(',');
                    }
                }
                
            }
            return policy;
        }
        return null;
    }
    
    private Options formOptions() {
        Options options = new Options();
        options.addOption("S", "startPartition", true, "Partition to start the comparison at. (Default: 0)");
        options.addOption("E", "endPartition", true, "Partition to end the comparison at. The comparsion will not include this partition. (Default: 4096)");
        options.addOption("t", "threads", true, "Number of threads to use. Use 0 to use 1 thread per core. (Default: 1)");
        options.addOption("f", "file", true, "Path to a CSV file. If a comparison is run, this file will be overwritten if present.");
        options.addOption("s", "setNames", true, "Set name to scan for differences. Multiple sets can be specified in a comma-separated list. If not specified, all sets will be scanned.");
        options.addOption("n", "namespaces", true, "Namespaces to scan for differences. Multiple namespaces can be specified in a comma-separated list. Must include at least one namespace.");
        options.addOption("q", "quiet", false, "Do not output spurious information like progress.");
        options.addOption("C", "compareMode", true, "Determine which sort of comparison to use. The options are:\n"
                + "QUICK_NAMESPACE - Perform a quick (partition by partition count) comparison of an entire namespace. Cannot be used if migrations are going on or not all partitions are available. NOTE:"
                + "This method compares object counts at a partition level, so is not always accurate. A partition which has record A on one side and record B on the other side would compare"
                + "as equal for example. Also, since this compares partition by partition, comparison must be at the namesapce level, not the set level.\n"
                + "MISSING_RECORDS (default) -- Check digests on both sides to find missing records. Does not compare record contents and does not need to read records off the drive. This "
                + "is fast but will not detect if the contents of the records are different.\n"
                + "RECORDS_DIFFERENT -- Runs through all records and detects both missing records on either side and if record contents themselves are different. This will read all the records "
                + "off the drives to be able to compare contents. This will only detect that records are different and not show the record differences\n"
                + "RECORD_DIFFERENCES -- Similar to RECORDS_DIFFERENT but will comprehensively inspect record pairs to determine the differences and show them.");
        options.addOption("u", "usage", false, "Display the usage and exit.");
        options.addOption("a", "action", true, "Action to take. Options are: 'scan' (scan for differences), 'touch' (touch the records specified in the file), 'read' (read the records in specified file), "
                + ", 'scan_touch' (scan for differences, if any differences then automatically touch the records), 'scan_read' (scan for differences, if any differences then automatically read the record), "
                + "'scan_ask' (scan for differences, if any differences then prompt the user, 'rerun' (read all the records from the previous run and see if they're still different. Requires an input file)"
                + "as to whether to touch or read the records or not. Every options besides 'scan' MUST specify the 'file' option too. (Default: scan)");
        options.addOption("c", "console", false, "Output differences to the console. 'quiet' flag does not affect what is output. Can be used in conjunction with 'file' flag for dual output");
        options.addOption("l", "limit", true, "Limit the number of differences to the passed value. Pass 0 for unlimited. (Default: 0)");
        options.addOption("h1", "hosts1", true, 
                "List of seed hosts for first cluster in format: " +
                        "hostname1[:tlsname][:port1],...\n" +
                        "The tlsname is only used when connecting with a secure TLS enabled server. " +
                        "If the port is not specified, the default port is used. " +
                        "IPv6 addresses must be enclosed in square brackets.\n" +
                        "Default: localhost\n" +
                        "Examples:\n" +
                        "host1\n" +
                        "host1:3000,host2:3000\n" +
                        "192.168.1.10:cert1:3000,[2001::1111]:cert2:3000\n");
        options.addOption("h2", "hosts2", true, 
                "List of seed hosts for second cluster in format: " +
                        "hostname1[:tlsname][:port1],...\n" +
                        "The tlsname is only used when connecting with a secure TLS enabled server. " +
                        "If the port is not specified, the default port is used. " +
                        "IPv6 addresses must be enclosed in square brackets.\n" +
                        "Default: localhost\n" +
                        "Examples:\n" +
                        "host1\n" +
                        "host1:3000,host2:3000\n" +
                        "192.168.1.10:cert1:3000,[2001::1111]:cert2:3000\n");
        options.addOption("U1", "user1", true, "User name for cluster 1");
        options.addOption("P1", "password1", true, "Password for cluster 1");
        options.addOption("U2", "user2", true, "User name for cluster 2");
        options.addOption("P2", "password2", true, "Password for cluster 2");
        options.addOption("r", "rps", true, "Limit requests per second on the cluster to this value. Use 0 for unlimited. (Default: 0)");
        options.addOption("t1", "tls1", true, "Set the TLS Policy options on cluster 1. The value passed should be a JSON string. Valid keys in this "
                + "string inlcude 'protocols', 'ciphers', 'revokeCerts', 'context' and 'loginOnly'. For 'context', the value should be a JSON string which "
                + "can contain keys 'certChain' (path to the certificate chain PEM), 'privateKey' (path to the certificate private key PEM), "
                + "'caCertChain' (path to the CA certificate PEM), 'keyPassword' (password used for the certificate chain PEM), 'tlsHost' (the tlsName of the Aerospike host). "
                + "For example: --tls1 '{\"context\":{\"certChain\":\"cert.pem\",\"privateKey\":\"key.pem\",\"caCertChain\":\"cacert.pem\",\"tlsHost\":\"tls1\"}}'");
        options.addOption("t2", "tls2", true, "Set the TLS Policy options on cluster 2. The value passed should be a JSON string. Valid keys in this"
                + "string inlcude 'protocols', 'ciphers', 'revokeCerts', 'context' and 'loginOnly'. For 'context', the value should be a JSON string which "
                + "can contain keys 'certChain' (path to the certificate chain PEM), 'privateKey' (path to the certificate private key PEM), "
                + "'caCertChain' (path to the CA certificate PEM), 'keyPassword' (password used for the certificate chain PEM), 'tlsHost' (the tlsName of the Aerospike host). "
                + "For example: --tls2 '{\"context\":{\"certChain\":\"cert.pem\",\"privateKey\":\"key.pem\",\"caCertChain\":\"cacert.pem\",\"tlsHost\":\"tls2\"}}'");
        options.addOption("a1", "authMode1", true, "Set the auth mode of cluster1. Default: INTERNAL");
        options.addOption("a2", "authMode2", true, "Set the auth mode of cluster2. Default: INTERNAL");
        options.addOption("cn1", "clusterName1", true, "Set the cluster name of cluster 1");
        options.addOption("cn2", "clusterName2", true, "Set the cluster name of cluster 2");
        options.addOption("sa1", "useServicesAlternate1", false, "Use services alternative when connecting to cluster 1");
        options.addOption("sa2", "useServicesAlternate2", false, "Use services alternative when connecting to cluster 2");
        options.addOption("db", "beginDate", true, "Specify the begin date of the scan. Any records whose last update time is this time or greater will be included in the scan. The format of the date is "
                + "by default "+DEFAULT_DATE_FORMAT+" but can be changed with -df flag. If the parameter is a just a number this will be treated as the epoch since 1/1/1970. If the end date "
                + "is also specified, only records falling between the 2 dates will be scanned. Default: scan from the start of time.");
        options.addOption("de", "endDate", true, "Specify the end date of the scan. Any records whose last update time is less than or equal to this time will be included in the scan. The format of the date is "
                + "by default "+DEFAULT_DATE_FORMAT+" but can be changed with -df flag. If the parameter is a just a number this will be treated as the epoch since 1/1/1970. If the start date "
                + "is also specified, only records falling between the 2 dates will be scanned. Default: scan until the end of time.");
        options.addOption("df", "dateFormat", true, "Format used to convert the dates passed with the -db and -de flags. Should conform to the spec of SimpleDateFormat.");
        options.addOption("pf", "pathOptionsFile", true, "YAML file used to contain path options. The options are used to determine whether to ignore paths or "
                + "compare list paths order insensitive.");
        options.addOption("rl", "recordLimit", true, "The maximum number of records to compare. Specify 0 for unlimited records (default)");
        options.addOption("m", "metadataCompare", false, "Perform a meta-data comparison between the 2 clusters");
        options.addOption("rs", "remoteServer", true, "This comparator instance is to be used as a remote server. That is, its operations will be controlled by another "
                + "comparator instance, and they will communicate over a socket. Note that in this mode, only host 1 is connected, any parameters associated with host 2 "
                + "will be silently ignored. This is useful when there is no single node which can see both clusters due to firewalls, NAT restrictions etc. To connect to "
                + "this remoteServer from the main comparator specify a host address of 'remote:<this_host_ip>:<port>. The port is specified as a parameter to this argument. "
                + "If using TLS, the -remoteServerTls parameter is also required for the server to get the appropriate certificates.\n"
                + "This argument takes 1 or 2 parameters in the format port,[heartbeatPort]. If the heartbeat port is specified, it is non-TLS enabled and just accepts connections"
                + " then echoes back any characters received. It can only handle one heartbeat at a time.");
        options.addOption("rst", "remoteServerTls", true, "TLS options for the remote server. Use the same format as -tls1, but only the context is needed");
        options.addOption("rcs", "remoteCacheSize", true, "When using a remote cache, set a buffer size to more efficiently transfer records from the "
                + "remote server to this comparator. Note this parameter only has an effect if >= 4");
        options.addOption("rsh", "remoteServerHashes", true, "When using the remote server, send hashes for record comparison. Default: true. Turning this to false might be more "
                + "efficient if you are finding record level differences and there are a lot of mismatching records.");
        options.addOption("V", "verbose", false, "Turn on verbose logging, especially for cluster details and TLS connections");
        options.addOption("D", "debug", false, "Turn on debug mode. This will output a lot of information and automatically turn on verbose mode and turn silent mode off");
        options.addOption("sm", "sortMaps", true, "Sort maps. If using hashes to compare a local cluster with a remote cluster and the order in the maps is different, the hashes will be different. "
                + "This can lead to false positives, especially when using RECORDS_DIFFERENT which relies on the hashes being accurate. RECORD_DIFFERENCES mode is not susceptible to this as it "
                + "first compares hashes and if they're different will transfer the whole record and find any differences. Hence if the hash is wrong due to order differences but the contents are "
                + "identical, no record will be flagged in this mode. This flag will cause more CPU usage on both the remote comparator and the main comparator but will make sure the hashes are "
                + "consistent irrespective of the underlying order of any maps. This flag only makes sense to set when using a remote comparator, especially with RECORDS_DIFFERENT mode. Default is false,"
                + "unless using a remote server with RECORDS_DIFFERENT mode and remoteServerHashes set to true.");
        options.addOption("i", "inputFile", true, "Specify an input file for records to compare. This is only used with the RERUN, READ and TOUCH actions and is "
                + "typically set to the output file of a previous run.");
        return options;
    }

    private void usage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        String syntax = ClusterComparator.class.getName() + " [<options>]";
        formatter.printHelp(pw, 100, syntax, "options:", options, 0, 2, null);
        System.out.println(sw.toString());
        System.exit(1);
    }
    
    private void validate(Options options) {
        boolean valid = false;
        if (this.isRemoteServer()) {
            if (this.hosts1 == null) {
                System.out.println("host1 must be specified to give connection details to the first cluster");
            }
            else {
                valid = true;
            }
        }
        else {
            if (this.threads < 0) {
                System.out.println("threads must be >= 0, not " + this.threads);
            }
            else if (this.startPartition < 0 || this.startPartition >= 4096) {
                System.out.println("startPartition must be >= 0 and < 4096, not " + this.startPartition);
            }
            else if (this.endPartition < 0 || this.endPartition > 4096) {
                System.out.println("endPartition must be >= 0 and <= 4096, not " + this.endPartition);
            }
            else if (this.endPartition <= this.startPartition) {
                System.out.println("endPartition must be > startPartition " + this.endPartition);
            }
            else if (this.namespaces == null || this.namespaces.length == 0) {
                System.out.println("namespace(s) must be specified");
            }
            else if ((this.action == Action.SCAN_ASK || this.action == Action.TOUCH || this.action == Action.READ || this.action == Action.RERUN) && this.outputFileName == null) {
                System.out.println("If action is not 'scan' or 'scan_touch' or 'scan_read', the fileName must also be specified");
            }
            else if (this.action == Action.RERUN && this.compareMode == CompareMode.QUICK_NAMESPACE) {
                System.out.println("Re-running is not supported for QUICK_NAMESPACE compare mode");
            }
            else if (this.rps < 0) {
                System.out.println("RPS must be >= 0, not " + this.rps);
            }
            else if (this.hosts1 == null) {
                System.out.println("host1 must be specified to give connection details to the first cluster");
            }
            else if (this.hosts2 == null) {
                System.out.println("host1 must be specified to give connection details to the second cluster");
            }
            else if (this.missingRecordsLimit < 0) {
                System.out.println("Records limit must be >= 0, not " + this.missingRecordsLimit);
            }
            else if (this.isQuickCompare() && this.setNames != null) {
                System.out.println("Quick compare cannot be used in conjunction with sets, it can only be used with namespace comparisons.");
            }
            else if (this.endDate != null && this.beginDate != null && this.beginDate.compareTo(this.endDate) > 0) {
                System.out.println("Start date (" + this.dateFormat.format(beginDate) + ") must be before the end date (" + this.dateFormat.format(endDate) + ")");
            }
            else if ((this.endDate != null || this.beginDate != null) && this.isQuickCompare()) {
                System.out.println("Date ranges for the scans cannot be used with quick compare");
            }
            else if (this.action.needsInputFile && this.inputFileName == null) {
                System.out.printf("Action %s requires an input file but none was provided.\n", this.action);
            }
            else {
                valid = true;
            }
            if (this.action == Action.RERUN && remoteCacheSize > 0) {
                System.out.println("Disabling caching from remote server when using RERUN as the action.");
            }
            if (!this.action.canUseOutputFile && this.outputFileName != null) {
                System.out.printf("Action %s cannot use an output file, but one was provided. Ignoring and continuing.\n", this.action);
            }
        }
        if (!valid) {
            usage(options);
        }
    }
    
    public ClusterComparatorOptions(String[] arguments) throws Exception {
        Options options = formOptions();
        CommandLineParser parser = new DefaultParser();
        CommandLine cl = parser.parse(options, arguments, false);
        SimpleDateFormat sdf = null;
        
        if (cl.hasOption("usage")) {
            usage(options);
        }
        this.silent = cl.hasOption("quiet");
        this.threads = Integer.valueOf(cl.getOptionValue("threads", "1"));
        this.startPartition = Integer.valueOf(cl.getOptionValue("startPartition", "0"));
        this.endPartition = Integer.valueOf(cl.getOptionValue("endPartition", "4096"));
        if (cl.hasOption("namespaces")) {
            this.namespaces = cl.getOptionValue("namespaces").split(",");
        }
        if (cl.hasOption("setNames")) {
            this.setNames = cl.getOptionValue("setNames").split(",");
        }
        this.outputFileName = cl.getOptionValue("file");
        this.inputFileName = cl.getOptionValue("inputFile");
        this.action = Action.valueOf(cl.getOptionValue("action", "scan").toUpperCase());
        this.rps = Integer.valueOf(cl.getOptionValue("rps","0"));
        this.hosts1 = cl.getOptionValue("hosts1");
        this.hosts2 = cl.getOptionValue("hosts2");
        this.userName1 = cl.getOptionValue("user1");
        this.userName2 = cl.getOptionValue("user2");
        this.password1 = cl.getOptionValue("password1");
        this.password2 = cl.getOptionValue("password2");
        this.tlsPolicy1 = parseTlsPolicy(cl.getOptionValue("tls1"));
        this.tlsPolicy2 = parseTlsPolicy(cl.getOptionValue("tls2"));
        this.authMode1 = AuthMode.valueOf(cl.getOptionValue("authMode1", "INTERNAL").toUpperCase());
        this.authMode2 = AuthMode.valueOf(cl.getOptionValue("authMode2", "INTERNAL").toUpperCase());
        this.clusterName1 = cl.getOptionValue("clusterName1");
        this.clusterName2 = cl.getOptionValue("clusterName2");
        this.servicesAlternate1 = cl.hasOption("useServicesAlternate1");
        this.servicesAlternate2 = cl.hasOption("useServicesAlternate2");
        this.console = cl.hasOption("console");
        this.missingRecordsLimit = Long.valueOf(cl.getOptionValue("limit", "0"));
        this.compareMode = CompareMode.valueOf(cl.getOptionValue("compareMode", CompareMode.MISSING_RECORDS.toString()).toUpperCase());
        if (cl.hasOption("dateFormat")) {
            sdf = new SimpleDateFormat(cl.getOptionValue("dateFormat"));
        }
        if (cl.hasOption("beginDate")) {
            String value = cl.getOptionValue("beginDate");
            if (value.matches("^\\d+$")) {
                this.beginDate = new Date(Long.parseLong(value));
            }
            else {
                this.beginDate = sdf.parse(value);
            }
        }
        if (cl.hasOption("endDate")) {
            String value = cl.getOptionValue("endDate");
            if (value.matches("^\\d+$")) {
                this.beginDate = new Date(Long.parseLong(value));
            }
            else {
                this.beginDate = sdf.parse(value);
            }
        }
        String pathOptionsFile = cl.getOptionValue("pathOptionsFile");
        if (pathOptionsFile != null) {
            ObjectMapper mapper = YAMLMapper.builder().enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS).build();
            mapper.findAndRegisterModules();
            this.pathOptions = mapper.readValue(new File (pathOptionsFile), PathOptions.class);
        }
        this.recordCompareLimit = Long.valueOf(cl.getOptionValue("recordLimit", "0"));
        if (cl.hasOption("metadataCompare")) {
            this.metadataCompare = true;
        }
        String portString = cl.getOptionValue("remoteServer", "-1");
        String[] ports = portString.split(",");
        this.remoteServerPort = Integer.valueOf(ports[0]);
        if (ports.length == 2) {
            this.remoteServerHeartbeatPort = Integer.valueOf(ports[1]);
        }
        this.remoteServerTls = parseTlsPolicy(cl.getOptionValue("remoteServerTls"));
        this.remoteCacheSize = Integer.valueOf(cl.getOptionValue("remoteCacheSize", "0"));
        this.remoteServerHashes = Boolean.valueOf(cl.getOptionValue("remoteServerHashes", "true"));
        this.verbose = cl.hasOption("verbose");
        this.debug = cl.hasOption("debug");
        if (this.debug) {
            this.silent = false;
            this.verbose = true;
        }
        if (cl.hasOption("sortMaps")) {
            this.sortMaps = Boolean.valueOf(cl.getOptionValue("sortMaps"));
        }
        else {
            this.sortMaps = (this.compareMode == CompareMode.RECORDS_DIFFERENT) && this.isRemoteServerHashes();
        }
        if (this.compareMode == CompareMode.QUICK_NAMESPACE && this.remoteCacheSize >= 4) {
            System.out.println("Remote caching is incompatible with QUICK_NAMESPACE mode, turning off caching.");
            this.remoteCacheSize = 0;
        }

        this.validate(options);
    }

    public boolean isSilent() {
        return silent;
    }

    public int getThreads() {
        return threads;
    }

    public int getStartPartition() {
        return startPartition;
    }

    public int getEndPartition() {
        return endPartition;
    }

    public String[] getNamespaces() {
        return namespaces;
    }

    public String[] getSetNames() {
        return setNames;
    }

    public String getOutputFileName() {
        return outputFileName;
    }

    public Action getAction() {
        return action;
    }

    public int getRps() {
        return rps;
    }

    public String getHosts1() {
        return hosts1;
    }

    public String getHosts2() {
        return hosts2;
    }

    public String getUserName1() {
        return userName1;
    }

    public String getUserName2() {
        return userName2;
    }

    public String getPassword1() {
        return password1;
    }

    public String getPassword2() {
        return password2;
    }

    public TlsPolicy getTlsPolicy1() {
        return tlsPolicy1;
    }

    public TlsPolicy getTlsPolicy2() {
        return tlsPolicy2;
    }

    public AuthMode getAuthMode1() {
        return authMode1;
    }

    public AuthMode getAuthMode2() {
        return authMode2;
    }
    
    public String getClusterName1() {
        return clusterName1;
    }
    
    public String getClusterName2() {
        return clusterName2;
    }
    public boolean isConsole() {
        return console;
    }
    
    public long getMissingRecordsLimit() {
        return missingRecordsLimit;
    }
    
    public boolean isQuickCompare() {
        return this.compareMode == CompareMode.QUICK_NAMESPACE;
    }
    
    public boolean isRecordLevelCompare() {
        return this.compareMode == CompareMode.RECORD_DIFFERENCES || this.compareMode == CompareMode.RECORDS_DIFFERENT;
    }
    
    public CompareMode getCompareMode() {
        return compareMode;
    }
    
    public Date getBeginDate() {
        return beginDate;
    }
    
    public Date getEndDate() {
        return endDate;
    }
    
    public SimpleDateFormat getDateFormat() {
        return dateFormat;
    }
    
    public PathOptions getPathOptions() {
        return pathOptions;
    }
    
    public void setPathOptions(PathOptions pathOptions) {
        this.pathOptions = pathOptions;
    }
    
    public long getRecordCompareLimit() {
        return recordCompareLimit;
    }
    
    public boolean isMetadataCompare() {
        return metadataCompare;
    }
    
    public boolean isServicesAlternate1() {
        return servicesAlternate1;
    }
    
    public boolean isServicesAlternate2() {
        return servicesAlternate2;
    }
    
    public boolean isRemoteServer() {
        return this.remoteServerPort > 0;
    }
    
    public int getRemoteServerPort() {
        return remoteServerPort;
    }
    
    public int getRemoteServerHeartbeatPort() {
        return remoteServerHeartbeatPort;
    }
    
    public TlsPolicy getRemoteServerTls() {
        return remoteServerTls;
    }
    
    public int getRemoteCacheSize() {
        return remoteCacheSize;
    }
    
    public boolean isRemoteServerHashes() {
        return remoteServerHashes;
    }
    
    public boolean isVerbose() {
        return verbose;
    }
    
    public boolean isDebug() {
        return debug;
    }
    
    public boolean isSortMaps() {
        return sortMaps;
    }
    
    public String getInputFileName() {
        return inputFileName;
    }
}

