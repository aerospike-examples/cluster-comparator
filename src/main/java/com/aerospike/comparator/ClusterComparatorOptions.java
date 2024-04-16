package com.aerospike.comparator;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import com.aerospike.client.policy.AuthMode;
import com.aerospike.client.policy.TlsPolicy;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

/**
 * Options parser and storage for controlling the cluster comparator.  
 * @author tfaulkes
 *
 */
public class ClusterComparatorOptions implements ClusterNameResolver {
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
    
    private List<ClusterConfig> clusters = null;
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
    private TlsOptions tlsOptions1;
    private TlsOptions tlsOptions2;
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
    private ConfigOptions configOptions = null;
    private boolean binsOnly = false;
    private boolean showMetadata = false;
    private int masterCluster = -1;
    private List<Integer> partitionList = null;
    
    static class ParseException extends RuntimeException {
        private static final long serialVersionUID = 5652947902453765251L;

        public ParseException(String message) {
            super(message);
        }
    }
    
    private SSLOptions parseTlsContext(String tlsContext) {
        SSLOptions options = new SSLOptions();
        
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
                options.setCertChain(subValue);
                break;
            case "privateKey":
                options.setPrivateKey(subValue);
                break;
            case "caCertChain":
                options.setCaCertChain(subValue);
                break;
            case "keyPassword":
                options.setKeyPassword(subValue);
                break;
            default: 
                throw new ParseException("Unexpected key '" + subkey + "' in TLS Context. Valid keys are: 'certChain', 'privateKey', 'caCertChain', and 'keyPassword'");
            }
        }
        return options;
    }
    
    private void setPropertyOnTlsPolicy(TlsOptions tlsOptions, String key, String value) {
        switch (key) {
        case "protocols":
            tlsOptions.setProtocols(value);
            break;
        case "ciphers":
            tlsOptions.setCiphers(value);
            break;
        case "revokeCerts":
            tlsOptions.setRevokeCertificates(value);
            break;
        case "loginOnly":
            tlsOptions.setLoginOnly(Boolean.parseBoolean(value));
            break;
        case "context":
            tlsOptions.setSsl(parseTlsContext(value));
            break;
        default: 
            throw new ParseException("Unexpected key '" + key + "' in TLS policy. Valid keys are: 'protocols', 'ciphers', 'revokeCerts', 'context' and 'loginOnly'");
        }
    }
    
    private TlsOptions parseTlsOptions(String tlsOptions) {
        if (tlsOptions != null) {
            TlsOptions policy = new TlsOptions();
            StringWithOffset stringData = new StringWithOffset(tlsOptions);
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
        options.addOption("cf", "configFile", true, "YAML file with config options in it");
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
                + "by default "+DEFAULT_DATE_FORMAT+" but can be changed with -df flag. If the parameter is a just a number this will be treated as the number of milliseconds since 1/1/1970. If the end date "
                + "is also specified, only records falling between the 2 dates will be scanned. Default: scan from the start of time.");
        options.addOption("de", "endDate", true, "Specify the end date of the scan. Any records whose last update time is less than or equal to this time will be included in the scan. The format of the date is "
                + "by default "+DEFAULT_DATE_FORMAT+" but can be changed with -df flag. If the parameter is a just a number this will be treated as the number of milliseconds since 1/1/1970. If the start date "
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
        options.addOption(null, "binsOnly", false, "When using RECORDS_DIFFERENT or RECORD_DIFFERENCES, do not list the full differences, just the bin names which are different");
        options.addOption(null, "showMetadata", false, "Output cluster metadata (Last update time, record size) on cluster differernces. This will require an additional read which will impact performance");
        options.addOption("pl", "partitionList", true, "Specify a list of partitions to scan. If this argument is specified, neither the beginPartition nor the endPartition argument can be specified");
//        options.addOption(null, "masterCluster", true, "Sets one cluster tobe the master for the sake of comparison. If this flag is set, the date range filter must be specified (beginDate and/or endDate) "
//                + "and this date range will apply only to the master cluster. ");
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
    
    private boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }
    private boolean validatePathOptions() {
        boolean hasErrors = false;
        for (PathOption path: this.pathOptions.getPaths()) {
            if (isBlank(path.getPath())) {
                if (path.getAction() == null) {
                    System.out.println("Path configurations has an empty element with no path and no action");
                }
                else {
                    System.out.printf("Empty path configuration against an action of %s\n", path.getAction());
                }
                hasErrors = true;
            }
            else if (path.getAction() == null) {
                System.out.printf("Path configuration \"%s\" has an empty element with no path and no action\n", path.getPath());
                hasErrors = true;
            }
        }
        return !hasErrors;
    }
    
    private boolean validatePartitionList() {
        boolean result = true;
        for (int part : this.partitionList) {
            if (part < 0 || part >= 4096) {
                result = false;
                System.out.printf("Partition list contains invalid partition %d.\n", part);
            }
        }
        return result;
    }
    
    private void validate(Options options, CommandLine cl) {
        boolean valid = false;
        boolean hasErrors = false;

        if (this.configOptions != null) {
            this.clusters = this.configOptions.getClusters();
        }
        if (this.clusters == null) {
            this.clusters = new ArrayList<>();
            
            // Create 2 clusters from the 2 explicitly passed parameters
            ClusterConfig cluster = new ClusterConfig();
            cluster.setAuthMode(getAuthMode1());
            cluster.setClusterName(getClusterName1());
            cluster.setHostName(getHosts1());
            cluster.setPassword(getPassword1());
            cluster.setUserName(getUserName1());
            cluster.setUseServicesAlternate(isServicesAlternate1());
            cluster.setTls(getTlsOptions1());
            this.clusters.add(cluster);

            cluster = new ClusterConfig();
            cluster.setAuthMode(getAuthMode2());
            cluster.setClusterName(getClusterName2());
            cluster.setHostName(getHosts2());
            cluster.setPassword(getPassword2());
            cluster.setUserName(getUserName2());
            cluster.setUseServicesAlternate(isServicesAlternate2());
            cluster.setTls(getTlsOptions2());
            this.clusters.add(cluster);
        }
        else {
            // Cluster configs can be specified in the config file or command line options 
            // but not both for now.
            if (this.getHosts1() != null || this.getHosts2() != null) {
                System.out.println("Hosts are specified in the config file so they cannot be passed on the command line as well");
                hasErrors = true;
            }
        }

        if (this.masterCluster >= 0) {
            this.showMetadata = true;
        }
        if (this.isRemoteServer()) {
            if (this.clusters.size() != 1) {
                System.out.printf("One cluster must be specified on the command line (host1 argument) or in the config file");
            }
            else {
                //
                valid = hasErrors;
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
            else if (this.clusters.size() < 2) {
                System.out.printf("At least 2 cluster hosts must be specified on the command line (host1, host2 arguments) or in the config file");
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
            else if (this.pathOptions != null && !validatePathOptions()) {
                System.out.println("Aborting scan due to invalid path options");
            }
            else if (this.partitionList != null && cl.hasOption("startPartition")) {
                System.out.println("Start partition argument cannot be specified if the partitionList argument is specified");
            }
            else if (this.partitionList != null && cl.hasOption("endPartition")) {
                System.out.println("End partition argument cannot be specified if the partitionList argument is specified");
            }
            else if (this.partitionList != null && !validatePartitionList()) {
                System.out.println("Aborting scan due to invalid partition list");
            }
            else {
                valid = !hasErrors;
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
    
    private void loadConfig(String fileName) throws Exception {
        ObjectMapper mapper = YAMLMapper.builder().enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS).build();
        mapper.findAndRegisterModules();
        this.configOptions = mapper.readValue(new File(fileName), ConfigOptions.class);
    }
    
    public ClusterComparatorOptions(String[] arguments) throws Exception {
        Options options = formOptions();
        CommandLineParser parser = new DefaultParser();
        CommandLine cl = parser.parse(options, arguments, false);
        
        if (cl.hasOption("usage")) {
            usage(options);
        }
        if (cl.hasOption("configFile")) {
            loadConfig(cl.getOptionValue("configFile"));
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
        this.tlsOptions1 = parseTlsOptions(cl.getOptionValue("tls1"));
        this.tlsOptions2 = parseTlsOptions(cl.getOptionValue("tls2"));
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
            this.dateFormat = new SimpleDateFormat(cl.getOptionValue("dateFormat"));
        }
        if (cl.hasOption("beginDate")) {
            String value = cl.getOptionValue("beginDate");
            if (value.matches("^\\d+$")) {
                this.beginDate = new Date(Long.parseLong(value));
            }
            else {
                this.beginDate = this.dateFormat.parse(value);
            }
        }
        if (cl.hasOption("endDate")) {
            String value = cl.getOptionValue("endDate");
            if (value.matches("^\\d+$")) {
                this.endDate = new Date(Long.parseLong(value));
            }
            else {
                this.endDate = this.dateFormat.parse(value);
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
        if (cl.hasOption("remoteServerTls")) {
            this.remoteServerTls = parseTlsOptions(cl.getOptionValue("remoteServerTls")).toTlsPolicy();
        }
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
        this.binsOnly = cl.hasOption("binsOnly");
        this.showMetadata = cl.hasOption("showMetadata");
        if (cl.hasOption("partitionList")) {
            this.partitionList = new ArrayList<>();
            String[] parts = cl.getOptionValue("partitionList").split(",");
            for (String s : parts) {
                this.partitionList.add(Integer.parseInt(s));
            }
        }
        
        this.masterCluster = Integer.parseInt(cl.getOptionValue("masterCluster", "-1"));
        this.validate(options, cl);
    }

    public String clusterIdToName(int id) {
        if (id < 0 || id >= this.getClusterConfigs().size()) {
            throw new IllegalArgumentException(String.format("cluster id must be in the range of 0 to %d, not %d", this.getClusterConfigs().size()-1, id));
        }
        String name = this.getClusterConfigs().get(id).getClusterName();
        if (name != null && !name.isEmpty()) {
            return "\"" + name + "\"";
        }
        else {
            return Integer.toString(id+1);
        }
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

    public List<ClusterConfig> getClusterConfigs() {
        return this.clusters;
    }
    
    public ConfigOptions getConfigOptions() {
        return this.getConfigOptions();
    }
    
    private String getHosts1() {
        return hosts1;
    }

    private String getHosts2() {
        return hosts2;
    }

    private String getUserName1() {
        return userName1;
    }

    private String getUserName2() {
        return userName2;
    }

    private String getPassword1() {
        return password1;
    }

    private String getPassword2() {
        return password2;
    }

    private TlsOptions getTlsOptions1() {
        return tlsOptions1;
    }

    private TlsOptions getTlsOptions2() {
        return tlsOptions2;
    }

    private AuthMode getAuthMode1() {
        return authMode1;
    }

    private AuthMode getAuthMode2() {
        return authMode2;
    }
    private String getClusterName1() {
        return clusterName1;
    }
    
    private String getClusterName2() {
        return clusterName2;
    }
    private boolean isServicesAlternate1() {
        return servicesAlternate1;
    }
    
    private boolean isServicesAlternate2() {
        return servicesAlternate2;
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
    
    public boolean isBinsOnly() {
        return binsOnly;
    }
    
    public boolean isShowMetadata() {
        return showMetadata;
    }
    
    public int getMasterCluster() {
        return masterCluster;
    }
    
    public List<Integer> getPartitionList() {
        return partitionList;
    }
    
    public boolean isDateInRange(long timestamp) {
        if (this.getBeginDate() == null && this.getEndDate() == null) {
            // No date range specified
            return true;
        }
        else if (this.getBeginDate() == null && this.getEndDate() != null ) {
            return timestamp < this.getEndDate().getTime();
        }
        else if (this.getBeginDate() != null && this.getEndDate() == null) {
            return timestamp >= this.getBeginDate().getTime();
        }
        else {
            // Both times specified
            return timestamp >= this.getBeginDate().getTime() && timestamp < this.getEndDate().getTime(); 
        }
    }
}

