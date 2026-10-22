package com.aerospike.comparator.web;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.aerospike.comparator.ConfigOptions;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

public class WebServerYamlTest {

    @Test
    void passwordWithQuotesAndNewlinesRoundTrips() throws Exception {
        String password = "p\"ass\\word\nnext: line";
        Map<String, Object> cluster = new HashMap<>();
        cluster.put("hostName", "h1:3000");
        cluster.put("userName", "us\"er");
        cluster.put("password", password);
        Map<String, Object> other = new HashMap<>();
        other.put("hostName", "h2:3000");
        List<Map<String, Object>> clusters = new ArrayList<>();
        clusters.add(cluster);
        clusters.add(other);

        File file = WebServer.writeClusterConfig(clusters);
        String yaml = Files.readString(file.toPath());
        assertFalse(yaml.contains("password: \"" + password + "\""));

        ObjectMapper mapper = YAMLMapper.builder().enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS).build();
        ConfigOptions loaded = mapper.readValue(file, ConfigOptions.class);
        assertEquals(password, loaded.getClusters().get(0).getPassword());
        assertEquals("us\"er", loaded.getClusters().get(0).getUserName());
        assertEquals("h2:3000", loaded.getClusters().get(1).getHostName());
    }
}
