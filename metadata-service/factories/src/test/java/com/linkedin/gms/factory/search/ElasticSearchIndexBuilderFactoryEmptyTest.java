package com.linkedin.gms.factory.search;

import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@PropertySource("classpath:/test-empty-application.yml")
@SpringBootTest(
        properties = {
                "elasticsearch.index.settingsOverrides=",
                "elasticsearch.index.entitySettingsOverrides=",
                "elasticsearch.index.prefix=test_prefix"
        },
        classes = {ElasticSearchIndexBuilderFactory.class})
public class ElasticSearchIndexBuilderFactoryEmptyTest extends AbstractTestNGSpringContextTests {
    @Autowired
    ESIndexBuilder test;

    @Test
    void testInjection() {
        assertNotNull(test);
        assertEquals(Map.of(), test.getIndexSettingOverrides());
    }
}
