package com.linkedin.gms.factory.search;

import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertEquals;

@TestPropertySource(locations = "classpath:/application.yml")
@SpringBootTest(classes = {ElasticSearchIndexBuilderFactory.class})
public class ElasticSearchIndexBuilderFactoryTest extends AbstractTestNGSpringContextTests {
    @Autowired
    ESIndexBuilder test;

    @Test
    void testInjection() {
        assertNotNull(test);
        assertEquals(Map.of(), test.getIndexSettingOverrides());
    }
}
