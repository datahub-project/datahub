package com.linkedin.gms.factory.search;

import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertNotNull;

@PropertySource("classpath:/test-empty-application.yml")
@SpringBootTest(classes = {ElasticSearchIndexBuilderFactory.class})
public class ElasticSearchIndexBuilderFactoryEmptyTest extends AbstractTestNGSpringContextTests {
    @Autowired
    ESIndexBuilder test;

    @Test
    void testInjection() {
        assertNotNull(test);
        assertNull(test.getIndexSettingOverrides());
    }
}
