package com.linkedin.metadata.dao.browse;

import java.util.Arrays;
import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class BrowseDAOTest {
  @Test
  public void testMatchingPaths() {
    List<String> browsePaths = Arrays.asList("/all/subscriptions/premium_new_signups_v2/subs_new_bookings",
        "/certified/lls/subs_new_bookings",
        "/certified/lls/lex/subs_new_bookings",
        "/certified/lls/consumer/subs_new_bookings",
        "/subs_new_bookings",
        "/School/Characteristics/General/Embedding/network_standardized_school_embeddings_v3"
    );

    // Scenario 1: inside /Certified/LLS
    String path1 = "/certified/lls";
    assertEquals(ESBrowseDAO.getNextLevelPath(browsePaths, path1), "/certified/lls/subs_new_bookings");

    // Scenario 2: inside /Certified/LLS/Consumer
    String path2 = "/certified/lls/consumer";
    assertEquals(ESBrowseDAO.getNextLevelPath(browsePaths, path2), "/certified/lls/consumer/subs_new_bookings");

    // Scenario 3: inside root directory
    String path3 = "";
    assertEquals(ESBrowseDAO.getNextLevelPath(browsePaths, path3), "/subs_new_bookings");

    // Scenario 4: inside an incorrect path /foo
    // this situation should ideally not arise for entity browse queries
    String path4 = "/foo";
    assertNull(ESBrowseDAO.getNextLevelPath(browsePaths, path4));

    // Scenario 5: one of the browse paths isn't normalized
    String path5 = "/school/characteristics/general/embedding";
    assertEquals(ESBrowseDAO.getNextLevelPath(browsePaths, path5),
        "/School/Characteristics/General/Embedding/network_standardized_school_embeddings_v3");

    // Scenario 6: current path isn't normalized, which ideally should not be the case
    String path6 = "/School/Characteristics/General/Embedding";
    assertEquals(ESBrowseDAO.getNextLevelPath(browsePaths, path6),
        "/School/Characteristics/General/Embedding/network_standardized_school_embeddings_v3");
  }
}
