/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package wherehows.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import org.testng.annotations.Test;
import wherehows.utils.ProcessorUtil;

import static org.testng.Assert.*;


public class ProcessorUtilTest {

  @Test
  public void testListDiffWithExclusion() {
    List<String> existing = new ArrayList<>(Arrays.asList("a", "b", "c"));
    List<String> updated = new ArrayList<>(Arrays.asList("b", "d"));

    assertEquals(ProcessorUtil.listDiffWithExclusion(existing, updated, Collections.emptyList()),
        Arrays.asList("a", "c"));

    List<Pattern> exclusions = Collections.singletonList(Pattern.compile("a"));

    assertEquals(ProcessorUtil.listDiffWithExclusion(existing, updated, exclusions), Arrays.asList("c"));
  }
}
