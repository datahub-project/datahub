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
package dataquality.utils;

import java.util.Arrays;


/**
 * Created by zechen on 8/4/15.
 */
public class StringUtil {

  /**
   * s1 and s2 are comma separated strings
   * check if s1 contains all elements in s2
   * @param s1
   * @param s2
   */
  public static boolean containsAll(String s1, String s2) {
    if (s2 == null) {
      return true;
    }

    if (s1 == null) {
      return false;
    }

    String[] a1 = s1.split(",");
    String[] a2 = s2.split(",");

    return Arrays.asList(a1).containsAll(Arrays.asList(a2));
  }
}
