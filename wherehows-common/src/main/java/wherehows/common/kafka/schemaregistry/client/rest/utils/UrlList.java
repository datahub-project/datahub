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

package wherehows.common.kafka.schemaregistry.client.rest.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class manages a set of urls for accessing an upstream registry. It basically
 * maintains a pointer to a known good url which can be accessed through {@link #current()}.
 * When a request against the current url fails, the {@link #fail(String)} method is invoked,
 * and we'll move on to the next url (returning back to the start if we have to).
 *
 */
public class UrlList {
  private final AtomicInteger index;
  private final List<String> urls;

  public UrlList(List<String> urls) {
    if (urls == null || urls.isEmpty()) {
      throw new IllegalArgumentException("Expected at least one URL to be passed in constructor");
    }

    this.urls = new ArrayList<String>(urls);
    this.index = new AtomicInteger(new Random().nextInt(urls.size()));
  }

  public UrlList(String url) {
    this(Arrays.asList(url));
  }

  /**
   * Get the current url
   * @return the url
   */
  public String current() {
    return urls.get(index.get());
  }

  /**
   * Declare the given url as failed. This will cause the urls to
   * rotate, so that the next request will be done against a new url
   * (if one exists).
   * @param url the url that has failed
   */
  public void fail(String url) {
    int currentIndex = index.get();
    if (urls.get(currentIndex).equals(url)) {
      index.compareAndSet(currentIndex, (currentIndex+1)%urls.size());
    }
  }

  /**
   * The number of unique urls contained in this collection.
   * @return the count of urls
   */
  public int size() {
    return urls.size();
  }

  @Override
  public String toString() {
    return urls.toString();
  }

}
