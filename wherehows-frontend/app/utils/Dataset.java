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
package utils;

import java.util.ArrayList;
import java.util.List;
import wherehows.models.table.User;


public class Dataset {

  private Dataset() {
  }

  public static List<User> fillDatasetOwnerList(String[] owners, String[] ownerNames, String[] ownerEmails) {
    List<User> users = new ArrayList<>();

    if (owners != null && ownerNames != null && ownerEmails != null && owners.length == ownerNames.length
        && owners.length == ownerEmails.length) {
      for (int i = 0; i < owners.length; i++) {
        User user = new User();
        user.setUserName(owners[i]);
        user.setName(ownerNames[i]);
        user.setEmail(ownerEmails[i]);
        users.add(user);
      }
    }
    return users;
  }
}
