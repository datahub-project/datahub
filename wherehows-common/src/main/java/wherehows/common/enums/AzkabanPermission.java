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
package wherehows.common.enums;

/**
 * Created by zechen on 9/22/15.
 */

import java.util.HashSet;
import java.util.Set;

public class AzkabanPermission {
  public enum Type {
    READ(0x0000001),
    WRITE(0x0000002),
    EXECUTE(0x0000004),
    SCHEDULE(0x0000008),
    METRICS(0x0000010),
    CREATEPROJECTS(0x40000000), // Only used for roles
    ADMIN(0x8000000);

    private int numVal;

    Type(int numVal) {
      this.numVal = numVal;
    }

    public int getFlag() {
      return numVal;
    }
  }

  private Set<Type> permissions = new HashSet<Type>();

  public AzkabanPermission(int flags) {
    setPermissions(flags);
  }

  public void setPermissions(int flags) {
    permissions.clear();
    if ((flags & Type.ADMIN.getFlag()) != 0) {
      addPermission(Type.ADMIN);
    } else {
      for (Type type : Type.values()) {
        if ((flags & type.getFlag()) != 0) {
          addPermission(type);
        }
      }
    }
  }

  public void addPermission(Type... list) {
    // Admin is all encompassing permission. No need to add other types
    if (!permissions.contains(Type.ADMIN)) {
      for (Type perm : list) {
        permissions.add(perm);
      }
      // We add everything, and if there's Admin left, we make sure that only
      // Admin is remaining.
      if (permissions.contains(Type.ADMIN)) {
        permissions.clear();
        permissions.add(Type.ADMIN);
      }
    }
  }


  public Set<Type> getTypes() {
    return permissions;
  }

  public String toFlatString() {
    StringBuilder sb = new StringBuilder();
    for (Type type : permissions) {
      sb.append(type.toString());
      sb.append(",");
    }

    return sb.substring(0, sb.length() - 1);
  }
}

