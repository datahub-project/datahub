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
package wherehows.dao.table;

import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class AclDao {

  public AclDao() {
  }

  public List<?> getDatasetAcls(@Nonnull String datasetUrn) throws Exception {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public void addUserToDatasetAcl(@Nonnull String datasetUrn, @Nonnull String user, @Nonnull String justification)
      throws Exception {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public void addUserToDatasetAcl(@Nonnull String datasetUrn, @Nonnull String user, @Nullable String accessType,
      @Nonnull String justification, @Nullable Long expiresAt) throws Exception {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public void removeUserFromDatasetAcl(@Nonnull String datasetUrn, @Nonnull String user) throws Exception {
    throw new UnsupportedOperationException("Not implemented yet");
  }
}
