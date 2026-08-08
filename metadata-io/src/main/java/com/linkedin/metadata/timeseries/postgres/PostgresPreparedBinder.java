package com.linkedin.metadata.timeseries.postgres;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * @deprecated Prefer {@link com.linkedin.metadata.postgres.jdbc.PostgresPreparedBinder}.
 */
@Deprecated
public final class PostgresPreparedBinder {

  private PostgresPreparedBinder() {}

  public static void bind(@Nonnull PreparedStatement ps, @Nonnull List<?> params)
      throws SQLException {
    com.linkedin.metadata.postgres.jdbc.PostgresPreparedBinder.bind(ps, params);
  }
}
