package com.linkedin.metadata.timeseries.postgres;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import javax.annotation.Nonnull;

/** Binds positional parameters; supports PostgreSQL {@code text[]} via {@link String[]}. */
public final class PostgresPreparedBinder {

  private PostgresPreparedBinder() {}

  public static void bind(@Nonnull PreparedStatement ps, @Nonnull List<?> params)
      throws SQLException {
    for (int i = 0; i < params.size(); i++) {
      Object o = params.get(i);
      if (o instanceof String[]) {
        ps.setArray(i + 1, ps.getConnection().createArrayOf("text", (String[]) o));
      } else {
        ps.setObject(i + 1, o);
      }
    }
  }
}
