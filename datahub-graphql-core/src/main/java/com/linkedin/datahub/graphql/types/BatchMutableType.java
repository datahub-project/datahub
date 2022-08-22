package com.linkedin.datahub.graphql.types;

import com.linkedin.datahub.graphql.QueryContext;

import javax.annotation.Nonnull;
import java.util.List;

public interface BatchMutableType<I, B, T> extends MutableType<I, T> {
    default Class<B[]> batchInputClass() throws UnsupportedOperationException {
        throw new UnsupportedOperationException(this.getClass().getName() + " does not implement batchInputClass method");
    }

    default List<T> batchUpdate(@Nonnull final B[] updateInput, QueryContext context) throws Exception {
        throw new UnsupportedOperationException(this.getClass().getName() + " does not implement batchUpdate method");
    }
}
