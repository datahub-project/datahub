package com.linkedin.metadata.dao;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.UnionTemplate;
import javax.annotation.Nonnull;


/**
 * A base class for all Remote DAO.
 *
 * Remote DAO is a standardized interface to fetch aspects stored on a remote service.
 * See http://go/gma for more details.
 *
 * @param <ASPECT_UNION> must be an aspect union type defined in com.linkedin.metadata.aspect
 */
public abstract class BaseRemoteDAO<ASPECT_UNION extends UnionTemplate, URN extends Urn>
    extends BaseReadDAO<ASPECT_UNION, URN> {

  public BaseRemoteDAO(@Nonnull Class<ASPECT_UNION> aspectUnionClass) {
    super(aspectUnionClass);
  }
}
