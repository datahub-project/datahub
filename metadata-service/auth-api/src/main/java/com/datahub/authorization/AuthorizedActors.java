package com.datahub.authorization;

import com.linkedin.common.urn.Urn;
import java.util.List;
import lombok.Builder;
import lombok.Value;


@Value
@Builder
public class AuthorizedActors {
  String privilege;
  List<Urn> users;
  List<Urn> groups;
  boolean allUsers;
  boolean allGroups;
}
