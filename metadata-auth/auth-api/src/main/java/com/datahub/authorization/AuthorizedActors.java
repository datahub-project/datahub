package com.datahub.authorization;

import com.linkedin.common.urn.Urn;
import java.util.List;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;


@Value
@AllArgsConstructor(access = AccessLevel.PUBLIC)
@Builder
public class AuthorizedActors {
  String privilege;
  List<Urn> users;
  List<Urn> groups;
  boolean allUsers;
  boolean allGroups;
}
