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
package wherehows.models.table;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;
import lombok.Data;


@Data
@Entity
@Table(name = "users")
public class User {

  @Id
  @Column(name = "id")
  private int id;

  @Column(name = "username")
  private String userName; // ldap user id

  @Column(name = "department_number")
  private int departmentNum;

  @Column(name = "email")
  private String email;

  @Column(name = "name")
  private String name; // ldap display name

  @Transient
  private UserSetting userSetting;
}
