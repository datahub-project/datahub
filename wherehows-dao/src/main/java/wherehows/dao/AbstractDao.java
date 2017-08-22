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
package wherehows.dao;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;


public class AbstractDao {

  final EntityManagerFactory entityManagerFactory;

  public AbstractDao(EntityManagerFactory factory) {
    this.entityManagerFactory = factory;
  }

  public void update(Object record) {
    EntityManager entityManager = entityManagerFactory.createEntityManager();
    entityManager.getTransaction().begin();
    entityManager.merge(record);
    entityManager.getTransaction().commit();
    entityManager.close();
  }


}
