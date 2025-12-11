/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.examples;

import com.linkedin.tag.TagProperties;
import datahub.client.MetadataWriteResponse;
import datahub.client.rest.RestEmitter;
import datahub.event.MetadataChangeProposalWrapper;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class TagCreate {

  private TagCreate() {}

  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException {
    TagProperties tagProperties =
        new TagProperties()
            .setName("Deprecated")
            .setDescription("Having this tag means this column or table is deprecated.");

    MetadataChangeProposalWrapper mcpw =
        MetadataChangeProposalWrapper.builder()
            .entityType("tag")
            .entityUrn("urn:li:tag:deprecated")
            .upsert()
            .aspect(tagProperties)
            .build();

    String token = "";
    RestEmitter emitter = RestEmitter.create(b -> b.server("http://localhost:8080").token(token));
    Future<MetadataWriteResponse> response = emitter.emit(mcpw, null);
    System.out.println(response.get().getResponseContent());
  }
}
