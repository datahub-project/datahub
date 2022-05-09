/**
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package datahub.client.rest;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

import org.apache.commons.lang.UnhandledException;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.ByteString;
import com.linkedin.data.template.JacksonDataTemplateCodec;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;

import datahub.event.MetadataChangeProposalWrapper;

public class StringEscapeUtils {

  private StringEscapeUtils() {

  }

  private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper()
      .setSerializationInclusion(JsonInclude.Include.NON_NULL);
  private final static JacksonDataTemplateCodec DATA_TEMPLATE_CODEC = new JacksonDataTemplateCodec(
      OBJECT_MAPPER.getFactory());

  public static MetadataChangeProposal convert(MetadataChangeProposalWrapper mcpw)
      throws IOException, URISyntaxException {
    String serializedAspect = escapeJava(DATA_TEMPLATE_CODEC.dataTemplateToString(mcpw.getAspect()));
    MetadataChangeProposal mcp = new MetadataChangeProposal().setEntityType(mcpw.getEntityType())
        .setAspectName(mcpw.getAspectName()).setEntityUrn(Urn.createFromString(mcpw.getEntityUrn()))
        .setChangeType(mcpw.getChangeType());

    mcp.setAspect(new GenericAspect().setContentType("application/json")
        .setValue(ByteString.unsafeWrap(serializedAspect.getBytes(StandardCharsets.UTF_8))));
    return mcp;
  }

  private static void escapeJavaStyleString(Writer out, String str, boolean escapeSingleQuote,
      boolean escapeForwardSlash) throws IOException {
    if (out == null) {
      throw new IllegalArgumentException("The Writer must not be null");
    } else if (str != null) {
      int sz = str.length();

      for (int i = 0; i < sz; ++i) {
        char ch = str.charAt(i);
        if (ch > 4095) {
          out.write("\\u" + hex(ch));
        } else if (ch > 255) {
          out.write("\\u0" + hex(ch));
        } else if (ch > 127) {
          out.write("\\u00" + hex(ch));
        } else if (ch < ' ') {
          switch (ch) {
          case '\b':
            out.write(92);
            out.write(98);
            break;
          case '\t':
            out.write(92);
            out.write(116);
            break;
          case '\n':
            out.write(92);
            out.write(110);
            break;
          case '\u000b':

          case '\f':
            out.write(92);
            out.write(102);
            break;
          case '\r':
            out.write(92);
            out.write(114);
            break;
          default:
            if (ch > 15) {
              out.write("\\u00" + hex(ch));
            } else {
              out.write("\\u000" + hex(ch));
            }
            break;
          }

        } else {
          out.write(ch);
        }
      }
    }
  }

  private static String hex(char ch) {
    return Integer.toHexString(ch).toUpperCase(Locale.ENGLISH);
  }

  private static String escapeJavaStyleString(String str, boolean escapeSingleQuotes, boolean escapeForwardSlash) {
    if (str == null) {
      return null;
    } else {
      try {
        StringWriter writer = new StringWriter(str.length() * 2);
        escapeJavaStyleString(writer, str, escapeSingleQuotes, escapeForwardSlash);
        return writer.toString();
      } catch (IOException var4) {
        throw new UnhandledException(var4);
      }
    }
  }

  public static String escapeJava(String str) {
    return escapeJavaStyleString(str, false, false);
  }
}
