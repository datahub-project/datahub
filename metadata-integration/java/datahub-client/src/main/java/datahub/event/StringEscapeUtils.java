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

package datahub.event;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Locale;

public class StringEscapeUtils {

  private StringEscapeUtils() {

  }
   
  /**
   * Worker method for the {@link #escapeJavaScript(String)} method.
   * 
   * @param out write to receieve the escaped string
   * @param str String to escape values in, may be null
   * @param escapeSingleQuote escapes single quotes if <code>true</code>
   * @param escapeForwardSlash TODO
   * @throws IOException if an IOException occurs
   */
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

  /**
   * Returns an upper case hexadecimal <code>String</code> for the given
   * character.
   *
   * @param ch The character to convert.
   * @return An upper case hexadecimal <code>String</code>
   */
  private static String hex(char ch) {
    return Integer.toHexString(ch).toUpperCase(Locale.ENGLISH);
  }

  /**
   * Worker method for the {@link #escapeJavaScript(String)} method.
   *
   * @param str String to escape values in, may be null
   * @param escapeSingleQuotes escapes single quotes if <code>true</code>
   * @param escapeForwardSlash TODO
   * @return the escaped string
   */
  private static String escapeJavaStyleString(String str, boolean escapeSingleQuotes, boolean escapeForwardSlash) throws IOException {
    if (str == null) {
      return null;
    } else {
      StringWriter writer = new StringWriter(str.length() * 2);
      escapeJavaStyleString(writer, str, escapeSingleQuotes, escapeForwardSlash);
      return writer.toString();
       
    }
  }
  
  /**
   * Escapes the characters in a <code>String</code> using Java String rules.
   * <p>
   * Deals correctly with quotes and control-chars (tab, backslash, cr, ff, etc.)
   * <p>
   * So a tab becomes the characters <code>'\\'</code> and <code>'t'</code>.
   * <p>
   * The only difference between Java strings and JavaScript strings
   * is that in JavaScript, a single quote must be escaped.
   * <p>
   * Example:
   * <pre>
   * input string: He didn't say, "Stop!"
   * output string: He didn't say, \"Stop!\"
   * </pre>
   *
   * @param str  String to escape values in, may be null
   * @return String with escaped values, <code>null</code> if null string input
   */
  public static String escapeJava(String str) throws IOException {
    return escapeJavaStyleString(str, false, false);
  }
}
