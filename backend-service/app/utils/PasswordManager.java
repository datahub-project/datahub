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
package utils;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import com.google.common.io.LineReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.jasypt.util.text.BasicTextEncryptor;
import play.Logger;


/**
 * A password util class for encryption and decryption
 * Modified from PasswordManager class in Gobblin (github.com/linkedin/gobblin) project
 *
 */
public class PasswordManager {

  private static final Logger.ALogger LOG = Logger.of(PasswordManager.class);

  private static final String ERROR_ENCRY_MSG = "A master password needs to be provided for encrypting passwords, check your master key location!";
  private static final String ERROR_DECRY_MSG = "A master password needs to be provided for decrypting passwords, check your master key location!";

  public static String encryptPassword(String plain, String masterPwdLoc) {
    return encryptPassword(plain, getMasterPassword(masterPwdLoc));
  }


  public static String decryptPassword(String plain, String masterPwdLoc) {
    return decryptPassword(plain, getMasterPassword(masterPwdLoc));
  }
  /**
   * Encrypt a password. A master password must have been provided in the constructor.
   * @param plain A plain password to be encrypted.
   * @return The encrypted password.
   */
  public static String encryptPassword(String plain, Optional<String> masterPassword) {
    Optional<BasicTextEncryptor> encryptor = getEncryptor(masterPassword);
    Preconditions.checkArgument(getEncryptor(masterPassword).isPresent(), ERROR_ENCRY_MSG);

    try {
      return encryptor.get().encrypt(plain);
    } catch (Exception e) {
      throw new RuntimeException("Failed to encrypt password", e);
    }
  }

  /**
   * Decrypt an encrypted password. A master password must have been provided in the constructor.
   * @param encrypted An encrypted password.
   * @return The decrypted password.
   */
  public static String decryptPassword(String encrypted, Optional<String> masterPassword) {
    Optional<BasicTextEncryptor> encryptor = getEncryptor(masterPassword);
    Preconditions
      .checkArgument(encryptor.isPresent(), ERROR_DECRY_MSG);

    try {
      return encryptor.get().decrypt(encrypted);
    } catch (Exception e) {
      throw new RuntimeException("Failed to decrypt password " + encrypted, e);
    }
  }

  private static Optional<BasicTextEncryptor> getEncryptor(Optional<String> masterPassword) {
    Optional<BasicTextEncryptor> encryptor;
    if (masterPassword.isPresent()) {
      encryptor = Optional.of(new BasicTextEncryptor());
      try {
        encryptor.get().setPassword(masterPassword.get());
      } catch (Exception e) {
        LOG.error("Failed to set master password for encryptor", e);
        encryptor = Optional.absent();
      }
    } else {
      encryptor = Optional.absent();
    }

    return encryptor;
  }

  public static Optional<String> getMasterPassword(String masterPwdLoc) {
    Closer closer = Closer.create();
    try {
      File file = new File(masterPwdLoc);
      if (!file.exists() || file.isDirectory()) {
        LOG.warn(masterPwdLoc + " does not exist or is not a file. Cannot decrypt any encrypted password.");
        return Optional.absent();
      }
      InputStream in = new FileInputStream(file);
      return Optional.of(new LineReader(new InputStreamReader(in, Charsets.UTF_8)).readLine());
    } catch (IOException e) {
      throw new RuntimeException("Failed to obtain master password from " + masterPwdLoc, e);
    } finally {
      try {
        closer.close();
      } catch (IOException e) {
        throw new RuntimeException("Failed to close inputstream for " + masterPwdLoc, e);
      }
    }
  }
}
