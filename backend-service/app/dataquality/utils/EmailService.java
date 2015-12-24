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
package dataquality.utils;

import java.util.Map;
import java.util.Properties;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import models.daos.EtlJobPropertyDao;


/**
 * Created by zechen on 7/9/15.
 */
public class EmailService {

  private static final String MAIL_HOST = "mail.host";

  public static void sendMail(String to, String subject, String content) throws Exception {
    String[] recipients = to.split(",");
    Map<String, String> smtpProps = EtlJobPropertyDao.getWherehowsPropertiesByGroup("smtp");
    sendMail(recipients, subject, content, smtpProps);
  }

  public static void sendMail(String[] to, String subject, String content, Map<String, String> smtpProps) throws MessagingException {
    InternetAddress[] toAddresses = new InternetAddress[to.length];
    for (int i = 0; i < to.length; i++) {
      if (to[i].contains("@")) {
        toAddresses[i] = new InternetAddress(to[i]);
      } else {
        toAddresses[i] = new InternetAddress(to[i] + "@" + smtpProps.get("smtp.server.default.domain"));
      }
    }
    sendMail(toAddresses, subject, content, smtpProps);
  }

  public static void sendMail(InternetAddress[] to, String subject, String content, Map<String, String> smtpProps) throws MessagingException {

    Properties props = new Properties();
    props.put(MAIL_HOST, smtpProps.get("smtp.server.host"));
    //TODO: add authentication properties
    Session session = Session.getDefaultInstance(props);
    Message message = new MimeMessage(session);
    InternetAddress fromAddress = new InternetAddress(smtpProps.get("smtp.server.from.email"));

    message.setFrom(fromAddress);
    message.setRecipients(Message.RecipientType.TO, to);
    message.setSubject(subject);
    message.setContent(content, "text/html; charset=utf-8");
    Transport.send(message);
  }
}
