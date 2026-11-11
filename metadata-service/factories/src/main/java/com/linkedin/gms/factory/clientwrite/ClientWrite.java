package com.linkedin.gms.factory.clientwrite;

import lombok.Value;

/** A write request to GMS, with the client that sent it as identified from its headers. */
@Value
public class ClientWrite {
  /** API family the write arrived on: {@code restli} or {@code openapi}. */
  String api;

  /** {@code python}, {@code java}, {@code browser}, or {@code unknown} for custom code. */
  String client;

  /** Client mode sent by the client ({@code INGESTION}, {@code CLI}, {@code SDK}), else none. */
  String clientMode;

  /** Client version reduced to its numeric part, or {@code none} / {@code unparseable}. */
  String clientVersion;

  /** User-Agent made safe to log; identifies the HTTP library behind custom-code writes. */
  String userAgent;
}
