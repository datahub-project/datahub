package com.linkedin.metadata.test.definition.literal;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.linkedin.metadata.test.definition.value.DateType;
import com.linkedin.metadata.test.definition.value.ValueType;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Builder;

@JsonDeserialize(builder = StringListLiteral.StringListLiteralBuilder.class)
@Builder
public class DateLiteral implements Literal {
  @JsonProperty("value")
  private final String value;

  public DateLiteral(String value) {
    if (!isValidRelativeDate(value)) {
      throw new IllegalArgumentException("Invalid relative date format: " + value);
    }
    this.value = value;
  }

  @Override
  public Object value() {
    return this.value;
  }

  @Override
  public ValueType valueType() {
    return new DateType();
  }

  private static boolean isValidRelativeDate(String relativeDate) {
    // Regular expression to match the valid relativeDate format
    String regex = "^[+-]\\d+(d|m|h|w|y|min)$";
    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(relativeDate);
    return matcher.matches();
  }

  private static ZonedDateTime resolveRelativeDate(String relativeDate) {
    // Regular expression to parse the relative date
    Pattern pattern = Pattern.compile("^([+-])(\\d+)(d|m|h|w|y|min)$");
    Matcher matcher = pattern.matcher(relativeDate);

    if (matcher.find()) {
      String sign = matcher.group(1);
      long amount = Long.parseLong(matcher.group(2));
      String unit = matcher.group(3);

      ZonedDateTime now = ZonedDateTime.now(ZoneId.systemDefault());
      switch (unit) {
        case "min":
          return sign.equals("+") ? now.plusMinutes(amount) : now.minusMinutes(amount);
        case "h":
          return sign.equals("+") ? now.plusHours(amount) : now.minusHours(amount);
        case "d":
          return sign.equals("+") ? now.plusDays(amount) : now.minusDays(amount);
        case "w":
          return sign.equals("+") ? now.plusWeeks(amount) : now.minusWeeks(amount);
        case "m":
          return sign.equals("+") ? now.plusMonths(amount) : now.minusMonths(amount);
        case "y":
          return sign.equals("+") ? now.plusYears(amount) : now.minusYears(amount);
        default:
          throw new IllegalArgumentException("Unsupported time unit: " + unit);
      }
    } else {
      throw new IllegalArgumentException("Invalid relative date format: " + relativeDate);
    }
  }

  public String resolveValue() {
    ZonedDateTime zonedDateTime = resolveRelativeDate(this.value);
    // Convert ZonedDateTime to Instant
    Instant instant = zonedDateTime.toInstant();
    // Return the epoch millisecond value of the instant
    return String.valueOf(instant.toEpochMilli());
  }

  @JsonPOJOBuilder(withPrefix = "")
  public static class DateLiteralBuilder {}
}
