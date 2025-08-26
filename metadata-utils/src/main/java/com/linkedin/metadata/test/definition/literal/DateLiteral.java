package com.linkedin.metadata.test.definition.literal;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.linkedin.metadata.test.definition.value.DateType;
import com.linkedin.metadata.test.definition.value.ValueType;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
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

  private static Instant resolveRelativeDate(String relativeDate) {
    Pattern pattern = Pattern.compile("^([+-])(\\d+)(d|m|h|w|y|min)$");
    Matcher matcher = pattern.matcher(relativeDate);

    if (matcher.find()) {
      String sign = matcher.group(1);
      long amount = Long.parseLong(matcher.group(2));
      String unit = matcher.group(3);

      Instant now = Instant.now();
      boolean isAddition = sign.equals("+");

      switch (unit) {
        case "min":
          return now.plus(Duration.ofMinutes(isAddition ? amount : -amount));
        case "h":
          return now.plus(Duration.ofHours(isAddition ? amount : -amount));
        case "d":
          return now.plus(Duration.ofDays(isAddition ? amount : -amount));
        case "w":
          return now.plus(Duration.ofDays((isAddition ? amount : -amount) * 7));
        case "m":
        case "y":
          // For months and years, we need to use ZonedDateTime because these are
          // calendar-based periods that don't have a fixed duration
          ZonedDateTime zdt = ZonedDateTime.now(ZoneOffset.systemDefault());
          if (unit.equals("m")) {
            zdt = isAddition ? zdt.plusMonths(amount) : zdt.minusMonths(amount);
          } else {
            zdt = isAddition ? zdt.plusYears(amount) : zdt.minusYears(amount);
          }
          return zdt.toInstant();
        default:
          throw new IllegalArgumentException("Unsupported time unit: " + unit);
      }
    } else {
      throw new IllegalArgumentException("Invalid relative date format: " + relativeDate);
    }
  }

  public String resolveValue() {
    Instant instant = resolveRelativeDate(this.value);
    return String.valueOf(instant.toEpochMilli());
  }

  @JsonPOJOBuilder(withPrefix = "")
  public static class DateLiteralBuilder {}
}
