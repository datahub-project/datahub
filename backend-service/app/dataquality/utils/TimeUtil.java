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

import dataquality.dq.TimeOption;
import dataquality.models.TimeRange;
import dataquality.models.enums.Frequency;
import dataquality.models.enums.TimeGrain;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import play.Logger;


/**
 * Created by zechen on 7/22/15.
 */
public class TimeUtil {

  public static Timestamp trunc(Timestamp timestamp, TimeGrain timeGrain) {
    return trunc(timestamp, timeGrain, 1);
  }

  public static Timestamp trunc(Timestamp timestamp, TimeGrain timeGrain, int shift) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(timestamp);
    switch (timeGrain) {
      case HOUR:
        cal.add(Calendar.HOUR_OF_DAY, -shift);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        break;
      case DAY:
        cal.add(Calendar.DAY_OF_MONTH, -shift);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        break;
      case WEEK:
        cal.add(Calendar.WEEK_OF_MONTH, -shift);
        cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        break;
      case BI_WEEK:
        cal.add(Calendar.WEEK_OF_MONTH, -2 * shift);
        cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        break;
      case MONTH:
        cal.add(Calendar.MONTH, -shift);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        break;
      case QUARTER:
        shift = shift * 3 + cal.get(Calendar.MONTH) % 3;
        cal.add(Calendar.MONTH, -shift);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        break;
      case YEAR:
        cal.add(Calendar.YEAR, -shift);
        cal.set(Calendar.MONTH, Calendar.JANUARY);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        break;
    }

    return new Timestamp(cal.getTimeInMillis());
  }

  public static Timestamp adjust(Timestamp ts, TimeOption option, int shift) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(ts);
    switch (option) {
      case RUN:
        break;
      case HOUR:
        cal.add(Calendar.HOUR_OF_DAY, -shift);
        break;
      case DAY:
        cal.add(Calendar.DAY_OF_MONTH, -shift);
        break;
      case WEEK:
        cal.add(Calendar.WEEK_OF_MONTH, -shift);
        break;
      case MONTH:
        cal.add(Calendar.MONTH, -shift);
        break;
      case QUARTER:
        cal.add(Calendar.MONTH, -shift * 3);
        break;
      case YEAR:
        cal.add(Calendar.YEAR, -shift);
        break;
    }

    return new Timestamp(cal.getTimeInMillis());
  }

  public static Timestamp adjust(Timestamp ts, TimeOption option) {
    return adjust(ts, option, 1);
  }

  public static TimeRange adjust(TimeRange tr, TimeOption option, int shift) {
    Timestamp newStart = adjust(tr.getStart(), option, shift);
    Timestamp newEnd = adjust(tr.getEnd(), option, shift);
    return new TimeRange(newStart, newEnd);
  }

  public static TimeRange adjust(TimeRange tr, TimeOption option) {
    return adjust(tr, option, 1);
  }

  public static SimpleDateFormat getSimpleDateFormat(TimeGrain grain) {
    switch (grain) {
      case HOUR:
        return new SimpleDateFormat("yyyy-MM-dd HH");
      case DAY:
        return new SimpleDateFormat("yyyy-MM-dd");
      case WEEK:
        return new SimpleDateFormat("'Week of 'yyyy-MM-dd");
      case BI_WEEK:
        return new SimpleDateFormat("'BiWeek of 'yyyy-MM-dd");
      case MONTH:
        return new SimpleDateFormat("yyyy-MM");
      case QUARTER:
        return new SimpleDateFormat("'Quarter of 'yyyy-MM");
      case YEAR:
        return new SimpleDateFormat("yyyy");
    }

    return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  }


  public static Timestamp getNextRun(Timestamp start, Frequency frequency) {
    Calendar ret = Calendar.getInstance();
    ret.setTimeInMillis(start.getTime());
    switch (frequency) {

      case DATA_TRIGGERED:
        Logger.info("data triggered, do nothing");
        break;
      case HOURLY:
        Logger.info("hourly, add an hour");
        ret.add(Calendar.HOUR_OF_DAY, 1);
        break;
      case DAILY:
        Logger.info("daily, add a day");
        ret.add(Calendar.DATE, 1);
        break;
      case WEEKLY:
        Logger.info("weekly, add a week");
        ret.add(Calendar.DATE, 7);
        break;
      case BI_WEEKLY:
        Logger.info("bi-weekly, add two week");
        ret.add(Calendar.DATE, 14);
        break;
      case MONTHLY:
        Logger.info("monthly, add a month");
        ret.add(Calendar.MONTH, 1);
        break;
      case QUARTERLY:
        Logger.info("quarterly, add three months");
        ret.add(Calendar.MONTH, 3);
        break;
      case YEARLY:
        Logger.info("yearly, add a year");
        ret.add(Calendar.YEAR, 1);
        break;
    }

    return new Timestamp(ret.getTimeInMillis());
  }

}
