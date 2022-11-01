package com.linkedin.datahub.graphql.types.timeline.mappers;

import com.linkedin.datahub.graphql.generated.ChangeEvent;
import com.linkedin.datahub.graphql.generated.GetDatasetChangeEventsResult;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class DatasetChangeEventsMapper {

  public static GetDatasetChangeEventsResult map(List<ChangeTransaction> changeTransactions) {
    if (changeTransactions.isEmpty()) {
      log.debug("Change transactions are empty");
      return null;
    }

    GetDatasetChangeEventsResult result = new GetDatasetChangeEventsResult();
    List<ChangeEvent> listChangeEvents = new ArrayList<>();
    for (ChangeTransaction changeTransaction : changeTransactions) {
      // add all the events from the same transaction
      for (com.linkedin.metadata.timeline.data.ChangeEvent changeEvent : changeTransaction.getChangeEvents()) {
        ChangeEvent changeEventQL = new ChangeEvent();
        changeEventQL.setActor(changeTransaction.getActor());
        changeEventQL.setTimestampMillis(changeTransaction.getTimestamp());
        changeEventQL.setOperation(changeEvent.getOperation().name());
        changeEventQL.setCategory(changeEvent.getCategory().name());
        changeEventQL.setDescription(changeEvent.getDescription());
        listChangeEvents.add(changeEventQL);
      }
    }

    // set the list and return the result
    result.setChangedEventsList(listChangeEvents);
    return result;
  }

  private DatasetChangeEventsMapper() {
  }
}