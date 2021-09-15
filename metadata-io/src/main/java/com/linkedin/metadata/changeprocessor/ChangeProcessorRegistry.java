package com.linkedin.metadata.changeprocessor;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;

// Responsible for looking for custom processor jars specified in config
public class ChangeProcessorRegistry {

  public Map<String, SortedSet<ChangeProcessor>> registeredChangeProcessors;

  public ChangeProcessorRegistry(){

  }

  public Map<String, SortedSet<ChangeProcessor>> getAll(){
    return new HashMap<>();
  }
}
