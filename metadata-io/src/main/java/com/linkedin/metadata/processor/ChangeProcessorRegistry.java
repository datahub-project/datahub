package com.linkedin.metadata.processor;

import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.Aspect;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

// Responsible for looking for custom processor jars specified in config
public class ChangeProcessorRegistry {

  public Map<String, SortedSet<ChangeProcessor>> registeredChangeProcessors;

  public ChangeProcessorRegistry(){

  }

  public Map<String, SortedSet<ChangeProcessor>> getAll(){
    return new HashMap<>();
  }
}
