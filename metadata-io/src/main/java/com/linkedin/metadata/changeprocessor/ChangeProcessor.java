package com.linkedin.metadata.changeprocessor;

import com.linkedin.data.template.RecordTemplate;

public interface ChangeProcessor {
  ProcessChangeResult beforeChange(String aspectName,
      RecordTemplate previousAspect,
      RecordTemplate newAspect
  );

  ProcessChangeResult afterChange(
      String aspectName,
      RecordTemplate previousAspect,
      RecordTemplate newAspect
  );

  Integer PRIORITY = 0;
}


