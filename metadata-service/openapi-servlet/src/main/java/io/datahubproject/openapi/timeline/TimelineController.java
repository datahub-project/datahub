package io.datahubproject.openapi.timeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.timeline.TimelineService;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


@RestController
@AllArgsConstructor
@RequestMapping("/timeline/v1")
public class TimelineController {

  private final TimelineService _timelineService;

  /**
   *
   * @param rawUrn
   * @param start
   * @param end
   * @param startVersionStamp
   * @param endVersionStamp
   * @param raw
   * @param categories
   * @return
   */
  @GetMapping(path = "/{urn}", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<List<ChangeTransaction>> getTimeline(
      @PathVariable("urn") String rawUrn,
      @RequestParam(defaultValue = "-1") long startTime,
      @RequestParam(defaultValue = "0") long endTime,
      @RequestParam(defaultValue = "false") boolean raw,
      @RequestParam Set<ChangeCategory> categories) throws URISyntaxException, JsonProcessingException {
    // Make request params when implemented
    String startVersionStamp = null;
    String endVersionStamp = null;
    Urn urn = Urn.createFromString(rawUrn);
    return ResponseEntity.ok(_timelineService.getTimeline(urn, categories, startTime, endTime, startVersionStamp, endVersionStamp, raw));
  }
}
