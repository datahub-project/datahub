package com.linkedin.datahub.models.view;

import lombok.Value;


/**
 * The DataOriginView class is used to find our dataset data origin. Previously, the values EI, CORP, and PROD
 * were hardcoded into the frontend UI, but users actually wanted even more information about the cluster the
 * data lives on (e.g. Holdem, War).
 */
@Value
public class DataOriginView {
  /**
   * This will be the item that actually shows up on the UI in the list of possible data origins. It would be the
   * more specific designation (e.g. Holdem)
   */
  private String displayTitle;

  /**
   * Since our urn for datasets isn't to the same degree of specificity as displayTitle, this origin maps to the
   * concepts of EI, CORP, PROD so that the UI can understand how to create the link for the dataset fabric
   */
  private String origin;
}
