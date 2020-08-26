import { computed } from '@ember/object';
import {
  IEntityComplianceSuggestion,
  SuggestionSource
} from '@datahub/metadata-types/constants/entity/dataset/compliance-suggestion';
import DatasetComplianceAnnotation from '@datahub/data-models/entity/dataset/modules/compliance-annotation';
import { readOnly } from '@ember/object/computed';
import { UserSuggestionInteraction } from '@datahub/data-models/constants/entity/dataset/compliance-suggestions';

/**
 * A dataset is expected to have system suggestions provided for its various fields, which in turn need to have
 * some decorated information in order to provide a readable view. Fortunately, suggestions are provided by the
 * data layer and do not update based on user action, so we don't worry too much about working copies in this
 * class.
 */
export default class DatasetComplianceSuggestion {
  /**
   * The original data that initialized this class. Stored here and expected to remain static. Allows us to
   * maintain the original data read from the API. The initialization of this class is not connected to the
   * API for each individual tag, so this property does not need an async getter
   * @type {IEntityComplianceSuggestion}
   */
  data: IEntityComplianceSuggestion;
  /**
   * The actual tag information for the annotation that is being suggested for a particular field
   * @type {DatasetComplianceAnnotation}
   */
  suggestion: DatasetComplianceAnnotation;
  /**
   * The source from which the suggestion was given. This is to handle cases in the future where users may be
   * able to provide suggestions for review, but for now the default should be "system"
   * @type {SuggestionSource | string}
   */
  source: SuggestionSource | string;
  /**
   * If the user has interacted with the suggestion in any way by declaring the suggestion to be accepted or
   * rejected, the changes can be captured here
   * @type {UserSuggestionInteraction}
   */
  interaction = UserSuggestionInteraction.NONE;
  /**
   * Static read of the uid of the suggestion from the provided data
   * @type {string}
   */
  @readOnly('data.uid')
  uid!: string;
  /**
   * Computed confidence to put into a percentage format out of 100 instead of 0->1 as the provided data and rounds up to two decimals if necessary
   * @type {number}
   */
  @computed('data')
  get confidence(): number {
    return Math.round(this.data.confidenceLevel * 10000) / 100;
  }

  /**
   * If the user chooses to discard their changes during their session, this method will capture all
   * the operations necessary to reset the suggestion to a clean slate.
   */
  resetWorkingCopy() {
    this.interaction = UserSuggestionInteraction.NONE;
  }

  constructor(
    suggestionInfo: IEntityComplianceSuggestion,
    source: SuggestionSource | string = SuggestionSource.system
  ) {
    this.data = suggestionInfo;
    this.suggestion = new DatasetComplianceAnnotation(suggestionInfo.suggestion);
    this.source = source;
  }
}
