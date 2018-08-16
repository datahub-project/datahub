import Controller from '@ember/controller';
import { computed, set, get, setProperties, getWithDefault } from '@ember/object';
import ComputedProperty from '@ember/object/computed';
import { or } from '@ember/object/computed';
import { encodeUrn } from 'wherehows-web/utils/validators/urn';
import { Tabs } from 'wherehows-web/constants/datasets/shared';
import { action } from '@ember-decorators/object';
import { DatasetPlatform } from 'wherehows-web/constants';
import { IDatasetView } from 'wherehows-web/typings/api/datasets/dataset';
import { alias } from '@ember-decorators/object/computed';
import DatasetMeta from 'wherehows-web/services/dataset-meta';
import { service } from '@ember-decorators/service';

export default class DatasetController extends Controller {
  queryParams = ['urn'];
  /**
   * References the controller model
   * @type {IDatasetView}
   * @memberof DatasetController
   */
  model: IDatasetView | void;

  /**
   * URN for the current dataset view
   * @type {(string | void)}
   * @memberof DatasetController
   */
  urn: string | void;

  /**
   * Enum of tab properties
   * @type {Tabs}
   * @memberof DatasetController
   */
  tabIds = Tabs;

  /**
   * The currently selected tab in view
   * @type {Tabs}
   * @memberof DatasetController
   */
  tabSelected: Tabs;

  /**
   * Flag indicating if the compliance info does not have user entered information
   * @type {boolean}
   * @memberof DatasetController
   */
  isNewComplianceInfo: boolean;

  /**
   * Flag indicating is there are suggestions that have not been accepted or ignored
   * @type {boolean}
   * @memberof DatasetController
   */
  hasSuggestions: boolean;

  /**
   * Flag indicating there are fields in the compliance policy that have not been updated by a user
   * @type {boolean}
   * @memberof DatasetController
   */
  compliancePolicyHasDrift: boolean;

  /**
   * Contains a list of whitelisted dataset platforms for JIT ACL access
   * @type {Array<DatasetPlatform>}
   * @memberof DatasetController
   */
  jitAclAccessWhitelist: Array<DatasetPlatform>;

  /**
   * Flag indicating the dataset policy is derived from an upstream source
   * @type {boolean}
   * @memberof DatasetController
   */
  isPolicyFromUpstream = false;

  /**
   * Flag indicating if the viewer is internal
   * @type {boolean}
   * @memberof DatasetController
   */
  isInternal: boolean;

  /**
   * Flags the lineage feature for datasets
   * @type {boolean}
   * @memberof DatasetController
   */
  shouldShowDatasetLineage: boolean;

  /**
   * Flags the health feature for datasets, which is currently in the development stage so we should not
   * have it appear in production
   * @type {boolean}
   * @memberof DatasetController
   */
  shouldShowDatasetHealth: boolean;

  /**
   * Flag indicating if the dataset contains personally identifiable information
   * @type {boolean}
   * @memberof DatasetController
   */
  datasetContainsPersonalData: boolean;

  /**
   * Including the datasetmeta property that is connected to each child container for the routable
   * tabs. Can be used to share information between these tabs from a higher level
   * @type {Ember.Service}
   */
  @service
  datasetMeta: ComputedProperty<DatasetMeta>;

  /**
   * Easy access in the template to the datasetMeta health score provided by the /health endpoint
   * called in the dataset-health container
   */
  @alias('datasetMeta.healthScore')
  datasetHealthScore: ComputedProperty<number>;

  /**
   * Flag indicating that the compliance policy needs user attention
   * @type {ComputedProperty<boolean>}
   */
  requiresUserAction: ComputedProperty<boolean> = or(
    'isNewComplianceInfo',
    'hasSuggestions',
    'compliancePolicyHasDrift'
  );

  /**
   * Converts the uri on a model to a usable URN format
   * @type {ComputedProperty<string>}
   */
  encodedUrn: ComputedProperty<string> = computed('model', function(this: DatasetController): string {
    const { uri } = get(this, 'model') || { uri: '' };
    return encodeUrn(uri);
  });

  shouldShowHealthGauge: ComputedProperty<boolean> = computed('datasetHealthScore', function(
    this: DatasetController
  ): boolean {
    return typeof get(this, 'datasetHealthScore') === 'number';
  });

  /**
   * Checks if the current platform exists in the supported list of JIT ACL whitelisted platforms
   * @type {ComputedProperty<boolean>}
   */
  isJitAclAccessEnabled: ComputedProperty<boolean> = computed('jitAclAccessWhitelist', function(
    this: DatasetController
  ): boolean {
    const jitAclAccessWhitelist = getWithDefault(this, 'jitAclAccessWhitelist', []);
    const { platform } = get(this, 'model') || { platform: '' };

    return !!platform && jitAclAccessWhitelist.includes(<DatasetPlatform>platform);
  });

  /**
   * Creates an instance of DatasetController.
   * @memberof DatasetController
   */
  constructor() {
    super(...arguments);
    this.tabSelected || (this.tabSelected = Tabs.Ownership);
    this.jitAclAccessWhitelist || (this.jitAclAccessWhitelist = []);
  }

  /**
   * Handler to capture changes in dataset PII status
   * @param {boolean} containingPersonalData
   */
  @action
  onNotifyPiiStatus(containingPersonalData: boolean): void {
    set(this, 'datasetContainsPersonalData', containingPersonalData);
  }

  /**
   * Handles user generated tab selection action by transitioning to specified route
   * @param {Tabs} tabSelected the currently selected tab
   * @returns {void}
   * @memberof DatasetController
   */
  @action
  tabSelectionChanged(tabSelected: Tabs): void {
    // if the tab selection is same as current, noop
    return get(this, 'tabSelected') === tabSelected
      ? void 0
      : this.transitionToRoute(`datasets.dataset.${tabSelected}`, get(this, 'encodedUrn'));
  }

  /**
   * Updates the hasSuggestions flag if the policy is not from an upstream dataset, otherwise set to false
   * @param {boolean} hasSuggestions
   * @memberof DatasetController
   */
  @action
  setOnChangeSetChange(hasSuggestions: boolean) {
    const fromUpstream = get(this, 'isPolicyFromUpstream');
    set(this, 'hasSuggestions', !fromUpstream && hasSuggestions);
  }

  /**
   * Updates the isNewComplianceInfo flag if the policy is not from an upstream dataset, otherwise set to false
   * Also sets the isPolicyFromUpstream attribute
   * @param {({
   *     isNewComplianceInfo: boolean;
   *     fromUpstream: boolean;
   *   })} {
   *     isNewComplianceInfo,
   *     fromUpstream
   *   }
   * @memberof DatasetController
   */
  @action
  setOnComplianceTypeChange({
    isNewComplianceInfo,
    fromUpstream
  }: {
    isNewComplianceInfo: boolean;
    fromUpstream: boolean;
  }) {
    setProperties(this, {
      isNewComplianceInfo: !fromUpstream && isNewComplianceInfo,
      isPolicyFromUpstream: fromUpstream
    });

    if (fromUpstream) {
      this.setOnChangeSetChange(false);
      this.setOnChangeSetDrift(false);
    }
  }

  /**
   * Setter to update the drift flag
   * @param {boolean} hasDrift
   * @memberof DatasetController
   */
  @action
  setOnChangeSetDrift(hasDrift: boolean) {
    const fromUpstream = get(this, 'isPolicyFromUpstream');
    set(this, 'compliancePolicyHasDrift', !fromUpstream && hasDrift);
  }
}
