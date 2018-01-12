import Component from '@ember/component';
import { get, set, observer } from '@ember/object';
import { run, next } from '@ember/runloop';
import { task } from 'ember-concurrency';
import {
  baseCommentEditorOptions,
  DatasetPlatform,
  exemptPolicy,
  getSupportedPurgePolicies,
  isExempt,
  missingPolicyText,
  PurgePolicy,
  purgePolicyProps
} from 'wherehows-web/constants';
import { IComplianceInfo } from 'wherehows-web/typings/api/datasets/compliance';
import { IDataPlatform } from 'wherehows-web/typings/api/list/platforms';
import { readPlatforms } from 'wherehows-web/utils/api/list/platforms';

export default class PurgePolicyComponent extends Component {
  /**
   * Reference to the purge exempt policy
   * @type {PurgePolicy}
   */
  exemptPolicy = exemptPolicy;

  /**
   * Reference to the informational text if the dataset does not have a saved purge policy
   * @type {string}
   */
  missingPolicyText = missingPolicyText;

  /**
   * Reference to client options for each purge policy
   * @type {PurgePolicyProperties}
   */
  purgePolicyProps = purgePolicyProps;

  /**
   * The list of supported purge policies for the related platform
   * @type {Array<PurgePolicy>}
   * @memberof PurgePolicyComponent
   */
  supportedPurgePolicies: Array<PurgePolicy> = [];

  /**
   * The dataset's  platform
   * @type {DatasetPlatform}
   * @memberof PurgePolicyComponent
   */
  platform: DatasetPlatform;

  /**
   * The currently save policy for the dataset purge
   * @type {PurgePolicy}
   * @memberof PurgePolicyComponent
   */
  purgePolicy: PurgePolicy;

  /**
   * Flag indication that policy has a request exemption reason
   * @type {boolean}
   */
  requestExemptionReason = false;

  /**
   * An options hash for the purge exempt reason text editor
   * @type {}
   * @memberof PurgePolicyComponent
   */
  editorOptions = {
    ...baseCommentEditorOptions,
    placeholder: {
      text: 'Please provide an explanation for why this dataset is marked "Purge Exempt" status',
      hideOnClick: false
    }
  };

  /**
   * Action to handle policy change, by default a no-op function
   * @type {(purgePolicy: PurgePolicy) => IComplianceInfo['complianceType'] | null}
   * @memberof PurgePolicyComponent
   */
  onPolicyChange: (purgePolicy: PurgePolicy) => IComplianceInfo['complianceType'] | null;

  didReceiveAttrs() {
    this._super(...arguments);
    this.checkExemption(get(this, 'purgePolicy'));
  }

  didInsertElement() {
    get(this, 'getPlatformPolicies').perform();
  }

  /**
   * Observes changes to the platform property and invokes the task to update the supportedPurgePolicies prop
   * @type {void}
   * @memberof PurgePolicyComponent
   */
  platformChanged = observer('platform', function(this: PurgePolicyComponent) {
    get(this, 'getPlatformPolicies').perform();
  });

  /**
   * Task to retrieve platform policies for and set supported policies for the current platform
   * @memberof PurgePolicyComponent
   */
  getPlatformPolicies = task(function*(this: PurgePolicyComponent): IterableIterator<Promise<Array<IDataPlatform>>> {
    const platform = get(this, 'platform');

    if (platform) {
      set(this, 'supportedPurgePolicies', getSupportedPurgePolicies(platform, yield readPlatforms()));
    }
  }).restartable();

  /**
   * Checks that the selected purge policy is exempt, if so, set the
   * flag to request the exemption to true
   * @param {PurgePolicy} purgePolicy
   */
  checkExemption(purgePolicy: PurgePolicy) {
    const exemptionReasonRequested = isExempt(purgePolicy);
    set(this, 'requestExemptionReason', exemptionReasonRequested);

    if (exemptionReasonRequested) {
      // schedule for a future queue, 'likely' post render
      // this allows us to ensure that editor it visible after the set above has been performed
      run(() => next(this, 'focusEditor'));
    }
  }

  /**
   * Applies cursor / document focus to the purge note text editor
   */
  focusEditor() {
    const exemptionReasonElement = <HTMLElement>get(this, 'element').querySelector('.comment-new__content');

    if (exemptionReasonElement) {
      exemptionReasonElement.focus();
    }
  }

  actions = {
    /**
     * Handles the change to the currently selected purge policy
     * @param {string} _name unused name for the radio group
     * @param {PurgePolicy} purgePolicy the selected purge policy
     */
    onChange(this: PurgePolicyComponent, _name: string, purgePolicy: PurgePolicy) {
      return get(this, 'onPolicyChange')(purgePolicy);
    }
  };
}
