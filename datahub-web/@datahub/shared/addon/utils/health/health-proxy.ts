import { computed } from '@ember/object';
import { map } from '@ember/object/computed';

/**
 * Provides an interface over the Com.Linkedin.Common.Health metadata instance,
 * and additional functionality over related values
 * @export
 * @class HealthProxy
 */
export default class HealthProxy {
  /**
   * Label text providing a quick description for the Health Score feature
   * @static
   */
  static descriptiveLabel =
    'Health Score is a numeric value computed by applying different quality and usability rules on this data entity';

  /**
   * Label text providing a description or help for the health score recalculation
   * @static
   */
  static descriptiveScoreRecalculationLabel =
    'The Health Score is recalculated automatically every 24 hours. You can also, if needed, manually recalculate the score for this data entity';

  /**
   * The Regular Expression object for pattern matching validators
   * @readonly
   * @static
   */
  static get validatorRegExp(): RegExp {
    return new RegExp(/validator/, 'gi');
  }

  /**
   * Method to convert a number into a percentage value
   * @private
   * @param {Com.Linkedin.Common.HealthValidation['score']} score
   */
  private scoreAsPercent(score: Com.Linkedin.Common.HealthValidation['score']): number {
    return parseInt((score * 100).toFixed(), 10);
  }

  /**
   * Parses the validator as a string that is more user friendly / readable
   * @private
   * @param {Com.Linkedin.Common.HealthValidation['validator']} validator
   */
  private humanReadableValidator(validator: Com.Linkedin.Common.HealthValidation['validator']): string {
    // The validator is expected to be a dot delimited string, the last string in the sequence is expected to be
    // the human readable name for the validator, this may be suffixed with validatorRegExp, so we replace that
    const lastStringInDotDelimitedSequence = validator.split('.').pop() || '';
    return lastStringInDotDelimitedSequence.replace(HealthProxy.validatorRegExp, () => '');
  }

  /**
   * Attempts to map the Health score number into a percentage value or null otherwise
   * @readonly
   */
  @computed('health.score')
  get score(): string | null {
    const score = this.health?.score;
    // score can be 0
    return typeof score === 'number' ? String(this.scoreAsPercent(score)) : null;
  }

  /**
   * References the timestamp when the score was last updated
   * @readonly
   */
  @computed('health.created')
  get lastUpdated(): number | undefined {
    return this.health?.created?.time;
  }

  /**
   * Maps the validator with a score percent and a validator value
   */
  @map('health.validations', function(
    this: HealthProxy,
    validation: Com.Linkedin.Common.HealthValidation
  ): Com.Linkedin.Common.HealthValidation {
    return {
      ...validation,
      score: this.scoreAsPercent(validation.score),
      validator: this.humanReadableValidator(validation.validator)
    };
  })
  validations!: Com.Linkedin.Common.Health['validations'];

  /**
   * Creates an instance of HealthProxy.
   * @param {(Com.Linkedin.Common.Health | null)} [health] the instance of the Health metadata
   */
  constructor(readonly health?: Com.Linkedin.Common.Health | null) {}
}
