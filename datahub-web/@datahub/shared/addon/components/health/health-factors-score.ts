import Component from '@glimmer/component';

interface IHealthHealthFactorsScoreArgs {
  // Attribute is supplied within the expected context of the Nacho Table Row
  // this value maps to the same name as the label in INachoTableConfigs
  field?: number;
}

/**
 * HealthFactorsScore is used within the context of a HealthFactors Nacho Table as a custom column to show the score
 * for the related validator
 * @export
 * @class HealthHealthFactorsScore
 * @extends {Component<IHealthHealthFactorsScoreArgs>}
 */
export default class HealthHealthFactorsScore extends Component<IHealthHealthFactorsScoreArgs> {}
