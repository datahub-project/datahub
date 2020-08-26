import Component from '@glimmer/component';
import { ScoreGrade } from '@datahub/shared/constants/health/score-grade';

interface IHealthHealthScoreValueArgs {
  // The numerical score for the Health metadata
  score?: number | string;
  // Optional ranges mapped to classification matching the type ScoreGrade
  scoreGrade?: typeof ScoreGrade;
}

/**
 * Provides a consistent way to represent the Health Score across use cases, including color coding for each range
 * @export
 * @class HealthHealthScoreValue
 * @extends {Component<IHealthHealthScoreValueArgs>}
 */
export default class HealthHealthScoreValue extends Component<IHealthHealthScoreValueArgs> {
  /**
   * Attempts to parse the score, potentially a string or undefined, as a number
   * @readonly
   */
  get numericalValueOfScore(): number {
    return parseInt(`${this.args.score}`, 10);
  }

  /**
   * Classifies the score based on the ranges available in the scoreGrade
   * @readonly
   */
  get scoreClassification(): string | void {
    const {
      numericalValueOfScore,
      args: { scoreGrade = ScoreGrade }
    } = this;

    if (Number.isInteger(numericalValueOfScore)) {
      const grade =
        numericalValueOfScore === scoreGrade.Positive
          ? scoreGrade.Positive
          : numericalValueOfScore >= scoreGrade.Neutral
          ? scoreGrade.Neutral
          : scoreGrade.Negative;

      return scoreGrade[grade].toLowerCase();
    }
  }
}
