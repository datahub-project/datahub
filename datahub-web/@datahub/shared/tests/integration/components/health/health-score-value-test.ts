import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, settled } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | health/health-score-value', function(hooks): void {
  setupRenderingTest(hooks);

  test('component and grade hint rendering', async function(assert): Promise<void> {
    assert.expect(17);
    type GradeTuple = [number | string | undefined, string];
    const className = '.health-score';
    const gradeHint = {
      negative: 'rgb(255, 44, 51)',
      neutral: 'rgb(229, 88, 0)',
      positive: 'rgb(70, 154, 31)',
      na: 'rgb(132, 135, 138)'
    };

    this.set('score', undefined);
    await render(hbs`<Health::HealthScoreValue @score={{this.score}}/>`);
    assert.dom().hasText('N/A');

    const gradeExpectations: Array<GradeTuple> = [
      [12, gradeHint.negative],
      [63, gradeHint.neutral],
      [0, gradeHint.negative],
      [100, gradeHint.positive],
      ['13', gradeHint.negative],
      ['64', gradeHint.neutral],
      ['100', gradeHint.positive],
      ['N/A', gradeHint.na]
    ];

    const asyncAssertions = async function*(
      expectations: Array<GradeTuple>,
      asyncTrigger: (expect: GradeTuple) => Promise<void>
    ): AsyncGenerator<GradeTuple, void, unknown> {
      let index = 0;

      while (index < expectations.length) {
        const expectation = expectations[index++];

        await asyncTrigger(expectation);
        yield expectation;
      }
    };

    const updateScore = ([score]: GradeTuple): Promise<void> => {
      this.set('score', score);
      return settled();
    };

    for await (const [score, gradeHint] of asyncAssertions(gradeExpectations, updateScore)) {
      assert.dom(className).hasText(new RegExp(`${score}%|N\/A`));
      assert.dom(className).hasStyle({ color: gradeHint });
    }
  });
});
