import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { setProperties } from '@ember/object';
import { TestContext } from 'ember-test-helpers';

interface IMockTaskParams {
  isRunning?: boolean;
  isError?: boolean;
}

interface IMockTask {
  isRunning: boolean;
  last: {
    isError: boolean;
  };
}

interface IMyTest extends TestContext {
  task: IMockTask;
}

const createMockTask = ({ isRunning = false, isError = false }: IMockTaskParams = {}): IMockTask => ({
  isRunning,
  last: {
    isError
  }
});

module('Integration | Component | concurrency-task-state-handler', function(hooks): void {
  setupRenderingTest(hooks);

  test('should yield when task is done', async function(this: IMyTest, assert) {
    const task = createMockTask();

    setProperties(this, { task });
    await render(hbs`
      <ConcurrencyTaskStateHandler @task={{this.task}}>
        template block text
      </ConcurrencyTaskStateHandler>
    `);

    assert.equal(this.element.textContent && this.element.textContent.trim(), 'template block text');
  });

  test('should not yield when task is running', async function(this: IMyTest, assert) {
    const task = createMockTask({ isRunning: true });

    setProperties(this, { task });
    await render(hbs`
      <ConcurrencyTaskStateHandler @task={{this.task}}>
        template block text
      </ConcurrencyTaskStateHandler>
    `);

    assert.equal(this.element.textContent && this.element.textContent.trim(), '');
  });

  test('should show error', async function(this: IMyTest, assert) {
    const task = createMockTask({ isError: true });

    setProperties(this, { task });
    await render(hbs`
      <ConcurrencyTaskStateHandler @task={{this.task}}>
        template block text
      </ConcurrencyTaskStateHandler>
    `);

    const subHead = this.element.querySelector('.empty-state__header');
    assert.equal(
      subHead && subHead.textContent && subHead.textContent.trim(),
      'An error occurred. Please try again shortly.'
    );
  });

  test('should show custom error', async function(this: IMyTest, assert) {
    const task = createMockTask({ isError: true });

    setProperties(this, { task });
    await render(hbs`
      <ConcurrencyTaskStateHandler @task={{this.task}} @errorHeading="Custom Error">
        template block text
      </ConcurrencyTaskStateHandler>
    `);
    const subHead = this.element.querySelector('.empty-state__header');
    assert.equal(subHead && subHead.textContent && subHead.textContent.trim(), 'Custom Error');
  });
});
