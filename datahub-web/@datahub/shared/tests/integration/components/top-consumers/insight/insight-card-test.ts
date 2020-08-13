import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { baseClass, PREVIEW_LIMIT } from '@datahub/shared/components/top-consumers/insight/insight-card';
import { IDynamicLinkParams } from 'dynamic-link/components/dynamic-link';
import { TopConsumer } from '@datahub/metadata-types/constants/metadata/top-consumers';

module('Integration | Component | top-consumers/insight/insight-card', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders and behaves as expected', async function(assert): Promise<void> {
    const title = 'test title';
    const testLink = 'https://linkedin.com';
    const testText = 'linkedin';

    const topConsumerLinkParam: IDynamicLinkParams = {
      href: testLink,
      text: testText
    };

    const topConsumers = new Array(10).fill(topConsumerLinkParam);

    this.setProperties({
      title,
      topConsumers,
      consumerType: TopConsumer.GROUP
    });

    await render(hbs`
      <TopConsumers::Insight::InsightCard
        @title={{this.title}}
        @topConsumers={{this.topConsumers}}
        @consumerType={{this.consumerType}}
      />
    `);

    const insightClassSelector = `.${baseClass}`;
    const insightTitleClassSelector = `.${baseClass}__title`;
    const insightItemClassSelector = `${baseClass}__item`;
    const showMoreClassSelector = `.${baseClass}__show-more`;

    assert.dom(insightClassSelector).exists();
    assert.dom(insightTitleClassSelector).hasText(title);

    assert.equal(
      document.getElementsByClassName(insightItemClassSelector).length,
      PREVIEW_LIMIT,
      'Expected number of items rendered to be equal to the preview limit'
    );

    const numberOfItemsHidden = topConsumers.length - PREVIEW_LIMIT;
    assert.dom(showMoreClassSelector).hasText(`${numberOfItemsHidden} more`);

    assert.equal(
      document.querySelector(`.${insightItemClassSelector}`)?.getAttribute('href'),
      testLink,
      'Expects the insight card to render the correct link'
    );
  });
});
