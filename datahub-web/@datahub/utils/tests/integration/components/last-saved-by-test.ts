import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, find } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import moment from 'moment';

module('Integration | Component | last-saved-by', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders', async function(assert): Promise<void> {
    await render(hbs`{{last-saved-by}}`);

    assert.ok(
      this.element.textContent?.includes('Never'),
      'it shows a modification time of never when no attributes are passed'
    );
    assert.ok(
      this.element.textContent?.includes('Last Saved:'),
      'it shows a default title when no title attribute is passed'
    );
  });

  test('property rendering', async function(assert): Promise<void> {
    const time = new Date().getTime();
    const actor = 'John Appleseed';
    const title = 'Last Modified';

    this.set('time', time);
    this.set('actor', actor);
    this.set('title', title);

    await render(hbs`{{last-saved-by time=time actor=actor}}`);

    assert.ok(this.element.textContent?.includes(moment(time).fromNow()), 'it shows the last saved time from now');
    assert.ok(this.element.textContent?.includes(actor), 'it shows the actor attribute');

    await render(hbs`{{last-saved-by time=time title=title}}`);

    assert.ok(this.element.textContent?.includes('Last Modified'), 'it shows the passed in title');
    assert.ok(!this.element.textContent?.includes('Last Saved:'), 'it does not show the default title');

    await render(hbs`
      {{#last-saved-by time=time title=title actor=actor as |ls|}}
        <div class="yielded-title">{{ls.title}}</div>
        <div class="yielded-actor">{{ls.actor}}</div>
      {{/last-saved-by}}
    `);

    assert.equal(find('.yielded-title')?.textContent?.trim(), title, 'block usage yields the title');
    assert.equal(find('.yielded-actor')?.textContent?.trim(), actor, 'block usage yields the actor');
  });
});
