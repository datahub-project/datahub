import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { defaultAnchorClass } from '@datahub/utils/components/link/optional-value';
import { IDynamicLinkNode } from '@datahub/utils/types/vendor/dynamic-link';
import setupRouter from '@datahub/utils/test-helpers/setup-router';

module('Integration | Component | link/optional-value', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders, behaves, and passes attributes as expected', async function(assert): Promise<void> {
    setupRouter(this, function() {
      this.route('route', function() {
        this.route('model1', { path: '/:model1' }, function() {
          this.route('model2', { path: '/:model2' }, function() {});
        });
      });
    });

    const value: IDynamicLinkNode = {
      title: 'A Route',
      text: 'A Route',
      route: 'route'
    };
    const valueWithModel: IDynamicLinkNode<string> = {
      title: 'A Route',
      text: 'A Route',
      route: 'route.model1',
      model: 'model1'
    };
    const valueWithModelArray: IDynamicLinkNode<Array<string>> = {
      title: 'A Route',
      text: 'A Route',
      route: 'route.model1.model2',
      model: ['model1', 'model2']
    };
    const valueWithModelArrayWithQueryParams: IDynamicLinkNode<Array<string>, string, { param: number }> = {
      title: 'A Route',
      text: 'A Route',
      route: 'route.model1.model2',
      model: ['model1', 'model2'],
      queryParams: {
        param: 1
      }
    };
    const aClass = 'class';

    this.setProperties({ value, aClass });

    await render(hbs`<Link::OptionalValue />`);

    assert.dom().hasText('', 'Expected to render an empty string with no arguments passed in');
    assert.dom(`#${this.element.id} a`).doesNotExist();

    await render(hbs`<Link::OptionalValue @value={{this.value}} />`);

    assert.dom().hasText(value.text);
    assert.dom(`#${this.element.id} a`).exists();
    assert.dom(`#${this.element.id} a`).hasClass(defaultAnchorClass);

    await render(hbs`<Link::OptionalValue @value={{this.value}} @linkClass={{this.aClass}} />`);
    assert.dom(`#${this.element.id} a`).hasClass(aClass);

    this.set('value', valueWithModel);
    assert.dom(`a`).hasAttribute('href', '#/route/model1');

    this.set('value', valueWithModelArray);
    assert.dom(`a`).hasAttribute('href', '#/route/model1/model2');

    this.set('value', valueWithModelArrayWithQueryParams);
    assert.dom(`a`).hasAttribute('href', '#/route/model1/model2?param=1');
  });
});
