import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, click } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import sinonTest from 'ember-sinon-qunit/test-support/test';
import { getRenderedComponent } from '@datahub/utils/test-helpers/register-component';
import { stubService } from '@datahub/utils/test-helpers/stub-service';
import EntityBaseContainer from '@datahub/shared/components/entity-page/entity-base-container';
import sinon from 'sinon';
import { noop } from 'lodash';

const name = 'fake-entity-class';
class DummyEntityTestClass {
  name = name;
  static displayName = 'dummy';
  static renderProps = {
    entityPage: {
      tabProperties: [
        {
          id: 'overview',
          title: 'Overview',
          contentComponent: ''
        }
      ]
    }
  };
}

module('Integration | Component | entity-base-container', function(hooks): void {
  setupRenderingTest(hooks);
  const currentTab = 'currentTab';
  const urn = 'urn';
  const entityClass = DummyEntityTestClass;

  hooks.beforeEach(function(): void {
    stubService('configurator', {
      getConfig(): boolean {
        return true;
      }
    });
  });

  test('it renders and yields attributes', async function(assert): Promise<void> {
    this.setProperties({ currentTab, entityClass, urn });
    stubService('data-models', {
      createInstance: noop
    });

    await render(hbs`
      <EntityPage::EntityBaseContainer @currentTab={{this.currentTab}} @entityClass={{this.entityClass}} @urn={{this.urn}} as |container|>
        {{container.currentTab}}
        {{container.entityClass.name}}
      </EntityPage::EntityBaseContainer>
    `);

    assert.dom().hasText(`${currentTab} ${DummyEntityTestClass.name}`);
  });

  sinonTest('it handles tab selection', async function(this: SinonTestContext, assert): Promise<void> {
    const transitionTo = sinon.fake();

    stubService('router', {
      transitionTo
    });
    stubService('data-models', {
      createInstance: noop
    });

    this.setProperties({ currentTab, entityClass, urn });

    await render(hbs`
      <EntityPage::EntityBaseContainer @urn={{this.urn}} @entityClass={{this.entityClass}} @currentTab={{this.currentTab}} as |container|>
        <button {{on "click" (fn container.tabSelectionDidChange "differentTabName")}} class="clickable" type="button"></button>
      </EntityPage::EntityBaseContainer>
    `);

    await click('.clickable');

    assert.ok(
      transitionTo.called,
      'Expected the container to invoke the transitionTo method on the router after tabSelectionDidChange is invoked'
    );
  });

  sinonTest('it creates an instance on DOM insertion', async function(this: SinonTestContext, assert): Promise<void> {
    stubService('data-models', {
      createInstance(): DummyEntityTestClass {
        return new DummyEntityTestClass();
      }
    });

    this.setProperties({ urn, entityClass, currentTab });

    const component = await getRenderedComponent({
      ComponentToRender: EntityBaseContainer,
      testContext: this,
      template: hbs`
        <EntityPage::EntityBaseContainer @urn={{this.urn}} @entityClass={{this.entityClass}} @currentTab={{this.currentTab}} as |container|>
          {{container.entity.name}}
        </EntityPage::EntityBaseContainer>
      `,
      componentName: 'entity-page/entity-base-container'
    });

    assert.ok(
      component.entity instanceof DummyEntityTestClass,
      'Expected the container to set an instance of entityClass by calling createInstance on the data-models service after DOM insertion'
    );

    assert.deepEqual(
      component.tabs,
      DummyEntityTestClass.renderProps.entityPage.tabProperties,
      'Expected component tabs member to match the static member on the entity class'
    );

    assert.dom().hasText(name);
  });
});
