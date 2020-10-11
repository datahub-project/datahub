import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { stubService } from '@datahub/utils/test-helpers/stub-service';
import { BaseEntity } from '@datahub/data-models/entity/base-entity';
import { IEntityRenderProps } from '@datahub/data-models/types/entity/rendering/entity-render-props';
import { TestContext } from 'ember-test-helpers';
import { MirageTestContext } from '@datahub/utils/types/vendor/ember-cli-mirage/mirage-tests';
import { getApiRoot, ApiVersion, ApiResponseStatus } from '@datahub/utils/api/shared';
import setupMirage from 'ember-cli-mirage/test-support/setup-mirage';
import { setupErrorHandler } from '@datahub/utils/test-helpers/setup-error';
import { ApiError } from '@datahub/utils/api/error';

const setupTest = async function(
  test: TestContext,
  options: {
    withBrowsePage: boolean;
    withNotFoundError?: boolean;
  }
): Promise<void> {
  class MockEntity extends BaseEntity<{}> {
    static get displayName(): string {
      return 'mock';
    }
    static get renderProps(): IEntityRenderProps {
      const renderProps: IEntityRenderProps = {
        apiEntityName: '',
        search: {
          attributes: [],
          autocompleteNameField: '',
          placeholder: '',
          defaultAspects: []
        }
      };
      if (options.withBrowsePage) {
        renderProps.browse = {
          showHierarchySearch: false
        };
      }
      return renderProps;
    }
    get name(): string {
      return 'entityName';
    }
    get displayName(): string {
      return this.staticInstance.displayName;
    }
  }
  stubService('configurator', {
    getConfig(): boolean {
      return true;
    }
  });

  stubService('data-models', {
    createInstance(_entityDisplayName: string, urn: string): Promise<MockEntity> {
      return new Promise(resolve => {
        if (options.withNotFoundError) {
          throw new ApiError(ApiResponseStatus.NotFound, 'Entity not found');
        } else {
          resolve(new MockEntity(urn));
        }
      });
    }
  });

  test.setProperties({
    entityClass: MockEntity,
    urn: 'some-urn',
    tabSelected: 'overview'
  });
  await render(
    hbs`<EntityPage::EntityPageMain @urn={{this.urn}} @entityClass={{this.entityClass}} @tabSelected={{this.tabSelected}} />`
  );
};

module('Integration | Component | entity-page/entity-page-main', function(hooks): void {
  setupRenderingTest(hooks);
  setupMirage(hooks);
  setupErrorHandler(hooks, { catchMsgs: ['Unexpected token E in JSON at position 0'] });

  test('it renders', async function(assert): Promise<void> {
    await setupTest(this, { withBrowsePage: false });
    assert.dom().hasText('MOCK entityName');
  });

  test('breadcrumb error', async function(this: MirageTestContext, assert): Promise<void> {
    this.server.namespace = getApiRoot(ApiVersion.v2);
    this.server.get('/browsePaths', () => 'Error', 500);
    await setupTest(this, { withBrowsePage: true });
    assert.dom().containsText('Error loading the path. Please refresh the page and try again');
  });

  test('not found error', async function(this: MirageTestContext, assert): Promise<void> {
    await setupTest(this, { withBrowsePage: true, withNotFoundError: true });
    assert.dom().containsText('Entity not found');
  });
});
