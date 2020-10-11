import { BaseEntity } from '@datahub/data-models/entity/base-entity';
import { IBaseEntity } from '@datahub/metadata-types/types/entity/index';
import { IEntityRenderProps } from '@datahub/data-models/types/entity/rendering/entity-render-props';

/**
 * Mock Entity for testing purposes only. The goal of this class is to
 * simplify testing by creating an metadata agnostic entity type that is a concrete implementation of BaseEntity.
 */
export class MockEntity<E = IBaseEntity> extends BaseEntity<E> {
  static displayName: 'mock-entity' = 'mock-entity';
  static kind = 'mock-entity';
  static get renderProps(): IEntityRenderProps {
    return {
      apiEntityName: 'mock-api',
      search: {
        placeholder: 'mock-placeholder',
        attributes: [
          {
            fieldName: 'pikachu',
            showInAutoCompletion: false,
            showInResultsPreview: false,
            showInFacets: false,
            displayName: 'Pikachu',
            desc: 'A mock field',
            example: 'Electrifying'
          },
          {
            fieldName: 'eevee',
            showInAutoCompletion: false,
            showInResultsPreview: false,
            showInFacets: false,
            displayName: 'Eevee',
            desc: 'A mock field',
            example: 'Evolutionary'
          }
        ],
        defaultAspects: []
      },
      entityPage: {
        route: 'entity-type.urn',
        tabProperties: [],
        defaultTab: 'mock-tab',
        apiRouteName: 'mock-api'
      }
    };
  }
  get displayName(): string {
    return MockEntity.displayName;
  }

  get name(): string {
    return 'mock entity';
  }

  constructor(readonly urn: string = 'mock-urn') {
    super(urn);
  }
}
