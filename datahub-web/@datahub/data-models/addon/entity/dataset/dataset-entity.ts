import { BaseEntity, IBaseEntityStatics, statics } from '@datahub/data-models/entity/base-entity';
import { IDatasetApiView } from '@datahub/metadata-types/types/entity/dataset/dataset-entity';
import { readDataset } from '@datahub/data-models/api/dataset/dataset';
import { readDatasetSchema } from '@datahub/data-models/api/dataset/schema';
import DatasetSchema from '@datahub/data-models/entity/dataset/modules/schema';
import { set } from '@ember/object';
import { isNotFoundApiError } from '@datahub/utils/api/shared';
import { IEntityRenderProps } from '@datahub/data-models/types/entity/rendering/entity-render-props';
import { oneWay } from '@ember/object/computed';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';
import { decodeUrn } from '@datahub/utils/validators/urn';
import { readCategories } from '@datahub/data-models/entity/dataset/read-categories';
import { getPrefix } from '@datahub/data-models/entity/dataset/utils/segments';
import { readDatasetsCount } from '@datahub/data-models/api/dataset/count';
import { setProperties } from '@ember/object';
import { DatasetLineage } from '@datahub/data-models/entity/dataset/modules/lineage';
import { readUpstreamDatasets } from '@datahub/data-models/api/dataset/lineage';
import { DatasetLineageList } from '@datahub/metadata-types/types/entity/dataset/lineage';
import { getRenderProps } from '@datahub/data-models/entity/dataset/render-props';
import { NotImplementedError } from '@datahub/data-models/constants/entity/shared';
import { IDatasetSnapshot } from '@datahub/metadata-types/types/metadata/dataset-snapshot';
import { IBrowsePath, IEntityLinkAttrsWithCount, IEntityLinkAttrs } from '@datahub/data-models/types/entity/shared';
import { IInstitutionalMemory } from '@datahub/data-models/types/entity/common/wiki/institutional-memory';
import { readDatasetInstitutionalMemory, writeDatasetInstitutionalMemory } from '@datahub/data-models/api/dataset/wiki';
import { InstitutionalMemory } from '@datahub/data-models/models/aspects/institutional-memory';
import { returnDefaultIfNotFound } from '@datahub/utils/api/fetcher';

/**
 * Common function used here for the read operation for datasets. If an item is not found we often have to
 * default to a UI provided factory value, which can be inserted here to be run as part of the catch
 * function in a promise situation
 * @type {<T>(value: T): (e: Error) => T}
 */
const returnValueIfNotFound = <T>(value: T): ((e: Error) => T) => (e: Error): T => {
  if (isNotFoundApiError(e)) {
    return value;
  }
  throw e;
};

/**
 * Defines the data model for the Dataset entity.
 */
@statics<IBaseEntityStatics<IDatasetApiView>>()
export class DatasetEntity extends BaseEntity<IDatasetApiView> {
  /**
   * The human friendly alias for Dataset entities
   */
  static displayName: 'datasets' = 'datasets';

  get displayName(): 'datasets' {
    return DatasetEntity.displayName;
  }

  /**
   * Discriminant when included in a tagged Entity union
   */
  static kind = 'DatasetEntity';

  get kind(): string {
    return DatasetEntity.kind;
  }

  /**
   * Shows the urn as an unencoded value
   */
  get rawUrn(): string {
    return decodeUrn(this.urn);
  }

  /**
   * Creates a link for this specific entity instance, useful for generating a dynamic link from a
   * single particular dataset entity
   */
  get linkForEntity(): IEntityLinkAttrs {
    return DatasetEntity.getLinkForEntity({
      entityUrn: this.urn,
      displayName: this.name
    });
  }

  /**
   * Reads the snapshots for a list of Dataset urns
   * @static
   */
  static readSnapshots(_urns: Array<string>): Promise<Array<IDatasetSnapshot>> {
    throw new Error(NotImplementedError);
  }

  /**
   * Holds the classified data retrieved from the API layer for the schema information as a DatasetSchema
   * class
   * @type {DatasetSchema}
   */
  schema?: DatasetSchema;

  /**
   * Holds the classified data retrieved from the API layer for the upstream datasets list as an array of
   * DatasetLineage classes
   * @type {Array<DatasetLineage>}
   */
  upstreams?: Array<DatasetLineage>;

  /**
   * Holds the raw data from the API layer for the institutional memory wiki links related to this dataset
   * entity by urn
   */
  institutionalMemories?: Array<InstitutionalMemory>;

  /**
   * Reference to the data entity, is the data platform to which the dataset belongs
   * @type {DatasetPlatform}
   */
  @oneWay('entity.platform')
  platform?: DatasetPlatform;

  /**
   * Reference to the data entity's native name, should not be something that is editable but gives us a
   * more human readable form for the dataset vs the urn
   */
  get name(): string {
    return this.entity ? this.entity.nativeName : '';
  }

  /**
   * Retrieves the value of the Dataset entity identified by this.urn
   */
  get readEntity(): Promise<IDatasetApiView> {
    return readDataset(this.urn);
  }

  /**
   * Class properties common across instances
   * Dictates how visual ui components should be rendered
   * Implemented as a getter to ensure that reads are idempotent
   * @readonly
   * @static
   * @type {IEntityRenderProps}
   */
  static get renderProps(): IEntityRenderProps {
    return getRenderProps();
  }

  /**
   * Builds a search query keyword from a list of segments for the a DatasetEntity instance
   */
  static getQueryForHierarchySegments(_segments: Array<string>): never {
    throw new Error(NotImplementedError);
  }

  /**
   * Is a promise that retrieves the schema information and returns and sets the schema information in a
   * thennable format
   */
  async readSchema(): Promise<DatasetSchema> {
    const schema = await readDatasetSchema(this.urn);
    const datasetSchema = new DatasetSchema(schema);
    set(this, 'schema', datasetSchema);
    return datasetSchema;
  }

  /**
   * Asynchronously retrieves the upstream datasets for this dataset and assigns to the upstreams property
   */
  async readUpstreams(): Promise<Array<DatasetLineage>> {
    const lineageList = await readUpstreamDatasets(this.urn).catch(
      // Handling for an expected possibility of receiving a 404 error for this upstream dataset response, which
      // would require us to replace error with just an empty list
      returnValueIfNotFound([] as DatasetLineageList)
    );

    const upstreams = lineageList.map((lineage): DatasetLineage => new DatasetLineage(lineage));
    set(this, 'upstreams', upstreams);
    return upstreams;
  }

  /**
   * Reads the institutional memory wiki links related to this dataset and stores it on the entity as well as returning
   * it as a value from this function
   */
  async readInstitutionalMemory(): Promise<Array<InstitutionalMemory>> {
    // Handling for expected possibility of receiving a 404 for institutional memory for this dataset, which would
    // likely mean nothing has been added yet and we should allow the user to be the first to add something
    const { elements: institutionalMemories } = await returnDefaultIfNotFound(
      readDatasetInstitutionalMemory(this.urn),
      {
        elements: [] as Array<IInstitutionalMemory>
      }
    );

    const institutionalMemoriesMap = institutionalMemories.map(
      (link): InstitutionalMemory => new InstitutionalMemory(link)
    );
    set(this, 'institutionalMemories', institutionalMemoriesMap);
    return institutionalMemoriesMap;
  }

  /**
   * Writes the institutional memory wiki links back to the backend to save any user changes (add/delete)
   */
  async writeInstitutionalMemory(): Promise<void> {
    const { institutionalMemories } = this;
    institutionalMemories &&
      (await writeDatasetInstitutionalMemory(
        this.urn,
        institutionalMemories.map((link): IInstitutionalMemory => link.readWorkingCopy())
      ));
  }

  /**
   * Interim implementation to read categories for datasets
   * TODO META-8863
   */
  static async readCategories(...args: Array<string>): Promise<IBrowsePath> {
    const [category, ...rest] = args;
    const prefix = await getPrefix(category || '', rest || []);
    const entitiesOrCategories = await readCategories(category || '', prefix);
    const groups = entitiesOrCategories
      .filter(({ entityUrn }): boolean => !entityUrn)
      .map(
        ({ segments = [], displayName }): IEntityLinkAttrsWithCount<unknown> =>
          this.getLinkForCategory({
            segments,
            displayName,
            count: 0
          })
      );
    const entities = entitiesOrCategories
      .filter(({ entityUrn }): boolean => Boolean(entityUrn))
      .map(
        ({ entityUrn = '', displayName }): IEntityLinkAttrs<unknown> =>
          this.getLinkForEntity({
            entityUrn,
            displayName
          })
      );

    return {
      segments: args,
      title: rest[rest.length - 1] || category || this.displayName,
      count: 0,
      entities,
      groups
    };
  }

  // TODO META-8863 remove once dataset is migrated
  static async readCategoriesCount(...args: Array<string>): Promise<number> {
    const [category, ...rest] = args;
    const prefix = await getPrefix(category || '', rest || []);
    return await readDatasetsCount({ platform: category || '', prefix });
  }

  constructor(readonly urn: string, entityData?: IDatasetApiView) {
    super(urn);
    // Sometimes we do not need readEntity to get this information as it was already provided by another entity
    // and we can just instantiate the class with it
    if (entityData) {
      set(this, 'entity', entityData);
    }
  }
}

/**
 * Custom factory for the dataset entity. We don't use the base entity factory as the dataset behavior currently
 * does not match the expectations of the base entity. Moving forward, when the backend response matches with the
 * expected dataset behavior, we may find a use case to rely on that function rather than use of this custom one
 * @param {string} urn - provides the context for what dataset entity to create by its urn identifier
 */
export const createDatasetEntity = async (urn: string, fetchedEntity?: IDatasetApiView): Promise<DatasetEntity> => {
  const dataset = new DatasetEntity(urn);
  const entity = fetchedEntity || (await dataset.readEntity);

  setProperties(dataset, {
    entity
  });

  return dataset;
};
