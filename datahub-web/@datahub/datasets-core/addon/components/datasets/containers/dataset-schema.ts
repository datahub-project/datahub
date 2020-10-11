import Component from '@ember/component';
import { setProperties } from '@ember/object';
import { task } from 'ember-concurrency';
import { IDatasetColumnWithHtmlComments } from '@datahub/datasets-core/types/datasets/columns';
import { augmentObjectsWithHtmlComments } from '@datahub/datasets-core/utils/api/columns';
import { readDatasetSchemaByUrn } from '@datahub/datasets-core/utils/api/schema';
import { containerDataSource } from '@datahub/utils/api/data-source';
import { ETaskPromise } from '@datahub/utils/types/concurrency';
import { IDatasetSchemaColumn, IDatasetSchema } from '@datahub/metadata-types/types/entity/dataset/schema';
import { layout } from '@ember-decorators/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../../templates/components/datasets/containers/dataset-schema';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';

@layout(template)
@containerDataSource<DatasetSchemaContainer>('getDatasetSchemaTask', ['entity'])
export default class DatasetSchemaContainer extends Component {
  /**
   * The urn identifier for the dataset
   * @type {string}
   */
  entity!: DatasetEntity;

  /**
   * json string for the dataset schema properties
   * @type {string}
   */
  json?: string;

  /**
   * Stores the last modified date on the dataset schema as an utc time string
   * @type {string}
   */
  lastModifiedString = '';

  /**
   * List of schema properties for the dataset
   * @type {IDatasetColumnWithHtmlComments | IDatasetColumn}
   */
  schemas?: Array<IDatasetColumnWithHtmlComments | IDatasetSchemaColumn>;

  /**
   * If there is schema or not
   */
  isEmpty = false;

  /**
   * Reads the schema for the dataset
   */
  @task(function*(this: DatasetSchemaContainer): IterableIterator<Promise<IDatasetSchema>> {
    const { columns = [], rawSchema: json, lastModified } = ((yield readDatasetSchemaByUrn(
      this.entity.urn
    )) as unknown) as IDatasetSchema;

    const lastModifiedString = lastModified ? new Date(lastModified).toLocaleString() : '';

    const schemas = augmentObjectsWithHtmlComments(columns);
    // Specifically tests for null or undefined here as an empty string for json does not necessarily imply a lack of
    // schema
    const isEmpty = json === null || json === undefined;

    setProperties(this, { schemas, json: json || '{}', lastModifiedString, isEmpty });
  })
  getDatasetSchemaTask!: ETaskPromise<IDatasetSchema>;
}
