import Component from '@ember/component';
import { setProperties } from '@ember/object';
import { task } from 'ember-concurrency';
import { IDatasetColumn, IDatasetColumnWithHtmlComments } from 'wherehows-web/typings/api/datasets/columns';
import { IDatasetSchema } from 'wherehows-web/typings/api/datasets/schema';
import { augmentObjectsWithHtmlComments } from 'wherehows-web/utils/api/datasets/columns';
import { readDatasetSchemaByUrn } from 'wherehows-web/utils/api/datasets/schema';
import { containerDataSource } from '@datahub/utils/api/data-source';
import { ETaskPromise } from '@datahub/utils/types/concurrency';

@containerDataSource('getDatasetSchemaTask', ['urn'])
export default class DatasetSchemaContainer extends Component {
  /**
   * The urn identifier for the dataset
   * @type {string}
   */
  urn!: string;

  /**
   * json string for the dataset schema properties
   * @type {string}
   */
  json: string;

  /**
   * Stores the last modified date on the dataset schema as an utc time string
   * @type {string}
   */
  lastModifiedString: string = '';

  /**
   * List of schema properties for the dataset
   * @type {IDatasetColumnWithHtmlComments | IDatasetColumn}
   */
  schemas: Array<IDatasetColumnWithHtmlComments | IDatasetColumn>;

  /**
   * If there is schema or not
   */
  isEmpty: boolean = false;

  /**
   * Reads the schema for the dataset
   */
  @task(function*(this: DatasetSchemaContainer): IterableIterator<Promise<IDatasetSchema>> {
    let schemas,
      { columns = [], rawSchema: json, lastModified }: IDatasetSchema = yield readDatasetSchemaByUrn(this.urn);

    let lastModifiedString = lastModified ? new Date(lastModified).toLocaleString() : '';

    schemas = augmentObjectsWithHtmlComments(columns);

    setProperties(this, { schemas, json: json || '{}', lastModifiedString, isEmpty: !json });
  })
  getDatasetSchemaTask!: ETaskPromise<IDatasetSchema>;
}
