import Component from '@glimmer/component';
import { computed, set, setProperties, action } from '@ember/object';
import { IDatasetColumnWithHtmlComments } from '@datahub/datasets-core/types/datasets/columns';
import { IDatasetSchemaColumn } from '@datahub/metadata-types/types/entity/dataset/schema';

/**
 * Presentational component for rendering a JSON Schema, either with a Table or a JSON representation
 * @export
 * @class DatasetSchema
 * @extends {Component}
 */
export default class DatasetSchema extends Component<{
  /**
   * Externally supplied JSON value for the related Dataset's schema. This is defaulted to an empty JSON object
   */
  json: string;
  /**
   * List of columns in the Dataset and additional attributes, used to back the table representation of the Dataset's schema
   */
  schemas: Array<IDatasetColumnWithHtmlComments | IDatasetSchemaColumn>;

  /**
   * Schema last modified
   */
  lastModified: Date;
}> {
  /**
   * Flag indicating that the schema should be rendered with a tabular representation
   * This currently is driven by the schemas attribute
   */
  isShowingTable = true;

  /**
   * Flag indicating that the component was able to successfully parse the value found in this.json as a JSON string
   */
  isValidJson = false;

  /**
   * This will reference the prettified JSON input from this.JSON.
   * By default, it points to the same value
   */
  possibleJsonOutput = this.args.json;

  /**
   * Toggle the render mode for ember-ace editor between JSON and text depending on the last task
   * @readonly
   */
  @computed('isValidJson')
  get aceMode(): string {
    const acePath = 'ace/mode/';
    // Treat content as text unless we are successfully able to parse as JSON
    // the task is always run when the user toggles the view type
    const jsonOrPlainText = this.isValidJson ? 'json' : 'text';
    return `${acePath}${jsonOrPlainText}`;
  }

  /**
   * Format the JSON string as a human friendly string
   */
  prettifyJson(_r: boolean = this.isShowingTable): string {
    return JSON.stringify(JSON.parse(this.args.json), null, '\t');
  }

  /**
   * Handle user toggling between JSON and tabular view of schema.
   * If requested view is JSON, then argument to shouldShowTable is false
   * An error will be thrown by the task if an invalid JSON string is provided
   * @param {boolean} [shouldShowTable=this.isShowingTable] Flag indicating the user toggled view,
   * if no value is passed in e.g. when attrs are updated, we retain the currently set flag state
   */

  @action
  onToggleSchemaViewTypeTask(shouldShowTable: DatasetSchema['isShowingTable'] = this.isShowingTable): void {
    let output = this.args.json;

    // Attempt to parse JSON
    try {
      if (!shouldShowTable) {
        output = this.prettifyJson();
        set(this, 'isValidJson', true);
      }
    } catch (e) {
      // Only handle errors related to parsing JSON
      if (!(e.message as string).startsWith('Unexpected token')) {
        throw e;
      }

      set(this, 'isValidJson', false);
    } finally {
      // Always toggle state even if the JSON is invalid , source value (this.json) will be rendered as plain text
      setProperties(this, { possibleJsonOutput: output, isShowingTable: shouldShowTable });
    }
  }
}
