import Component from '@ember/component';
import { set, get, setProperties, getProperties } from '@ember/object';
import { warn } from '@ember/debug';

/**
 * Cached module reference to the class name for visually hiding toggled off schema view
 * @type {string}
 */
const hiddenClassName = 'hidden';

export default class DatasetSchema extends Component {
  /**
   * Flag indicating that the schema should be rendered in a tabular format
   * @type {boolean}
   * @default true
   */
  isTable = true;

  /**
   * Default json schema string
   * @type {string}
   */
  json = '{}';

  /**
   * Reference to the jsonViewer dom element
   * @type {Element}
   */
  jsonViewer = <Element | null>null;

  /**
   * Reference to the jsonTable dom element
   * @type {Element}
   */
  jsonTable = <Element | null>null;

  /**
   * Constructs a readable JSON structure of the dataset schema
   * @param {Element} jsonViewer
   * @return {JQuery}
   */
  buildJsonView(this: DatasetSchema, jsonViewer: Element): JQuery | void {
    try {
      return $(jsonViewer).JSONView(JSON.parse(get(this, 'json')));
    } catch (e) {
      warn(`Exception occurred parsing JSON schema: ${e.message}`);
    }
  }

  /**
   * Constructs a table view of the dataset schema
   * @param {Element} jsonTable
   * @return {JQuery}
   */
  buildTableView = (jsonTable: Element) => $(jsonTable).treegrid();

  /**
   * Retains references to the DOM elements for showing the schema
   */
  cacheDomReference(this: DatasetSchema) {
    const { jsonViewer, jsonTable } = getProperties(this, ['jsonTable', 'jsonViewer']);

    if (!(jsonViewer && jsonTable)) {
      const dom = get(this, 'element');

      setProperties(this, {
        jsonViewer: dom.querySelector('#json-viewer'),
        jsonTable: dom.querySelector('#json-table')
      });
    }
  }

  didReceiveAttrs(this: DatasetSchema) {
    if (get(this, 'isTable')) {
      const jsonTable = get(this, 'jsonTable');
      jsonTable && this.buildTableView(jsonTable);
    } else {
      const jsonViewer = get(this, 'jsonViewer');
      jsonViewer && this.buildJsonView(jsonViewer);
    }
  }

  didRender() {
    this.cacheDomReference();
  }

  willDestroyElement(this: DatasetSchema) {
    setProperties(this, {
      jsonViewer: null,
      jsonTable: null
    });
  }

  actions = {
    /**
     * Handles the toggle for which schema view to be rendered.
     * Currently toggled between a table render and a JSON view
     * @param {"table" | "json"} [view = table]
     */
    showView(this: DatasetSchema, view: 'table' | 'json' = 'table') {
      const isTable = set(this, 'isTable', view === 'table');
      const { jsonViewer, jsonTable } = getProperties(this, ['jsonTable', 'jsonViewer']);

      if (jsonTable && jsonViewer) {
        isTable
          ? (jsonViewer.classList.add(hiddenClassName), jsonTable.classList.remove(hiddenClassName))
          : (jsonViewer.classList.remove(hiddenClassName), jsonTable.classList.add(hiddenClassName));
      }
    }
  };
}
