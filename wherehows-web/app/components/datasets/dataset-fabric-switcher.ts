import Component from '@ember/component';
import { computed, get, set, getProperties } from '@ember/object';
import { DatasetPlatform, Fabric } from 'wherehows-web/constants';
import { action } from '@ember-decorators/object';
import { buildLiUrn, datasetUrnRegexLI, getUrnParts } from 'wherehows-web/utils/validators/urn';
import { arrayMap } from 'wherehows-web/utils/array';
import { task, TaskInstance, timeout } from 'ember-concurrency';
import ComputedProperty from '@ember/object/computed';

export default class DatasetFabricSwitcher extends Component {
  classNames = ['dataset-fabric-switcher'];

  /**
   * Urn for the related dataset
   * @type {string}
   * @memberof DatasetFabricSwitcher
   */
  urn: string;

  /**
   * Toggle state of the switcher dropdown
   * @type {boolean}
   * @memberof DatasetFabricSwitcher
   */
  isExpanded: boolean = false;

  /**
   * Lists the Fabrics that are available for the dataset at this urn
   * @type {Array<Fabric>}
   * @memberof DatasetFabricSwitcher
   */
  fabrics: Array<Fabric>;

  /**
   * References the most recent TaskInstance to hide the dropdown options
   * @type {TaskInstance<Promise<void>>}
   * @memberof DatasetFabricSwitcher
   */
  mostRecentHide: TaskInstance<Promise<void>>;

  /**
   * Creates an instance of DatasetFabricSwitcher.
   * @memberof DatasetFabricSwitcher
   */
  constructor() {
    super(...arguments);
    // If fabrics is not provided as an external list, default to hard-coded list of
    // Fabrics in constants module
    this.fabrics || set(this, 'fabrics', Object.values(Fabric));
  }

  /**
   * The current dataset's Fabric if available, culled from this urn
   * @type {(ComputedProperty<Fabric | void>)}
   * @memberof DatasetFabricSwitcher
   */
  fabric: ComputedProperty<Fabric | void> = computed('urn', function(this: DatasetFabricSwitcher): Fabric | void {
    return getUrnParts(get(this, 'urn')).fabric;
  });

  /**
   * Lists the properties for the available Fabric urns related to this dataset including the  Fabric and urns
   * @type {(ComputedProperty<Array<{ fabric: Fabric; urn: string }>>)}
   * @memberof DatasetFabricSwitcher
   */
  fabricUrns = computed('fabrics', function(this: DatasetFabricSwitcher): Array<{ fabric: Fabric; urn: string }> {
    const { fabrics, urn } = getProperties(this, ['fabrics', 'urn']);
    const match = datasetUrnRegexLI.exec(urn);

    if (match) {
      const [, platform, segment] = match;

      // Creates a map of fabric, and urn string
      return arrayMap((fabric: Fabric) => ({
        fabric,
        urn: buildLiUrn(<DatasetPlatform>platform, segment, fabric)
      }))(fabrics);
    }

    return [];
  });

  /**
   * Action handler to prevent bubbling DOM event action
   * @returns
   * @memberof DatasetFabricSwitcher
   */
  @action
  prevent() {
    return false;
  }

  /**
   * EC task triggers the rendering of the list of dataset Fabrics
   * @type TaskProperty<void> & {
    perform: (a?: {
        actions: {
            open: () => void;
        };
    } | undefined) => TaskInstance<void>}
   * @memberof DatasetFabricSwitcher
   */
  showFabricsTask = task(function*(
    this: DatasetFabricSwitcher,
    dropdown: { actions: { open: () => void } }
  ): IterableIterator<void> {
    set(this, 'isExpanded', true);
    dropdown.actions.open();
  });

  /**
   * EC task triggers the occluding of the list of dataset Fabrics
   * @type TaskProperty<void> & {
    perform: (a?: {
        actions: {
            close: () => void;
        };
    } | undefined) => TaskInstance<void>}
   * @memberof DatasetFabricSwitcher
   */
  hideFabricsTask = task(function*(
    this: DatasetFabricSwitcher,
    dropdown: { actions: { close: () => void } }
  ): IterableIterator<Promise<void>> {
    set(this, 'isExpanded', false);
    yield timeout(200);
    dropdown.actions.close();
  });

  /**
   * Handles the DOM onmouseenter event to show list of Fabrics
   * @param dropdown {{ actions: { open: () => void } }}
   * @memberof DatasetFabricSwitcher
   */
  @action
  showFabrics(dropdown: { actions: { open: () => void } }) {
    const lastHideTask = get(this, 'mostRecentHide');

    if (lastHideTask) {
      lastHideTask.cancel();
    }

    get(this, 'showFabricsTask').perform(dropdown);
  }

  /**
   * Handles the DOM event onmouseleave to hide the list of Fabrics and
   * stores a reference to the last invoked hideFabricsTask TaskInstance
   * @param dropdown {{ actions: { close: () => void } }}
   * @memberof DatasetFabricSwitcher
   */
  @action
  hideFabrics(dropdown: { actions: { close: () => void } }) {
    set(this, 'mostRecentHide', get(this, 'hideFabricsTask').perform(dropdown));
  }
}
