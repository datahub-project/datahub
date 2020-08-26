import Component from '@ember/component';
import { set } from '@ember/object';
import { computed } from '@ember/object';
import { action } from '@ember/object';
import {
  buildDatasetLiUrn,
  datasetUrnRegexLI,
  getDatasetUrnParts
} from '@datahub/data-models/entity/dataset/utils/urn';
import { arrayMap, arrayReduce } from '@datahub/utils/array/index';
import { TaskInstance, timeout, task } from 'ember-concurrency';
import { DatasetOrigins, IDatasetOrigin } from 'datahub-web/typings/api/datasets/origins';
import { classNames } from '@ember-decorators/component';
import { FabricType } from '@datahub/metadata-types/constants/common/fabric-type';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';
import { ETask } from '@datahub/utils/types/concurrency';

/**
 * Params to show dropdown
 */
interface IShowDropdownParams {
  actions: { open: () => void };
}

/**
 * Params to hide dropdown
 */
interface IHideDropdownParams {
  actions: { close: () => void };
}

@classNames('dataset-fabric-switcher')
export default class DatasetFabricSwitcher extends Component {
  /**
   * Urn for the related dataset
   * @type {string}
   * @memberof DatasetFabricSwitcher
   */
  urn!: string;

  /**
   * Toggle state of the switcher dropdown
   * @type {boolean}
   * @memberof DatasetFabricSwitcher
   */
  isExpanded = false;

  /**
   * Lists the Fabrics that are available for the dataset at this urn
   * @type {DatasetOrigins}
   * @memberof DatasetFabricSwitcher
   */
  fabrics: DatasetOrigins = [];

  /**
   * This creates an easy to use mapping between the fabric of a dataset (EI, CORP, PROD) to the cluster/origin of the
   * dataset.
   * @type {Record<Fabric, string>}
   */
  @computed('fabrics')
  get fabricsMap(): Record<FabricType, string> {
    return arrayReduce(
      (
        fabricMap: Partial<Record<FabricType, string>>,
        fabric: IDatasetOrigin
      ): Partial<Record<FabricType, string>> => ({
        ...fabricMap,
        [fabric.origin]: fabric.displayTitle
      }),
      {}
    )(this.fabrics) as Record<FabricType, string>;
  }

  /**
   * References the most recent TaskInstance to hide the dropdown options
   * @type {TaskInstance<Promise<void>>}
   * @memberof DatasetFabricSwitcher
   */
  mostRecentHide?: TaskInstance<Promise<void>>;

  /**
   * The current dataset's Fabric if available, culled from this urn
   * @type {(ComputedProperty<string | undefined>)}
   * @memberof DatasetFabricSwitcher
   */
  @computed('urn')
  get fabric(): string | undefined {
    const fabric = getDatasetUrnParts(this.urn).fabric;
    return fabric ? this.fabricsMap[fabric] : undefined;
  }

  /**
   * Lists the properties for the available Fabric urns related to this dataset including the  Fabric and urns
   * @type {(ComputedProperty<Array<{ fabric: Fabric; urn: string }>>)}
   * @memberof DatasetFabricSwitcher
   */
  @computed('fabrics')
  get fabricUrns(): Array<{ fabric: string; urn: string }> {
    const { fabrics, urn } = this;
    const match = datasetUrnRegexLI.exec(urn);

    if (match) {
      const [, platform, segment] = match;

      // Creates a map of fabric, and urn string
      return arrayMap((origin: IDatasetOrigin): { fabric: string; urn: string } => ({
        fabric: origin.displayTitle,
        urn: buildDatasetLiUrn(platform as DatasetPlatform, segment, origin.origin)
      }))(fabrics);
    }

    return [];
  }

  /**
   * Action handler to prevent bubbling DOM event action
   * @returns
   * @memberof DatasetFabricSwitcher
   */
  @action
  prevent(): boolean {
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
  @task(function*(this: DatasetFabricSwitcher, dropdown: IShowDropdownParams): IterableIterator<void> {
    set(this, 'isExpanded', true);
    dropdown.actions.open();
  })
  showFabricsTask: ETask<void, IShowDropdownParams>;

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
  @task(function*(this: DatasetFabricSwitcher, dropdown: IHideDropdownParams): IterableIterator<Promise<void>> {
    set(this, 'isExpanded', false);
    yield timeout(200);
    dropdown.actions.close();
  })
  hideFabricsTask: ETask<Promise<void>, IHideDropdownParams>;

  /**
   * Handles the DOM onmouseenter event to show list of Fabrics
   * @param dropdown {{ actions: { open: () => void } }}
   * @memberof DatasetFabricSwitcher
   */
  @action
  showFabrics(dropdown: { actions: { open: () => void } }): void {
    const lastHideTask = this.mostRecentHide;

    if (lastHideTask) {
      lastHideTask.cancel();
    }

    this.showFabricsTask.perform(dropdown);
  }

  /**
   * Handles the DOM event onmouseleave to hide the list of Fabrics and
   * stores a reference to the last invoked hideFabricsTask TaskInstance
   * @param dropdown {{ actions: { close: () => void } }}
   * @memberof DatasetFabricSwitcher
   */
  @action
  hideFabrics(dropdown: { actions: { close: () => void } }): void {
    set(this, 'mostRecentHide', this.hideFabricsTask.perform(dropdown));
  }
}
