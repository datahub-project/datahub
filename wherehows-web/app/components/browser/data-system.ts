import Component from '@ember/component';
import { computed, get } from '@ember/object';
import { IDynamicLinkNode } from 'wherehows-web/typings/app/datasets/dynamic-link';

export default class DataSystem extends Component {
  tagName = 'li';

  classNames = ['data-system'];

  /**
   * References the dynamic link properties for a data system
   * @type {IDynamicLinkNode}
   */
  dataSystem: IDynamicLinkNode;

  /**
   * Determines if the related data system directly references a dataset
   * @type {ComputedProperty<boolean>}
   */
  isDataset = computed(function(this: DataSystem): boolean {
    const { route } = get(this, 'dataSystem');
    return route === 'datasets.dataset';
  });
}
