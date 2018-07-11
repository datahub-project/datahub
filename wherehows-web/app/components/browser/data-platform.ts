import Component from '@ember/component';
import { IDynamicLinkNode } from 'wherehows-web/typings/app/datasets/dynamic-link';
import { DatasetPlatform } from 'wherehows-web/constants';

export default class DataPlatform extends Component {
  /**
   * Props the dataset platform, including name and count of datasets within the platform
   * @type {{platform: DatasetPlatform | string, count?: number}}
   */
  platform: { platform: DatasetPlatform | string; count?: number };

  /**
   * References the dynamic link properties for the related platform
   * @type {IDynamicLinkNode}
   */
  node: IDynamicLinkNode;
}
