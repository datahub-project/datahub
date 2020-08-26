import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../templates/components/custom-search-result-property-component/icon';
import { layout, tagName } from '@ember-decorators/component';
import { DataModelEntity } from '@datahub/data-models/constants/entity';
import { ICustomSearchResultPropertyComponentIcon } from '@datahub/data-models/types/search/custom-search-result-property-component/icon';

/**
 * Component to render field as icons. Normally used in the header of a search result
 */
@tagName('')
@layout(template)
export default class CustomSearchResultPropertyComponentSearchIcon extends Component {
  /**
   * The entity that we want to get the property from
   */
  entity?: DataModelEntity;

  /**
   * should it show the icon or not
   */
  value?: boolean;

  /**
   * Options for this component
   */
  options?: ICustomSearchResultPropertyComponentIcon['options'];
}
