import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../templates/components/custom-search-result-property-component/link';
import { DataModelEntity } from '@datahub/data-models/constants/entity';
import { layout, tagName } from '@ember-decorators/component';
import { ICustomSearchResultPropertyComponentLink } from '@datahub/data-models/types/search/custom-search-result-property-component/link';

/**
 * Component to render external links
 */
@tagName('')
@layout(template)
export default class CustomSearchResultPropertyComponentSearchLink extends Component {
  /**
   * The entity that we want to get the property from
   */
  entity?: DataModelEntity;

  /**
   * Options for this component
   */
  options?: ICustomSearchResultPropertyComponentLink['options'];
}
