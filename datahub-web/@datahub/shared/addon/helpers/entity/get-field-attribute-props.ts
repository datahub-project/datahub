import { helper } from '@ember/component/helper';
import { DataModelEntityInstance } from '@datahub/data-models/constants/entity/index';
import { ISearchEntityRenderProps } from '@datahub/data-models/types/search/search-entity-render-prop';

/**
 * Sometimes we want to access directly a certain field's attributes from an entity's render props,
 * but we only have the field name to go off of. Rather than trying weird template logic or
 * individual container logic, this helper makes it easy to get the object for further query
 * @param dataEntity - the data model entity instace that we are working with
 * @param fieldName - the key (corresponds to fieldName is renderProps.attributes) to find the props for
 */
export function entityGetFieldAttributeProps([dataEntity, fieldName]: [
  DataModelEntityInstance,
  string
]): ISearchEntityRenderProps | null {
  const renderProps = dataEntity.staticInstance.renderProps;
  const { search } = renderProps;
  if (!search) {
    return null;
  }

  return search.attributes.findBy('fieldName', fieldName) || null;
}

export default helper(entityGetFieldAttributeProps);
