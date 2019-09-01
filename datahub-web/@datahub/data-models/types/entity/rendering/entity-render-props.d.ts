import { ISearchEntityRenderProps } from '@datahub/data-models/types/entity/rendering/search-entity-render-prop';
import { Tab, ITabProperties } from '@datahub/data-models/constants/entity/shared/tabs';
import { EntityRoute } from '@datahub/data-models/types/entity/shared';

/**
 * Defines the interface of objects expected to be passed to a nacho table instance
 * For example, they are used to describe the interface properties extracted from a snapshot
 * @export
 * @interface IAttributeValue
 */
export interface IAttributeValue {
  // The attribute key or display label
  attribute: string;
  // String representation of the attributes value
  value?: string;
}

/**
 * Defines a set of properties that guide the rendering of ui elements and features in the host application
 * @interface IEntityRenderProps
 */
export interface IEntityRenderProps {
  search: {
    // Lists a set of render properties that indicate how search attributes for an entity are rendered in the ui
    attributes: Array<ISearchEntityRenderProps>;
    // Placeholder text describing call to action for search input
    placeholder: string;
    // API use a specific name to refer to an entity. For example, 'features' in api is 'feature'.
    apiName: string;
    // Lists the optional search result item secondary action component string references,
    // actions are rendered in the order they are presented, this may evolve to include options for rendering such as properties e.t.c
    secondaryActions?: Array<string>;
  };
  userEntityOwnership?: {
    // List of fields to render in the `I Own` page if different from search fields.
    attributes: Array<ISearchEntityRenderProps>;
  };
  browse: {
    // Flag indicating if the count of entities should be rendered in the browse page
    // TODO META-8863 remove once dataset is migrated
    showCount: boolean;
    // Flag indicating if the `SearchWithinHierarchy` component should be rendered on the browse page
    // This performs an advanced search query with the category fields populated in the query string
    showHierarchySearch: boolean;
    // Route for the entity page
    entityRoute: EntityRoute;
    // List of field metadata that browse may use. For example, it can send specific params to API to
    // get a custom browse experience.
    attributes?: Array<ISearchEntityRenderProps>;
  };
  // Specifies properties for rendering the 'profile' page of an entity, aka Entity page
  entityPage: {
    // Lists the sub navigation tab ids for the associated entity
    tabIds: Array<Tab>;
    // Lists the tab properties for the tabs on this Entity
    tabProperties: Array<ITabProperties>;
    // Specifies the default tab from the list of tabIds if a specific tab is not provided on navigation
    defaultTab: Tab;
    // Placeholder rendered when attribute are missing for entity metadata
    attributePlaceholder: string;
  };
  // Specifies attributes for displaying Entity properties in a list component
  list?: {
    fields: Array<{
      // Flag indicating that this field's value should be shown in a list item view
      showInResultsPreview: boolean;
      // The title name for the field
      displayName: string;
      // The name of the field to display
      fieldName: string;
      // Optional compute function on how to render the field value
      compute?: (arg: unknown) => string;
    }>;
  };
}
