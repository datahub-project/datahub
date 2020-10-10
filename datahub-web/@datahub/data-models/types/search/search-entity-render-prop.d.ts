import { ICustomSearchResultPropertyComponent } from '@datahub/data-models/types/search/custom-search-result-property-component';
import { IDynamicComponent } from '@datahub/shared/types/dynamic-component';

export interface IEntityRenderCommonPropsSearch {
  // Lists a set of render properties that indicate how search attributes for an entity are rendered in the ui
  attributes: Array<ISearchEntityRenderProps>;
  // Should show facets in search, by default is true
  showFacets?: boolean;
  // Lists the optional search result item secondary action component string references,
  // actions are rendered in the order they are presented, this may evolve to include options for rendering such as properties e.t.c
  secondaryActionComponents?: Array<ICustomSearchResultPropertyComponent>;
  // Lists the optional search result item footer component string references
  // Footer components are rendered in the order they are presented, this may evolve to include options for rendering
  customFooterComponents?: Array<ICustomSearchResultPropertyComponent>;
  searchResultEntityFields?: {
    // Field that returns the name of the entity, by default is 'name'
    name?: string;
    // Field that returns the description of the entity, by default is 'description'
    description?: string;
    // Field that returns the entity picture of the entity, it won't be shown by default
    pictureUrl?: string;
  };
  // Whether or not this entity is searchable through conventional means in the application. One
  // can still have search props but have isEnabled set to false if the entity relies on search but
  // is not necessary available in the global search functionality for the app
  // Note: Though this parameter is optional, items that rely on listing entities that are searchable
  // will default this to false
  isEnabled?: boolean;
  defaultAspects: Array<string>;
}
/**
 * This interface should live in search addon, but search addon depends on data-models and
 * we don't want to create a circular dependency. Storing this interface at data-models for now.
 *
 * Property defintion in the context of search result
 */
export interface IEntityRenderPropsSearch extends IEntityRenderCommonPropsSearch {
  // Placeholder text describing call to action for search input
  placeholder: string;
  // By default autocomplete field is `name`, but some entities maybe need some other field
  autocompleteNameField?: string;
}

/**
 * This interface should live in search addon, but search addon depends on data-models and
 * we don't want to create a circular dependency. Storing this interface at data-models for now.
 *
 * Defines attributes that determine how search related visual elements are rendered
 * @export
 * @interface ISearchEntityRenderProps
 */
export interface ISearchEntityRenderProps {
  // The name of the search entity attribute that these render properties apply to e.g. dataorigin
  fieldName: string;
  // Alias of the field to use for the data model entity property to avoid name collisions
  fieldNameAlias?: string;
  // Flag indicating that this search entity attribute should be available or hidden from search auto complete
  showInAutoCompletion: boolean;
  // Flag indicating that this search entity attribute should be available or hidden from results preview
  showInResultsPreview: boolean;
  // Flag indicating that this search entity attribute should be shown as a search facet property
  showInFacets: boolean;
  // Component to use for tag, if none, it won't render tag
  tagComponent?: ICustomSearchResultPropertyComponent;
  // An alternative string representation of the fieldName attribute that's more human-friendly e.g. Data Origin
  displayName: string;
  // A description of the search attribute
  desc: string;
  // An example of the search pattern for the field
  example: string;
  // forced selection for facets, this should be send to the api regardless the user selection
  forcedFacetValue?: Array<string>;
  // Default may be selected on initial search
  facetDefaultValue?: Array<string>;
  // browse may require to filter by some facets on some cases
  browseFiltersValue?: Array<string>;
  // minimum query length to fetch suggestions for backend
  minAutocompleteFetchLength?: number;
  // Optional text that is shown over hovering of the element, to provide more meaning and context
  hoverText?: string;
  // Component that can serve different purposes when default rendering options are not enough
  // attributes is an optional object used to provide additional rendering information about the component
  component?: ICustomSearchResultPropertyComponent;
  // Optional dynamic component(s) that are supplied as a way to flexibly control what and how components work
  // An implementation of this interface would contain custom `DynamicComponent` names and custom options on a per component basis
  headerComponent?: IDynamicComponent;
}
