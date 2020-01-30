type ICustomSearchResultPropertyComponent = any; // eslint-disable-line @typescript-eslint/no-explicit-any

export interface IEntityRenderCommonPropsSearch {
  // Lists a set of render properties that indicate how search attributes for an entity are rendered in the ui
  attributes: Array<ISearchEntityRenderProps>;
  // Should show facets in search, by default is true
  showFacets?: boolean;
  // Lists the optional search result item secondary action component string references,
  // actions are rendered in the order they are presented, this may evolve to include options for rendering such as properties e.t.c
  secondaryActionComponents?: Array<ICustomSearchResultPropertyComponent>;
  // Lists the optional search result item footer component string references
  // Footer components are rendered in the order they are presented, this may evolve to include
  // options for rendering
  customFooterComponents?: Array<ICustomSearchResultPropertyComponent>;
  searchResultEntityFields?: {
    // Field that returns the name of the entity, by default is 'name'
    name?: string;
    // Field that returns the description of the entity, by default is 'description'
    description?: string;
    // Field that returns the entity picture of the entity, it won't be shown by default
    pictureUrl?: string;
  };
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
  // API use a specific name to refer to an entity. For example, 'features' in api is 'feature'.
  apiName: string;
  // By default autocomplete field is `name`, but some entities maybe need some other field
  autocompleteNameField?: string;
  // Flag indicating that this entity should be searchable, by default all entities are searchable so this attribute is optional
  isEnabled?: boolean;
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
  // minimun query length to fetch suggestions for backend
  minAutocompleteFetchLength?: number;
  // Optional text that is shown over hovering of the element, to provide more meaning and context
  hoverText?: string;
  // Component that can serve different purposes when default rendering options are not enough
  // attributes is an optional object used to provide additional rendering information about the component
  component?: ICustomSearchResultPropertyComponent;
}
