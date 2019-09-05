/**
 * Defines attributes that determine how search related visual elements are rendered
 * @export
 * @interface ISearchEntityRenderProps
 */
export interface ISearchEntityRenderProps {
  // The name of the search entity attribute that these render properties apply to e.g. dataorigin
  fieldName: string;
  // Flag indicating that this search entity attribute should be available or hidden from search auto complete
  showInAutoCompletion: boolean;
  // Flag indicating that this search entity attribute should be available or hidden from results preview
  showInResultsPreview: boolean;
  // Flag indicating that this search entity attribute should be shown as a search facet property
  showInFacets: boolean;
  // Flag indicating that this search entity attribute should be shown as a search result tag or not
  showAsTag?: boolean;
  // An alternative string representation of the fieldName attribute that's more human-friendly e.g. Data Origin
  displayName: string;
  // A description of the search attribute
  desc: string;
  // An example of the search pattern for the field
  example: string;
  // The compute function is helpful when we want to define how a specific field gets rendered in a template
  // An example is when we have nested objects or array of objects that we need to massage to a standardized
  // way to view
  compute?: (data: unknown) => string;
  // forced selection for facets, this should be send to the api regardless the user selection
  facetDefaultValue?: Array<string>;
  // browse may require to filter by some facets on some cases
  browseFiltersValue?: Array<string>;
  // minimun query length to fetch suggestions for backend
  minAutocompleteFetchLength?: number;
  // Flag indicating if the search entity attribute should be represented as an Icon or not
  showAsIcon?: boolean;
  // A string which serves as an identifier for the icon itself. Generally associated with a className
  iconName?: string;
  // Optional text that is shown over hovering of the element, to provide more meaning and context
  hoverText?: string;
  // Component that can serve different purposes when default rendering options are not enough
  // attributes is an optional object used to provide additional rendering information about the component
  component?: {
    name: string;
    attrs?: Record<string, unknown>;
  };
}
