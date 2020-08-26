import { AppRoute } from '@datahub/data-models/types/entity/shared';
import {
  ISearchEntityRenderProps,
  IEntityRenderPropsSearch,
  IEntityRenderCommonPropsSearch
} from '@datahub/data-models/types/search/search-entity-render-prop';
import { DataModelEntityInstance } from '@datahub/data-models/constants/entity';
import { IEntityPageMainComponent, PageComponent } from '@datahub/data-models/types/entity/rendering/page-components';
import { BaseEntity } from '@datahub/data-models/entity/base-entity';
import { IDynamicComponent } from '@datahub/shared/types/dynamic-component';

/**
 * Tab properties for entity profile pages
 * @export
 * @interface ITabProperties
 */
export interface ITabProperties {
  // A unique identifier for the tab so that we can tell what tab navigation piece is connected to
  // what content piece inside ivy tabs
  id: string;
  // The rendered label title for the tab, will show up in the tab navigation menu
  title: string;
  // The component that is rendered in the content section for the tab, in charge of taking in top
  // level data and supplying the proper view
  // TODO META-10757: As we move forward with generic entity page, this component needs to accept options
  //  new pages won't support string as contentComponent.
  contentComponent: string | PageComponent | IDynamicComponent;
  // Will only insert the tab element and run its render logic if the tab is currently selected.
  // The tradeoff is more time to load between tab changes but less heavy initial load times
  lazyRender?: boolean;
  // Whenever the tablist menu is more complex than a simple label, we need can choose to specify
  // a component to handle the more sophisticated logic (example use case would be rendering
  // subtabs or expansion/collapsing of menus)
  tablistMenuComponent?: string;
}

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
 * Render descriptions for search results
 */
export interface IEntityRenderPropsEntityPage<E extends DataModelEntityInstance | BaseEntity<{}> = never> {
  // Route for the entity page
  route: AppRoute;
  // The URL path segment to the entity's api endpoint
  apiRouteName: string;
  // Lists the tab properties for the tabs on this Entity. A fn is also accepted to dynamically return tabs.
  tabProperties: Array<ITabProperties> | ((entity: E) => Array<ITabProperties>);
  // Specifies the default tab from the list of tabIds if a specific tab is not provided on navigation
  defaultTab: ITabProperties['id'] | ((entityUrn: string) => ITabProperties['id']);
  // Placeholder rendered when attribute are missing for entity metadata
  attributePlaceholder?: string;
  // References the main component used for the entity page. It will receive entity type, urn and tab as parameters
  pageComponent?: IEntityPageMainComponent | IDynamicComponent;
  // List of components to be dynamically inserted into the entity page header at runtime
  customHeaderComponents?: Array<IDynamicComponent>;
}

/**
 * Browse definitions of an entity
 */
export interface IEntityRenderPropsBrowse {
  // Flag indicating if the `SearchWithinHierarchy` component should be rendered on the browse page
  // This performs an advanced search query with the category fields populated in the query string
  showHierarchySearch: boolean;
  // List of field metadata that browse may use. For example, it can send specific params to API to
  // get a custom browse experience.
  attributes?: Array<ISearchEntityRenderProps>;
}

/**
 * `Lists` property definitions
 */
export interface IEntityRenderPropsList {
  searchResultConfig: IEntityRenderCommonPropsSearch;
}

/**
 * Defines a set of properties that guide the rendering of ui elements and features in the host application
 * @interface IEntityRenderProps
 */
export interface IEntityRenderProps {
  // References to the Entity in RestliUtils
  // file://../../../datahub-dao/src/main/java/com/linkedin/datahub/util/RestliUtils.java
  apiEntityName: string;
  search: IEntityRenderPropsSearch;
  userEntityOwnership?: IEntityRenderCommonPropsSearch;
  browse?: IEntityRenderPropsBrowse;
  // Some entities need to show custom visuals on top of browse, these entities
  // can add those custom components here
  browseHeaderComponents?: Array<IDynamicComponent>;
  // Specifies properties for rendering the 'profile' page of an entity, aka Entity page
  // if there is not an entity page, this should be undefined
  entityPage?: IEntityRenderPropsEntityPage;
  // Specifies attributes for displaying Entity properties in a list component
  list?: IEntityRenderPropsList;
}
