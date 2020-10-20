import { INachoTableConfigs } from '@nacho-ui/table/types/nacho-table';
import { KeyNamesWithValueType } from '@datahub/utils/types/base';
import { IDynamicComponent } from '@datahub/shared/types/dynamic-component';
import { IStandardDynamicProperty } from '@datahub/data-models/types/entity/rendering/properties-panel';
import { INachoDropdownOption } from '@nacho-ui/core/types/nacho-dropdown';

// TODO https://jira01.corp.linkedin.com:8443/browse/META-10764: Investigate how to solve the 'wrapper' components scalability issue in generic entity page and search
/**
 * Property to show in the entity page header
 * Value will be fetched from the entity using the propery `name`
 */
export interface IEntityPageHeaderProperty<T = string> extends IStandardDynamicProperty<T> {
  // Dynamic component that this property can use to be rendered as a tag
  tagComponent?: IDynamicComponent;
}

/**
 * Entity Page Header configuration params for an entity
 */
export interface IEntityPageHeader {
  // Properties to show in the entity page header
  properties: Array<IEntityPageHeaderProperty>;
  // Page components at CTA level that will show up in the header
  actionComponents?: Array<IDynamicComponent>;
  // Additional custom components
  customComponents?: Array<IDynamicComponent>;
}

/**
 * Will render an empty state with a message
 */
export interface IEmptyStateComponent {
  // name of the component
  name: 'empty-state';
  options: {
    // Message to show as a header in the empty state
    heading: string;
  };
}

/**
 * Generic component to render the entity page
 */
export interface IEntityPageMainComponent extends IDynamicComponent {
  // name of the component
  name: 'entity-page/entity-page-main';
  options: {
    // header options for the page
    header: IEntityPageHeader;
  };
}

/**
 * Content panel that will wrap a component with title, maybe a subheader, and some other common display info
 */
export interface IContentPanelComponent<E> extends IDynamicComponent {
  // name of the component
  name: 'entity-page/entity-page-content/content-panel';
  options: {
    // Id for the element
    domId?: string;
    // Title to render
    title: string;
    // Component inside
    contentComponent: PageComponent<E> | IDynamicComponent;
  };
}

/**
 * File Viewer Component input content
 */
export interface IFileViewerItem {
  /**
   * type of the content:
   * to add new ones, check modes available here:
   * https://github.com/ajaxorg/ace-builds/tree/master/src-noconflict
   *
   * then add in the options (index.js) for ember ace:
   * ace: {
   *   modes: [...],
   *   workers: [...],
   *   exts: [...]
   * }
   *
   * then add that mode as type
   */
  type: 'json' | 'graphqlschema' | 'text';

  // content of the file
  content: string;
}

/**
 * Component that will use ember-ace to display the content of a file
 */
export interface IFileViewerComponent<E> extends IDynamicComponent {
  // name of the component
  name: 'entity-page/entity-page-content/file-viewer';
  options: {
    // properties name that has IFileViewerItem as type
    propertyName: KeyNamesWithValueType<E, IFileViewerItem>;
    // message to display when no file is available
    emptyStateMessage: string;
  };
}

/**
 * Nacho table wrapper for generic entity page
 */
export interface INachoTableComponent<E> extends IDynamicComponent {
  // name of the component
  name: 'entity-page/entity-page-content/nacho-table';
  options: {
    // optional css class name for this component
    className?: string;
    // properties names to retrieve the array of values to show
    propertyName?: KeyNamesWithValueType<E, Array<unknown> | undefined> | KeyNamesWithValueType<E, Array<unknown>>;
    // message to show when no data is available
    emptyStateMessage: string;
    // table configuration
    tableConfigs: INachoTableConfigs<unknown>;
  };
}

/**
 * Simple aggregation component that will show all contained components
 */
export interface ILinearLayoutComponent<E> extends IDynamicComponent {
  // name of the component
  name: 'entity-page/layouts/linear-layout';
  options: {
    // List of components to render
    components: Array<PageComponent<E>>;
  };
}

/**
 * This component will switch the entity passed down to components to another one
 * using a property of the original entity. This way we can reuse components meant for entity1,
 * on entity2 if there is a relation
 */
export interface IEntitySwitch<E> extends IDynamicComponent {
  name: 'entity-page/entity-page-content/entity-switch';
  options: {
    // property name to fetch the new entity from E
    propertyName: keyof E;
    // Child component to render
    component: IDynamicComponent;
  };
}

/**
 * This component will create an entity given an entity... wait what? The problem is that
 * sometimes entity intances are not completely instatiated (with all props). This class will act as
 * a simple container calling DataModels and instanciating a complete instance.
 */
export interface IHydrateEntity extends IDynamicComponent {
  name: 'entity-page/entity-page-content/hydrate-entity';
  options: {
    // Child component to render
    component: IDynamicComponent;
  };
}

/**
 * See IContentPanelWithToggle
 *
 * For each option in the toggle, the component to be rendered inside is needed
 * plus an optional toolbar component in case the content component needs a toolbar.
 *
 * The comunication between content and toolbar is done through a shared object called 'state'.
 */
export interface IContentPanelWithToggleItem {
  // component to be rendered in the content section of the panel
  contentComponent: IDynamicComponent;
  // toolbar to be rendered next to the toggle
  toolbarComponent?: IDynamicComponent;
}

/**
 * Content panel that will have a toggle to switch between content
 */
export interface IContentPanelWithToggle extends IDynamicComponent {
  // name of the component
  name: 'entity-page/entity-page-content/content-panel-with-toggle';
  options: {
    // title of content
    title: string;
    // optional description for the content
    description?: string;
    // toggle options available
    dropDownItems: Array<INachoDropdownOption<IContentPanelWithToggleItem>>;
  };
}

/**
 * Map between the value of a property in an entity and a dynamic component
 */
export type Discriminator<T> = Partial<Record<Extract<T[keyof T], string>, IDynamicComponent>>;

/**
 * Component to create variants of a page using the value of a property
 */
export interface IEntityDiscriminator<T> extends IDynamicComponent {
  name: 'entity-page/entity-page-content/entity-discriminator';
  options: {
    // Map with platform and desired page params
    discriminator: Discriminator<T>;
    // If no suitable platform is found, default page will be used
    default: IDynamicComponent;
    // Property name where to fetch the value to discriminate
    propertyName: keyof T;
  };
}

/**
 * All Page Components available
 *
 * @template E entity or object where page components can read properties
 */
export type PageComponent<E = unknown> =
  | IFileViewerComponent<E>
  | ILinearLayoutComponent<E>
  | INachoTableComponent<E>
  | IContentPanelComponent<E>
  | IEmptyStateComponent
  | IContentPanelWithToggle
  | IEntityDiscriminator<E>
  | IEntitySwitch<E>
  | IHydrateEntity
  | IDynamicComponent;
