import { ReactElement } from 'react';

export enum TabRenderType {
    /**
     * A default, full screen tab.
     */
    DEFAULT,
    /**
     * A compact tab
     */
    COMPACT,
}

export enum TabContextType {
    /**
     * A tab rendered horizontally in the main content.
     */
    PROFILE,
    /**
     * A tab rendered in the main profile sidebar.
     */
    PROFILE_SIDEBAR,
    /**
     * A tab rendered in the form sidebar.
     */
    FORM_SIDEBAR,
    /**
     * A tab rendered in the lineage sidebar
     */
    LINEAGE_SIDEBAR,
    /**
     * A tab rendered in the chrome extension sidebar
     */
    CHROME_SIDEBAR,
    /**
     * A tab rendered in the search sidebar
     */
    SEARCH_SIDEBAR,
}

export type EntityTabProps = {
    /**
     * The render type for the tab, e.g. whether it's full screen / horizontal or compact / vertical
     */
    renderType: TabRenderType;
    /**
     * The context type, detailing the scenario in which the tab is being rendered.
     */
    contextType: TabContextType;
    /**
     * Atr that can be provided from the outside.
     */
    properties?: any;
};

export type EntityTab = {
    name: string;
    component: React.FunctionComponent<EntityTabProps>;
    icon?: React.FunctionComponent<any>;
    display?: {
        visible: (GenericEntityProperties, T) => boolean; // Whether the tab is visible on the UI. Defaults to true.
        enabled: (GenericEntityProperties, T) => boolean; // Whether the tab is enabled on the UI. Defaults to true.
    };
    properties?: any;
    id?: string;
    getDynamicName?: (GenericEntityProperties, T, loading: boolean) => ReactElement;
};

export type EntitySidebarTab = {
    name: string;
    component: React.FunctionComponent<EntityTabProps>;
    icon: React.FunctionComponent<any>;
    display?: {
        visible: (GenericEntityProperties, T) => boolean; // Whether the tab is visible on the UI. Defaults to true.
        enabled: (GenericEntityProperties, T) => boolean; // Whether the tab is enabled on the UI. Defaults to true.
    };
    description?: string; // Used to power tooltip if present.
    properties?: any;
    id?: string;
};

export type EntitySidebarSection = {
    component: React.FunctionComponent<{
        properties?: any;
        readOnly?: boolean;
        renderType?: TabRenderType;
        contexType?: TabContextType;
    }>;
    display?: {
        visible: (GenericEntityProperties, T, contextType?: TabContextType | undefined) => boolean; // Whether the sidebar is visible on the UI. Defaults to true.
    };
    properties?: any;
};
