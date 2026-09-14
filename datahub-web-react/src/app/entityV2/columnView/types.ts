import {
    DataHubColumnViewColumnInput,
    DataHubColumnViewExpand,
    DataHubColumnViewLabelStyle,
    DataHubColumnViewOverflow,
    DataHubColumnViewTarget,
    DataHubViewFilter,
    DataHubViewType,
    SortOrder,
} from '@types';

/** Target this UI edits; the only one that exists today. */
export const SCHEMA_TARGET = DataHubColumnViewTarget.DatasetSchemaFields;

/**
 * Presentation hints for a column. Superset of the GraphQL output (`DataHubColumnViewColumnDisplay`)
 * and input (`DataHubColumnViewColumnDisplayInput`) shapes; never part of column identity.
 */
export interface ColumnDisplayLike {
    width?: number | null;
    labelStyle?: DataHubColumnViewLabelStyle | null;
    overflow?: DataHubColumnViewOverflow | null;
    expand?: DataHubColumnViewExpand | null;
    maxItems?: number | null;
    /** Free-form renderer hints; `renderer` selects a registered cell renderer. Matches StringMapEntry(Input). */
    custom?: { key: string; value?: string | null }[] | null;
}

/** `display.custom` key that selects a renderer from entityV2/columnView/renderers/registry. */
export const RENDERER_CUSTOM_KEY = 'renderer';

/** Width presets offered by the column gear menu. */
export const WIDTH_PRESETS = { S: 120, M: 200, L: 320 } as const;

export const DEFAULT_LIST_COLUMN_VIEWS_PAGE_SIZE = 20;

export interface ColumnViewBuilderState {
    viewType?: DataHubViewType;
    name?: string;
    description?: string | null;
    target?: DataHubColumnViewTarget;
    definition?: {
        columns: DataHubColumnViewColumnInput[];
        sort?: { column: DataHubColumnViewColumnInput; order: SortOrder } | null;
        filter?: DataHubViewFilter | null;
    };
}

export const DEFAULT_COLUMN_VIEW_BUILDER_STATE: ColumnViewBuilderState = {
    viewType: DataHubViewType.Personal,
    name: '',
    description: null,
    target: SCHEMA_TARGET,
    definition: {
        columns: [],
        sort: null,
        filter: null,
    },
};
