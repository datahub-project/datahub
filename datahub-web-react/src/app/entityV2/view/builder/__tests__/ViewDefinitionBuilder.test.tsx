import { act, fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import { ViewDefinitionBuilder } from '@app/entityV2/view/builder/ViewDefinitionBuilder';
import { ViewBuilderMode } from '@app/entityV2/view/builder/types';
import { ViewBuilderState } from '@app/entityV2/view/types';
import { LogicalPredicate } from '@app/sharedV2/queryBuilder/builder/types';
import themeV2 from '@conf/theme/themeV2';

import { EntityType, FacetFilter, FilterOperator, LogicalOperator } from '@types';

// Capture what the builder hands to the (heavy) child tabs and let the test drive
// their onChange, so we exercise ViewDefinitionBuilder's own wiring without
// rendering the real filter UI or asset search.
const captured = vi.hoisted(() => ({
    filters: undefined as LogicalPredicate | undefined,
    onChangeFilters: undefined as ((p?: LogicalPredicate) => void) | undefined,
    onChangeSelectedUrns: undefined as ((urns: string[]) => void) | undefined,
}));

vi.mock('@app/sharedV2/queryBuilder/LogicalFiltersBuilder', () => ({
    default: (props: { filters?: LogicalPredicate; onChangeFilters?: (p?: LogicalPredicate) => void }) => {
        captured.filters = props.filters;
        captured.onChangeFilters = props.onChangeFilters;
        return null;
    },
}));

vi.mock('@app/entityV2/view/builder/SelectFilterValuesTab', () => ({
    SelectFilterValuesTab: (props: { onChangeSelectedUrns?: (urns: string[]) => void }) => {
        captured.onChangeSelectedUrns = props.onChangeSelectedUrns;
        return null;
    },
}));

// The property list is fetched via GraphQL (structured properties) and is not
// what this test exercises — serve the static list so no Apollo mock is needed.
vi.mock('@app/entityV2/view/builder/useViewBuilderProperties', async () => {
    const actual = await vi.importActual<typeof import('@app/entityV2/view/builder/viewBuilderProperties')>(
        '@app/entityV2/view/builder/viewBuilderProperties',
    );
    return { useViewBuilderProperties: () => actual.viewBuilderProperties };
});

vi.mock('react-i18next', () => ({
    useTranslation: () => ({ t: (key: string) => key }),
}));

const SCOPE = [EntityType.Dataset, EntityType.Dashboard, EntityType.Container];

/** A view scoped to several entity types, with one Build Filters condition. */
const SCOPED_VIEW: ViewBuilderState = {
    name: 'Public Data Catalog',
    definition: {
        entityTypes: SCOPE,
        filter: {
            operator: LogicalOperator.Or,
            filters: [
                { field: 'tags', values: ['urn:li:tag:private'], condition: FilterOperator.Equal },
            ] as FacetFilter[],
        },
    },
};

/** A view that pins specific assets *and* carries an entity-type scope, so the
 *  builder opens on Select Assets even though there is scope to preserve. */
const URN_SCOPED_VIEW: ViewBuilderState = {
    name: 'Pinned Assets',
    definition: {
        entityTypes: [EntityType.Dataset],
        filter: {
            operator: LogicalOperator.Or,
            filters: [{ field: 'urn', values: ['urn:li:dataset:(urn:li:dataPlatform:hive,t,PROD)'] }] as FacetFilter[],
        },
    },
};

/** A view that pins assets *and* carries a Build Filters condition. The two tabs
 *  are mutually exclusive: the URN belongs to Select Assets and must stay out of
 *  the Build Filters predicate and of the state that tab emits. */
const MIXED_VIEW: ViewBuilderState = {
    name: 'Pinned Assets And Filters',
    definition: {
        entityTypes: [EntityType.Dataset],
        filter: {
            operator: LogicalOperator.And,
            filters: [
                { field: 'urn', values: ['urn:li:dataset:(urn:li:dataPlatform:hive,t,PROD)'] },
                { field: 'tags', values: ['urn:li:tag:private'], condition: FilterOperator.Equal },
            ] as FacetFilter[],
        },
    },
};

const renderBuilder = (state: ViewBuilderState, updateState = vi.fn()) => {
    render(
        <ThemeProvider theme={themeV2}>
            <ViewDefinitionBuilder mode={ViewBuilderMode.EDITOR} state={state} updateState={updateState} />
        </ThemeProvider>,
    );
    return updateState;
};

const lastDefinition = (updateState: ReturnType<typeof vi.fn>) =>
    (updateState.mock.calls.at(-1)?.[0] as ViewBuilderState).definition;

describe('ViewDefinitionBuilder entity-type scope', () => {
    it('keeps the scope in the state emitted when the filters are edited', () => {
        const updateState = renderBuilder(SCOPED_VIEW);

        // Simulate the user editing the filters (echo the seeded predicate back).
        act(() => captured.onChangeFilters?.(captured.filters));

        const definition = lastDefinition(updateState);
        expect(definition?.entityTypes).toEqual(SCOPE);
        expect(definition?.filter?.filters).toHaveLength(1);
        expect(definition?.filter?.filters?.[0]).toMatchObject({ field: 'tags' });
    });

    it('keeps the scope when switching to the Select Assets tab', () => {
        const updateState = renderBuilder(SCOPED_VIEW);

        fireEvent.click(screen.getByText('viewDefinition.tabSelectAssets'));

        expect(lastDefinition(updateState)?.entityTypes).toEqual(SCOPE);
    });

    it('keeps the scope when the pinned assets change', () => {
        const updateState = renderBuilder(URN_SCOPED_VIEW);

        act(() => captured.onChangeSelectedUrns?.(['urn:li:dataset:(urn:li:dataPlatform:hive,other,PROD)']));

        expect(lastDefinition(updateState)?.entityTypes).toEqual([EntityType.Dataset]);
    });

    it('keeps the scope and the saved filters when switching from Select Assets to Build Filters', () => {
        const updateState = renderBuilder(MIXED_VIEW);

        // The view pins a URN, so it opens on Select Assets.
        fireEvent.click(screen.getByText('viewDefinition.tabBuildFilters'));

        // The URN belongs to the other tab, so it is not seeded here...
        expect((captured.filters as LogicalPredicate).operands).toHaveLength(1);
        expect((captured.filters as LogicalPredicate).operands[0]).toMatchObject({ property: 'tags' });

        // ...and the tab click must not clear the scope or the saved condition.
        const definition = lastDefinition(updateState);
        expect(definition?.entityTypes).toEqual([EntityType.Dataset]);
        expect(definition?.filter?.filters?.map((f) => f.field)).toEqual(['tags']);
    });
});
