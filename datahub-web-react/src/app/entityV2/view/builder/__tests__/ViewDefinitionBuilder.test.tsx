import { act, fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import { ViewDefinitionBuilder } from '@app/entityV2/view/builder/ViewDefinitionBuilder';
import { ViewBuilderMode } from '@app/entityV2/view/builder/types';
import { ViewBuilderState } from '@app/entityV2/view/types';
import { LogicalOperatorType, LogicalPredicate } from '@app/sharedV2/queryBuilder/builder/types';
import themeV2 from '@conf/theme/themeV2';

import { EntityType, FacetFilter, FilterOperator, LogicalOperator } from '@types';

// Capture what the builder hands to the (heavy) query-builder and let the test
// drive its onChange, so we exercise ViewDefinitionBuilder's own wiring without
// rendering the real filter UI.
const captured = vi.hoisted(() => ({
    filters: undefined as LogicalPredicate | undefined,
    onChange: undefined as ((p?: LogicalPredicate) => void) | undefined,
}));

vi.mock('@app/sharedV2/queryBuilder/LogicalFiltersBuilder', () => ({
    default: (props: { filters?: LogicalPredicate; onChangeFilters?: (p?: LogicalPredicate) => void }) => {
        captured.filters = props.filters;
        captured.onChange = props.onChangeFilters;
        return null;
    },
}));

vi.mock('@app/entityV2/view/builder/SelectFilterValuesTab', () => ({
    SelectFilterValuesTab: () => null,
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

// Mirrors the reported regression: a view scoped to several entity types with a
// single "tag does not equal" filter.
const EXISTING_VIEW: ViewBuilderState = {
    name: 'Public Data Catalog',
    definition: {
        entityTypes: [EntityType.Dataset, EntityType.Dashboard, EntityType.Container],
        filter: {
            operator: LogicalOperator.Or,
            filters: [
                { field: 'tags', values: ['urn:li:tag:private'], condition: FilterOperator.Equal, negated: true },
            ] as FacetFilter[],
        },
    },
};

// A view that pins specific assets *and* carries an entity-type scope, so the
// builder opens on Select Assets even though there is scope to preserve.
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

// A view that pins assets *and* carries a Build Filters condition. The two tabs
// are mutually exclusive: the URN belongs to Select Assets and must stay out of
// the Build Filters predicate and of the state that tab emits.
const MIXED_VIEW: ViewBuilderState = {
    name: 'Pinned Assets And Filters',
    definition: {
        entityTypes: [EntityType.Dataset],
        filter: {
            operator: LogicalOperator.And,
            filters: [
                { field: 'urn', values: ['urn:li:dataset:(urn:li:dataPlatform:hive,t,PROD)'] },
                { field: 'tags', values: ['urn:li:tag:private'], condition: FilterOperator.Equal, negated: true },
            ] as FacetFilter[],
        },
    },
};

describe('ViewDefinitionBuilder wiring', () => {
    it('seeds entity types and per-row negation when opening an existing view', () => {
        render(
            <ThemeProvider theme={themeV2}>
                <ViewDefinitionBuilder mode={ViewBuilderMode.EDITOR} state={EXISTING_VIEW} updateState={vi.fn()} />
            </ThemeProvider>,
        );

        // An OR view's scope is ANDed with the conditions, so the conditions sit
        // in their own OR group rather than beside the scope row.
        const seeded = captured.filters as LogicalPredicate;
        expect(seeded.operator).toBe(LogicalOperatorType.AND);
        expect(seeded.operands).toHaveLength(2);
        expect(seeded.operands[0]).toMatchObject({
            property: '_entityType',
            operator: 'equals',
            values: [EntityType.Dataset, EntityType.Dashboard, EntityType.Container],
        });
        const conditions = seeded.operands[1] as LogicalPredicate;
        expect(conditions.operator).toBe(LogicalOperatorType.OR);
        expect(conditions.operands[0]).toMatchObject({ property: 'tags', operator: 'not_equals' });
    });

    it('preserves entity types and negation in the state emitted on edit', () => {
        const updateState = vi.fn();
        render(
            <ThemeProvider theme={themeV2}>
                <ViewDefinitionBuilder mode={ViewBuilderMode.EDITOR} state={EXISTING_VIEW} updateState={updateState} />
            </ThemeProvider>,
        );

        // Simulate the user editing the filters (echo the seeded predicate back).
        act(() => captured.onChange?.(captured.filters));

        expect(updateState).toHaveBeenCalledTimes(1);
        const emitted = updateState.mock.calls[0][0] as ViewBuilderState;
        expect(emitted.definition?.entityTypes).toEqual([
            EntityType.Dataset,
            EntityType.Dashboard,
            EntityType.Container,
        ]);
        expect(emitted.definition?.filter?.operator).toBe(LogicalOperator.Or);
        expect(emitted.definition?.filter?.filters).toHaveLength(1);
        expect(emitted.definition?.filter?.filters?.[0]).toMatchObject({
            field: 'tags',
            condition: FilterOperator.Equal,
            negated: true,
        });
    });

    it('keeps the entity-type scope when switching from Select Assets to Build Filters', () => {
        const updateState = vi.fn();
        render(
            <ThemeProvider theme={themeV2}>
                <ViewDefinitionBuilder
                    mode={ViewBuilderMode.EDITOR}
                    state={URN_SCOPED_VIEW}
                    updateState={updateState}
                />
            </ThemeProvider>,
        );

        fireEvent.click(screen.getByText('viewDefinition.tabBuildFilters'));

        const emitted = updateState.mock.calls.at(-1)?.[0] as ViewBuilderState;
        expect(emitted.definition?.entityTypes).toEqual([EntityType.Dataset]);
    });

    it('keeps URN filters out of the Build Filters predicate and the state it emits', () => {
        const updateState = vi.fn();
        render(
            <ThemeProvider theme={themeV2}>
                <ViewDefinitionBuilder mode={ViewBuilderMode.EDITOR} state={MIXED_VIEW} updateState={updateState} />
            </ThemeProvider>,
        );

        // The view pins a URN, so it opens on Select Assets.
        fireEvent.click(screen.getByText('viewDefinition.tabBuildFilters'));

        const seeded = captured.filters as LogicalPredicate;
        expect(seeded.operands).toHaveLength(2);
        expect(seeded.operands[0]).toMatchObject({ property: '_entityType', values: [EntityType.Dataset] });
        expect(seeded.operands[1]).toMatchObject({ property: 'tags', operator: 'not_equals' });

        const emitted = updateState.mock.calls.at(-1)?.[0] as ViewBuilderState;
        expect(emitted.definition?.entityTypes).toEqual([EntityType.Dataset]);
        expect(emitted.definition?.filter?.filters?.map((f) => f.field)).toEqual(['tags']);
    });
});
