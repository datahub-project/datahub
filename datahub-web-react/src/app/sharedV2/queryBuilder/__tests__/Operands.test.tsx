import React, { useState } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import LogicalFiltersBuilder from '@app/sharedV2/queryBuilder/LogicalFiltersBuilder';
import Operands from '@app/sharedV2/queryBuilder/Operands';
import { Property } from '@app/sharedV2/queryBuilder/builder/property/types/properties';
import { ValueTypeId } from '@app/sharedV2/queryBuilder/builder/property/types/values';
import { LogicalOperatorType, LogicalPredicate, PropertyPredicate } from '@app/sharedV2/queryBuilder/builder/types';
import { fireEvent, render, screen } from '@utils/test-utils/customRender';

afterEach(() => {
    vi.restoreAllMocks();
});

const PROPERTIES: Property[] = [
    { id: 'name', displayName: 'Name', valueType: ValueTypeId.STRING },
    { id: 'platform', displayName: 'Platform', valueType: ValueTypeId.URN },
];

const condition = (property: string): PropertyPredicate => ({ type: 'property', property });

const group = (
    operands: (PropertyPredicate | LogicalPredicate)[],
    operator = LogicalOperatorType.AND,
): LogicalPredicate => ({
    type: 'logical',
    operator,
    operands,
});

function Harness({ initial, onChange }: { initial: LogicalPredicate; onChange: (p?: LogicalPredicate) => void }) {
    const [filters, setFilters] = useState<LogicalPredicate>(initial);
    return (
        <LogicalFiltersBuilder
            filters={filters}
            onChangeFilters={(newFilters) => {
                onChange(newFilters);
                if (newFilters) setFilters(newFilters);
            }}
            properties={PROPERTIES}
        />
    );
}

// Header controls render in document order: the root group first, then each nested group.
const expandButtons = () => Array.from(document.querySelectorAll('button[aria-expanded]'));
const groupDeleteButtons = () => screen.getAllByTestId('query-builder-delete-button');
// Only the selected operation button gets a background (see OperationButton in styledComponents.ts).
const UNSELECTED_BACKGROUND = 'rgba(0, 0, 0, 0)';
const selectedOperation = (groupIndex: number) => {
    const buttons = {
        [LogicalOperatorType.AND]: screen.getAllByTestId('query-builder-all-button')[groupIndex],
        [LogicalOperatorType.OR]: screen.getAllByTestId('query-builder-any-button')[groupIndex],
        [LogicalOperatorType.NOT]: screen.getAllByTestId('query-builder-none-button')[groupIndex],
    };
    const selected = Object.entries(buttons).filter(
        ([, button]) => getComputedStyle(button).backgroundColor !== UNSELECTED_BACKGROUND,
    );
    return selected.map(([operator]) => operator);
};

describe('Operands', () => {
    it('renders several conditions and groups without a missing or duplicate "key" warning', () => {
        const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});

        render(
            <Operands
                operands={[condition('name'), group([condition('platform')]), condition('platform'), group([])]}
                onChangeOperands={() => {}}
                onDeletePredicate={() => {}}
                properties={PROPERTIES}
                depth={0}
            />,
        );

        // 2 top-level conditions plus 1 inside the first group.
        expect(screen.getAllByTestId('query-builder-delete-condition-button')).toHaveLength(3);

        const keyWarnings = consoleError.mock.calls.filter((args) =>
            args.some(
                (arg) =>
                    typeof arg === 'string' &&
                    (arg.includes('unique "key" prop') || arg.includes('two children with the same key')),
            ),
        );
        expect(keyWarnings).toEqual([]);
    });

    it("keeps each remaining group's expanded state and operation when an earlier group is deleted", () => {
        const onChange = vi.fn();
        render(
            <Harness
                initial={group([
                    group([condition('name')], LogicalOperatorType.OR),
                    group([condition('platform')], LogicalOperatorType.AND),
                    group([condition('name')], LogicalOperatorType.NOT),
                ])}
                onChange={onChange}
            />,
        );

        // Collapse the second nested group (index 0 is the root group).
        fireEvent.click(expandButtons()[2]);
        expect(expandButtons().map((b) => b.getAttribute('aria-expanded'))).toEqual(['true', 'true', 'false', 'true']);
        expect([1, 2, 3].map(selectedOperation)).toEqual([['or'], ['and'], ['not']]);

        fireEvent.click(groupDeleteButtons()[1]);

        expect(expandButtons().map((b) => b.getAttribute('aria-expanded'))).toEqual(['true', 'false', 'true']);
        expect([1, 2].map(selectedOperation)).toEqual([['and'], ['not']]);
        expect(onChange).toHaveBeenLastCalledWith(
            group([
                group([condition('platform')], LogicalOperatorType.AND),
                group([condition('name')], LogicalOperatorType.NOT),
            ]),
        );
    });

    it('keeps the remaining conditions mounted when an earlier condition is deleted', () => {
        render(
            <Harness
                initial={group([condition('name'), condition('platform'), condition('name')])}
                onChange={vi.fn()}
            />,
        );
        const [, second, third] = screen.getAllByTestId('query-builder-delete-condition-button');

        fireEvent.click(screen.getAllByTestId('query-builder-delete-condition-button')[0]);

        // The same DOM nodes survive, so each Condition keeps its own component state.
        const remaining = screen.getAllByTestId('query-builder-delete-condition-button');
        expect(remaining).toHaveLength(2);
        expect(remaining[0]).toBe(second);
        expect(remaining[1]).toBe(third);
    });

    it('emits exactly the same predicate JSON as before, with no key or id fields', () => {
        const onChange = vi.fn();
        render(
            <Harness
                initial={group([
                    condition('name'),
                    group([condition('platform'), condition('name')], LogicalOperatorType.OR),
                    group([condition('platform')]),
                ])}
                onChange={onChange}
            />,
        );

        // Delete the first group, change the remaining group's operation, add a condition and a group to the
        // root, then delete the root's first condition ('name': a group's conditions render after the root's).
        fireEvent.click(groupDeleteButtons()[1]);
        fireEvent.click(screen.getAllByTestId('query-builder-none-button')[1]);
        fireEvent.click(screen.getAllByTestId('query-builder-add-condition-button')[0]);
        fireEvent.click(screen.getAllByTestId('query-builder-add-group-button')[0]);
        fireEvent.click(screen.getAllByTestId('query-builder-delete-condition-button')[0]);

        expect(JSON.stringify(onChange.mock.lastCall?.[0])).toBe(
            '{"type":"logical","operator":"and","operands":[' +
                '{"type":"logical","operator":"not","operands":[{"type":"property","property":"platform"}]},' +
                '{"type":"property"},' +
                '{"type":"logical","operator":"and","operands":[]}]}',
        );
    });
});
