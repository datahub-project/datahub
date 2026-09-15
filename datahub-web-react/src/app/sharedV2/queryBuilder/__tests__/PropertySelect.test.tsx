import React, { useState } from 'react';
import { beforeAll, describe, expect, it, vi } from 'vitest';

import PropertySelect from '@app/sharedV2/queryBuilder/PropertySelect';
import { STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID } from '@app/sharedV2/queryBuilder/builder/property/constants';
import { Property } from '@app/sharedV2/queryBuilder/builder/property/types/properties';
import { ValueTypeId } from '@app/sharedV2/queryBuilder/builder/property/types/values';
import { fireEvent, render, screen, within } from '@utils/test-utils/customRender';

// SimpleSelect only mounts its trigger once visible, so force IntersectionObserver to report visible.
beforeAll(() => {
    vi.stubGlobal(
        'IntersectionObserver',
        class {
            private readonly callback: IntersectionObserverCallback;

            constructor(callback: IntersectionObserverCallback) {
                this.callback = callback;
            }

            observe(target: Element) {
                this.callback(
                    [{ isIntersecting: true, target } as IntersectionObserverEntry],
                    this as unknown as IntersectionObserver,
                );
            }

            unobserve() {}

            disconnect() {}

            takeRecords() {
                return [];
            }
        },
    );
});

const TIER_ID = 'structuredProperties.io.acryl.tier';
const RETENTION_ID = 'structuredProperties.io.acryl.retentionDays';

const PROPERTIES: Property[] = [
    { id: 'platform', displayName: 'Platform', valueType: ValueTypeId.URN },
    {
        id: STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID,
        displayName: 'Structured Property',
        children: [
            { id: TIER_ID, displayName: 'Tier', valueType: ValueTypeId.STRING },
            { id: RETENTION_ID, displayName: 'Retention Days', valueType: ValueTypeId.NUMBER },
        ],
    },
];

function Harness({ initial, onChange }: { initial?: string; onChange?: (id?: string) => void }) {
    const [selected, setSelected] = useState<string | undefined>(initial);
    return (
        <PropertySelect
            selectedProperty={selected}
            properties={PROPERTIES}
            onChangeProperty={(id) => {
                setSelected(id);
                onChange?.(id);
            }}
        />
    );
}

function openSelect(testId: string) {
    fireEvent.click(screen.getByTestId(`${testId}-base`));
}

describe('PropertySelect', () => {
    it('reveals the child picker after choosing a group, without committing a leaf yet', () => {
        const onChange = vi.fn();
        render(<Harness onChange={onChange} />);

        expect(screen.queryByTestId('condition-select-child')).not.toBeInTheDocument();

        openSelect('condition-select');
        fireEvent.click(screen.getByTestId(`option-${STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID}`));

        expect(screen.getByTestId('condition-select-child')).toBeInTheDocument();
        expect(onChange).not.toHaveBeenCalled();
    });

    it('commits the property once a child is chosen', () => {
        const onChange = vi.fn();
        render(<Harness onChange={onChange} />);

        openSelect('condition-select');
        fireEvent.click(screen.getByTestId(`option-${STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID}`));
        openSelect('condition-select-child');
        fireEvent.click(screen.getByTestId(`option-${TIER_ID}`));

        expect(onChange).toHaveBeenLastCalledWith(TIER_ID);
    });

    it('clears the previously selected leaf when switching from a top-level property to a group', () => {
        const onChange = vi.fn();
        render(<Harness initial="platform" onChange={onChange} />);

        expect(screen.queryByTestId('condition-select-child')).not.toBeInTheDocument();

        openSelect('condition-select');
        fireEvent.click(screen.getByTestId(`option-${STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID}`));

        expect(onChange).toHaveBeenCalledWith(undefined);
        expect(screen.getByTestId('condition-select-child')).toBeInTheDocument();
    });

    it('restores both levels when editing an existing grouped filter', () => {
        render(<Harness initial={RETENTION_ID} />);

        expect(
            within(screen.getByTestId('condition-select-base')).getByText('Structured Property'),
        ).toBeInTheDocument();
        expect(
            within(screen.getByTestId('condition-select-child-base')).getByText('Retention Days'),
        ).toBeInTheDocument();
    });

    it('drops a transient open group when the row is rebound to a concrete top-level property', () => {
        function Rebindable() {
            const [selected, setSelected] = useState<string | undefined>(undefined);
            return (
                <>
                    <button type="button" data-testid="rebind" onClick={() => setSelected('platform')}>
                        rebind
                    </button>
                    <PropertySelect selectedProperty={selected} properties={PROPERTIES} onChangeProperty={() => {}} />
                </>
            );
        }
        render(<Rebindable />);

        openSelect('condition-select');
        fireEvent.click(screen.getByTestId(`option-${STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID}`));
        expect(screen.getByTestId('condition-select-child')).toBeInTheDocument();

        fireEvent.click(screen.getByTestId('rebind'));

        expect(screen.queryByTestId('condition-select-child')).not.toBeInTheDocument();
    });
});
