import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';

import { CompactView } from '@app/previewV2/CompactView';

import { EntityType } from '@types';

// Heavier children (ContextPath, ViewInPlatform) pull in EntityRegistryContext / external-link
// hooks unrelated to this behavior, so they're stubbed out rather than given real providers.
vi.mock('@app/previewV2/EntityHeader', () => ({
    default: () => null,
}));
vi.mock('@app/entityV2/shared/externalUrl/ViewInPlatform', () => ({
    default: () => null,
}));
vi.mock('@app/previewV2/ContextPath', () => ({
    default: () => null,
    // LineageCard styles this named export at module init. CompactView loads the entity
    // registry, which reaches that card, so the stub has to export it too.
    GenericPropertiesContextPath: () => null,
}));

const baseProps = {
    name: 'events',
    urn: 'urn:li:glossaryTerm:my_term',
    isIconPresent: false,
    url: '/glossaryTerm/urn',
    previewData: null,
};

// The remove-from-list button (e.g. "Remove Relationship" on a term's Related Terms tab) used to
// render only in the full card view, so it was unreachable in compact view.
describe('CompactView remove-from-list button', () => {
    it('renders the remove button and calls onRemove when clicked', () => {
        const onRemove = vi.fn();
        render(
            <CompactView
                {...baseProps}
                entityType={EntityType.GlossaryTerm}
                data={null}
                removeButtonText="Remove Relationship"
                onRemove={onRemove}
            />,
        );
        fireEvent.click(screen.getByText('Remove Relationship'));
        expect(onRemove).toHaveBeenCalledTimes(1);
    });

    it('does not render the remove button without removeButtonText', () => {
        render(<CompactView {...baseProps} entityType={EntityType.GlossaryTerm} data={null} />);
        expect(screen.queryByText('Remove Relationship')).toBeNull();
    });
});
