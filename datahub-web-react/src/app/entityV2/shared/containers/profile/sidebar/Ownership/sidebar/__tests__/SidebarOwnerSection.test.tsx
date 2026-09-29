import { render, screen } from '@testing-library/react';
import React from 'react';

import { SidebarOwnerSection } from '@app/entityV2/shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';

// Shared mutable state so each test can set the owners returned by the mocked entity context.
const mockState = vi.hoisted(() => ({ owners: [] as any[] }));

vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityData: () => ({
        entityType: 'DATASET',
        entityData: { ownership: { owners: mockState.owners }, privileges: {} },
    }),
    useMutationUrn: () => 'urn:li:dataset:test',
    useRefetch: () => vi.fn(),
}));

// All owners land under one ownership type so we exercise the per-type dedup in a single group.
vi.mock('@app/entityV2/shared/containers/profile/sidebar/Ownership/ownershipUtils', () => ({
    getOwnershipTypeName: () => 'Technical Owner',
}));

vi.mock('@app/entityV2/shared/containers/profile/sidebar/SidebarSection', () => ({
    SidebarSection: ({ content }: any) => <div>{content}</div>,
}));

// Stub the type section so we can read exactly which owners (deduped) it was handed.
vi.mock('@app/entityV2/shared/containers/profile/sidebar/Ownership/sidebar/OwnershipTypeSection', () => ({
    OwnershipTypeSection: ({ owners }: any) => (
        <div>
            {owners.map((o: any) => (
                <span
                    key={o.owner.urn}
                    data-testid="owner"
                    data-urn={o.owner.urn}
                    data-propagated={String(
                        !!o.attribution?.sourceDetail?.some((d: any) => d.key === 'propagated' && d.value === 'true'),
                    )}
                />
            ))}
        </div>
    ),
}));

vi.mock('@app/entityV2/shared/containers/profile/sidebar/SectionActionButton', () => ({ default: () => null }));
vi.mock('@app/entityV2/shared/containers/profile/sidebar/EmptySectionText', () => ({ default: () => null }));
vi.mock('@app/entityV2/shared/containers/profile/sidebar/Ownership/EditOwnersModal', () => ({
    EditOwnersModal: () => null,
}));

const PROPAGATED = { attribution: { sourceDetail: [{ key: 'propagated', value: 'true' }] } };
const owner = (urn: string, propagated = false) => ({
    owner: { urn },
    ownershipType: { urn: 'urn:li:ownershipType:technical' },
    ...(propagated && PROPAGATED),
});

const renderedOwnerUrns = () => screen.getAllByTestId('owner').map((el) => el.getAttribute('data-urn'));

describe('SidebarOwnerSection deduplication', () => {
    it('renders an owner urn only once when it appears more than once', () => {
        mockState.owners = [owner('urn:li:corpuser:a', true), owner('urn:li:corpuser:a'), owner('urn:li:corpuser:b')];
        render(<SidebarOwnerSection />);
        expect(renderedOwnerUrns()).toEqual(['urn:li:corpuser:a', 'urn:li:corpuser:b']);
    });

    it('prefers the user-applied owner over a propagated duplicate', () => {
        mockState.owners = [owner('urn:li:corpuser:a', true), owner('urn:li:corpuser:a')];
        render(<SidebarOwnerSection />);
        const owners = screen.getAllByTestId('owner');
        expect(owners).toHaveLength(1);
        expect(owners[0]).toHaveAttribute('data-propagated', 'false');
    });

    it('still displays a propagated owner that has no duplicate', () => {
        mockState.owners = [owner('urn:li:corpuser:a', true)];
        render(<SidebarOwnerSection />);
        const owners = screen.getAllByTestId('owner');
        expect(owners).toHaveLength(1);
        expect(owners[0]).toHaveAttribute('data-propagated', 'true');
    });
});
