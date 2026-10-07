import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { EntitySearchSelect } from '@app/entityV2/shared/EntitySearchSelect/EntitySearchSelect';
import { render, screen, waitFor } from '@utils/test-utils/customRender';

import { GetEntitiesDocument } from '@graphql/entity.generated';
import { EntityType } from '@types';

// The dropdown fires its own autocomplete queries and is irrelevant to the crash, which happens
// while rendering the selected-value chips in the trigger.
vi.mock('@app/entityV2/shared/EntitySearchSelect/EntitySearchDropdown', () => ({
    EntitySearchDropdown: ({ trigger }: { trigger: React.ReactNode }) => <div>{trigger}</div>,
}));

const mockEntityRegistry = {
    getDisplayName: (_type: EntityType, entity: { urn: string }) => entity.urn,
};

vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: () => mockEntityRegistry,
    useEntityRegistryV2: () => mockEntityRegistry,
}));

// A document that no longer resolves. `entities(urns:)` is typed `[Entity]`, so GMS answers 200
// with a null slot rather than an error when the urn is deleted or not viewable.
const MISSING_DOC_URN = 'urn:li:document:ecomm-metrics';

const NULL_ENTITY_MOCK = {
    request: {
        query: GetEntitiesDocument,
        variables: { urns: [MISSING_DOC_URN], skipSiblingsSearch: false, skipLineage: false },
    },
    result: { data: { entities: [null] } },
};

describe('EntitySearchSelect', () => {
    it('should render the raw urn when getEntities resolves a selected urn to null', async () => {
        render(
            <EntitySearchSelect
                selectedUrns={[MISSING_DOC_URN]}
                entityTypes={[EntityType.Document]}
                isMultiSelect
                onUpdate={vi.fn()}
            />,
            { apolloMocks: [NULL_ENTITY_MOCK] },
        );

        await waitFor(() => {
            expect(screen.getByText(MISSING_DOC_URN)).toBeInTheDocument();
        });
    });
});
