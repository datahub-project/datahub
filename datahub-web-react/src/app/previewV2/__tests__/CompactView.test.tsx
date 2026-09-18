import { render } from '@testing-library/react';
import React from 'react';

import { CompactView } from '@app/previewV2/CompactView';

import { Entity, EntityType, FabricType } from '@types';

// CompactView is the default (non-full-view, non-hover-card) search-result row and the other
// EntityHeader caller besides DefaultPreviewCard. The pill's show/hide gate is already covered by
// EntityHeader.test.tsx; this test only guards that CompactView actually threads `environment`
// into EntityHeader — a regression that already slipped through once (dropped silently by tsc and
// eslint, since `environment` is an optional prop). Heavier children (ContextPath, ViewInPlatform)
// pull in EntityRegistryContext / external-link hooks unrelated to this wiring, so they're stubbed
// out rather than given real providers.
const entityHeaderMock = vi.fn((_props: unknown) => null);
vi.mock('@app/previewV2/EntityHeader', () => ({
    default: (props: unknown) => entityHeaderMock(props),
}));
vi.mock('@app/entityV2/shared/externalUrl/ViewInPlatform', () => ({
    default: () => null,
}));
vi.mock('@app/previewV2/ContextPath', () => ({
    default: () => null,
}));

const baseProps = {
    name: 'events',
    urn: 'urn:li:dataset:(urn:li:dataPlatform:mysql,my_db.my_schema.events,PROD)',
    isIconPresent: false,
    url: '/dataset/urn',
    previewData: null,
};

function renderCompactView(entityType: EntityType, data: unknown) {
    return render(<CompactView {...baseProps} entityType={entityType} data={data as Entity as unknown as null} />);
}

describe('CompactView environment threading', () => {
    beforeEach(() => {
        entityHeaderMock.mockClear();
    });

    it('passes a resolved dataset origin through to EntityHeader', () => {
        renderCompactView(EntityType.Dataset, { type: EntityType.Dataset, origin: FabricType.Prod });
        expect(entityHeaderMock).toHaveBeenCalledWith(expect.objectContaining({ environment: FabricType.Prod }));
    });

    it('passes a resolved container env through to EntityHeader', () => {
        renderCompactView(EntityType.Container, {
            type: EntityType.Container,
            properties: { env: FabricType.Dev },
        });
        expect(entityHeaderMock).toHaveBeenCalledWith(expect.objectContaining({ environment: FabricType.Dev }));
    });

    it('passes null when the entity type has no environment', () => {
        renderCompactView(EntityType.Chart, { type: EntityType.Chart });
        expect(entityHeaderMock).toHaveBeenCalledWith(expect.objectContaining({ environment: null }));
    });
});
