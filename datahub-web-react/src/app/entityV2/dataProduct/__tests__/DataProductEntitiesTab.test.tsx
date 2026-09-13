import { render } from '@testing-library/react';
import React from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { DataProductEntitiesTab } from '@app/entityV2/dataProduct/DataProductEntitiesTab';
import { EmbeddedListSearchSection } from '@app/entityV2/shared/components/styled/search/EmbeddedListSearchSection';

vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityData: vi.fn(),
}));
vi.mock('react-i18next', () => ({
    useTranslation: () => ({ t: (key: string) => key }),
}));
vi.mock('@app/entityV2/dataProduct/generateUseListDataProductAssets', () => ({
    default: vi.fn(() => vi.fn()),
}));
vi.mock('@app/entityV2/dataProduct/generateUseListDataProductAssetsCount', () => ({
    generateUseListDataProductAssetsCount: vi.fn(() => vi.fn()),
}));
vi.mock('@app/entityV2/shared/components/styled/search/EmbeddedListSearchSection', () => ({
    EmbeddedListSearchSection: vi.fn(() => null),
}));

describe('DataProductEntitiesTab', () => {
    beforeEach(() => {
        (useEntityData as unknown as ReturnType<typeof vi.fn>).mockReturnValue({
            urn: 'urn:li:dataProduct:analytics',
        });
    });

    afterEach(() => {
        vi.resetAllMocks();
    });

    it('uses the data product-scoped download hook for CSV exports', () => {
        render(<DataProductEntitiesTab />);

        expect(EmbeddedListSearchSection).toHaveBeenCalledWith(
            expect.objectContaining({
                useGetDownloadSearchResults: expect.any(Function),
            }),
            expect.any(Object),
        );
    });
});
