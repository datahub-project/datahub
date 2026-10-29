import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useRemoveRelationship } from '@app/previewV2/DefaultPreviewCard';

import { EntityType } from '@types';

const mocks = vi.hoisted(() => ({
    pageEntityType: 'APPLICATION' as string,
    removeApplication: vi.fn(),
}));

vi.mock('react-i18next', async (importOriginal) => ({
    ...(await importOriginal<typeof import('react-i18next')>()),
    useTranslation: () => ({ t: (key: string) => key }),
}));

vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityContext: () => ({ setShouldRefetchEmbeddedListSearch: vi.fn() }),
    useEntityData: () => ({ urn: 'urn:li:application:parent', entityType: mocks.pageEntityType }),
}));

vi.mock('@app/entityV2/shared/SearchCardContext', () => ({
    useSearchCardContext: () => ({ showRemovalFromList: true }),
}));

vi.mock('@app/entityV2/shared/PreviewContext', () => ({
    usePreviewData: () => ({ previewData: { urn: 'urn:li:application:child' } }),
}));

vi.mock('@app/previewV2/utils', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@app/previewV2/utils')>()),
    useRemoveDomainAssets: () => ({ removeDomain: vi.fn() }),
    useRemoveGlossaryTermAssets: () => ({ removeTerm: vi.fn() }),
    useRemoveDataProductAssets: () => ({ removeDataProduct: vi.fn() }),
    useRemoveApplicationAssets: () => ({ removeApplication: mocks.removeApplication }),
}));

describe('useRemoveRelationship on an Application page', () => {
    beforeEach(() => {
        mocks.pageEntityType = EntityType.Application;
    });

    it('offers removal for an asset assigned to the Application', () => {
        const { result } = renderHook(() => useRemoveRelationship(EntityType.Dataset));
        expect(result.current.removeButtonText).toBe('removeFromApplication');
    });

    it('hides removal for a child Application, which is linked through parentApplication', () => {
        const { result } = renderHook(() => useRemoveRelationship(EntityType.Application));
        expect(result.current.removeButtonText).toBeNull();
    });
});
