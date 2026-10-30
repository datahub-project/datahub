import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useResolveEntityNames } from '@app/entityV2/shared/tabs/Dataset/Schema/history/useResolveEntityNames';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetEntityDisplayNamesQuery } from '@graphql/timeline.generated';
import { ChangeTransaction, EntityType } from '@types';

vi.mock('@app/useEntityRegistry');
vi.mock('@graphql/timeline.generated');

const mockUseGetEntityDisplayNamesQuery = vi.mocked(useGetEntityDisplayNamesQuery);

const SERVICE_ACCOUNT_URN = 'urn:li:corpuser:service_00000000-0000-4000-8000-000000000001';
const DOMAIN_URN = 'urn:li:domain:engineering';

function makeTransactions(): ChangeTransaction[] {
    return [
        {
            timestampMillis: 1,
            actor: SERVICE_ACCOUNT_URN,
            changes: [
                {
                    category: 'DOMAIN',
                    operation: 'ADD',
                    modifier: DOMAIN_URN,
                    parameters: [],
                },
            ],
        } as unknown as ChangeTransaction,
    ];
}

describe('useResolveEntityNames', () => {
    beforeEach(() => {
        vi.resetAllMocks();
        (useEntityRegistry as any).mockReturnValue({
            getDisplayName: (type: EntityType, entity: any) => {
                if (type === EntityType.CorpUser) {
                    return entity.editableProperties?.displayName || entity.username || entity.urn;
                }
                if (type === EntityType.Domain) {
                    return entity.properties?.name || entity.urn;
                }
                return entity.urn;
            },
        });
    });

    it('includes the transaction actor urn alongside referenced entity urns in the batch-resolve query', () => {
        mockUseGetEntityDisplayNamesQuery.mockReturnValue({ data: undefined, error: undefined } as any);

        renderHook(() => useResolveEntityNames(makeTransactions()));

        expect(mockUseGetEntityDisplayNamesQuery).toHaveBeenCalledWith(
            expect.objectContaining({
                variables: { urns: expect.arrayContaining([SERVICE_ACCOUNT_URN, DOMAIN_URN]) },
            }),
        );
    });

    it("resolves a service account's configured display name instead of its raw username", () => {
        mockUseGetEntityDisplayNamesQuery.mockReturnValue({
            data: {
                entities: [
                    {
                        urn: SERVICE_ACCOUNT_URN,
                        type: EntityType.CorpUser,
                        username: 'service_00000000-0000-4000-8000-000000000001',
                        editableProperties: { displayName: 'example_ingestion_service' },
                    },
                    {
                        urn: DOMAIN_URN,
                        type: EntityType.Domain,
                        properties: { name: 'Engineering' },
                    },
                ],
            },
            error: undefined,
        } as any);

        const { result } = renderHook(() => useResolveEntityNames(makeTransactions()));

        expect(result.current.get(SERVICE_ACCOUNT_URN)).toBe('example_ingestion_service');
        expect(result.current.get(DOMAIN_URN)).toBe('Engineering');
    });

    it('does not resolve a service account with no configured display name (username falls through, matching pre-fix behavior)', () => {
        mockUseGetEntityDisplayNamesQuery.mockReturnValue({
            data: {
                entities: [
                    {
                        urn: SERVICE_ACCOUNT_URN,
                        type: EntityType.CorpUser,
                        username: 'service_00000000-0000-4000-8000-000000000001',
                        editableProperties: null,
                    },
                ],
            },
            error: undefined,
        } as any);

        const { result } = renderHook(() => useResolveEntityNames(makeTransactions()));

        // getDisplayName falls back to username, which is the same string ChangeTransactionView's
        // raw-URN-segment fallback already produced — nameMap intentionally omits entries whose
        // resolved name equals the urn, but a username-only fallback is a real, non-urn name, so
        // it's still surfaced (and happens to match the old behavior byte-for-byte).
        expect(result.current.get(SERVICE_ACCOUNT_URN)).toBe('service_00000000-0000-4000-8000-000000000001');
    });

    it('skips the query and returns an empty map when there are no urns to resolve', () => {
        mockUseGetEntityDisplayNamesQuery.mockReturnValue({ data: undefined, error: undefined } as any);

        const { result } = renderHook(() => useResolveEntityNames([]));

        expect(mockUseGetEntityDisplayNamesQuery).toHaveBeenCalledWith(expect.objectContaining({ skip: true }));
        expect(result.current.size).toBe(0);
    });
});
