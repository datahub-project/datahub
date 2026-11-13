import { describe, expect, it, vi } from 'vitest';

import { getOwnershipTypeName } from '@app/entityV2/shared/containers/profile/sidebar/Ownership/ownershipUtils';

import { EntityType, OwnershipTypeEntity } from '@types';

// Echo interpolation so we can assert the pluralized vs. verbatim branch deterministically.
vi.mock('i18next', () => ({
    default: {
        t: (key: string, opts?: { name?: string }) => {
            if (key.endsWith('pluralName')) return `${opts?.name}s`;
            if (key.endsWith('otherName')) return 'Other';
            return key;
        },
    },
}));

const makeOwnershipType = (urn: string, name?: string | null): OwnershipTypeEntity =>
    ({
        urn,
        type: EntityType.CustomOwnershipType,
        info: name === undefined ? undefined : { name },
    }) as OwnershipTypeEntity;

describe('getOwnershipTypeName', () => {
    it('shows custom ownership type names verbatim, without pluralization', () => {
        // A French custom type must not gain an English "s" (the bug this fixes).
        expect(getOwnershipTypeName(makeOwnershipType('urn:li:ownershipType:custom', 'Responsable métier'))).toEqual(
            'Responsable métier',
        );
    });

    it('pluralizes the built-in system ownership types', () => {
        expect(
            getOwnershipTypeName(
                makeOwnershipType('urn:li:ownershipType:__system__technical_owner', 'Technical Owner'),
            ),
        ).toEqual('Technical Owners');
    });

    it('falls back to "Other" when there is no name', () => {
        expect(getOwnershipTypeName(makeOwnershipType('urn:li:ownershipType:custom', null))).toEqual('Other');
        expect(getOwnershipTypeName(null)).toEqual('Other');
    });
});
