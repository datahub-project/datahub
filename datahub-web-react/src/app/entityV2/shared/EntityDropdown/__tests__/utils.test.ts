import { canShowEditDeprecation, getDeprecationMenuActions } from '@app/entityV2/shared/EntityDropdown/utils';

import { EntityPrivileges } from '@types';

describe('canShowEditDeprecation', () => {
    it('defaults to allowed when privileges are missing or the flag is unset', () => {
        expect(canShowEditDeprecation(undefined)).toBe(true);
        expect(canShowEditDeprecation(null)).toBe(true);
        expect(canShowEditDeprecation({} as EntityPrivileges)).toBe(true);
    });

    it('respects an explicit canEditDeprecation flag', () => {
        expect(canShowEditDeprecation({ canEditDeprecation: true } as EntityPrivileges)).toBe(true);
        expect(canShowEditDeprecation({ canEditDeprecation: false } as EntityPrivileges)).toBe(false);
    });
});

describe('getDeprecationMenuActions', () => {
    it('offers only "mark" when the entity is not deprecated', () => {
        expect(getDeprecationMenuActions(false, { canEditDeprecation: true } as EntityPrivileges)).toEqual([
            'markDeprecated',
        ]);
    });

    it('offers edit then un-deprecate when deprecated and editing is allowed', () => {
        expect(getDeprecationMenuActions(true, { canEditDeprecation: true } as EntityPrivileges)).toEqual([
            'editDeprecated',
            'markUnDeprecated',
        ]);
    });

    it('omits edit when the user cannot edit deprecation', () => {
        expect(getDeprecationMenuActions(true, { canEditDeprecation: false } as EntityPrivileges)).toEqual([
            'markUnDeprecated',
        ]);
    });
});
