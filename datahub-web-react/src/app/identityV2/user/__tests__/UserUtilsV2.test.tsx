import { BookOpen, Gear, PencilSimple, User } from '@phosphor-icons/react';

import { mapRoleIconV2 } from '@app/identityV2/user/UserUtilsV2';

describe('UserUtilsV2', () => {
    describe('mapRoleIconV2', () => {
        it('should return Gear for Admin role', () => {
            const result: any = mapRoleIconV2('Admin');
            expect(result.type).toEqual(Gear);
        });

        it('should return PencilSimple for Editor role', () => {
            const result: any = mapRoleIconV2('Editor');
            expect(result.type).toEqual(PencilSimple);
        });

        it('should return BookOpen for Reader role', () => {
            const result: any = mapRoleIconV2('Reader');
            expect(result.type).toEqual(BookOpen);
        });

        it('should return User for unknown role names', () => {
            const result: any = mapRoleIconV2('CustomRole');
            expect(result.type).toEqual(User);
        });

        it('should return User for empty role name', () => {
            const result: any = mapRoleIconV2('');
            expect(result.type).toEqual(User);
        });

        it('should return User for null role name', () => {
            const result: any = mapRoleIconV2(null);
            expect(result.type).toEqual(User);
        });

        it('should return User for undefined role name', () => {
            const result: any = mapRoleIconV2(undefined);
            expect(result.type).toEqual(User);
        });
    });
});

