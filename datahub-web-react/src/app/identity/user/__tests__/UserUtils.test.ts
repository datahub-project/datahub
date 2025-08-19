import { EditOutlined, ReadOutlined, SettingOutlined, UserOutlined } from '@ant-design/icons';

import { getRoleNameFromUrn, mapRoleIcon, shouldShowGlossary } from '@app/identity/user/UserUtils';

describe('UserUtils', () => {
    describe('getRoleNameFromUrn', () => {
        it('should convert Admin role URN to Admin', () => {
            const result = getRoleNameFromUrn('urn:li:dataHubRole:admin');
            expect(result).toEqual('Admin');
        });

        it('should convert Editor role URN to Editor', () => {
            const result = getRoleNameFromUrn('urn:li:dataHubRole:editor');
            expect(result).toEqual('Editor');
        });

        it('should convert Reader role URN to Reader', () => {
            const result = getRoleNameFromUrn('urn:li:dataHubRole:reader');
            expect(result).toEqual('Reader');
        });

        it('should handle uppercase role names', () => {
            const result = getRoleNameFromUrn('urn:li:dataHubRole:ADMIN');
            expect(result).toEqual('Admin');
        });

        it('should handle mixed case role names', () => {
            const result = getRoleNameFromUrn('urn:li:dataHubRole:EdItOr');
            expect(result).toEqual('Editor');
        });

        it('should handle custom role names', () => {
            const result = getRoleNameFromUrn('urn:li:dataHubRole:customRole');
            expect(result).toEqual('Customrole');
        });

        it('should handle role names with underscores', () => {
            const result = getRoleNameFromUrn('urn:li:dataHubRole:custom_role');
            expect(result).toEqual('Custom_role');
        });

        it('should handle role names with hyphens', () => {
            const result = getRoleNameFromUrn('urn:li:dataHubRole:custom-role');
            expect(result).toEqual('Custom-role');
        });

        it('should handle empty role name after prefix', () => {
            const result = getRoleNameFromUrn('urn:li:dataHubRole:');
            expect(result).toEqual(undefined);
        });

        it('should handle single character role names', () => {
            const result = getRoleNameFromUrn('urn:li:dataHubRole:a');
            expect(result).toEqual('A');
        });

        it('should handle numeric role names', () => {
            const result = getRoleNameFromUrn('urn:li:dataHubRole:123');
            expect(result).toEqual('123');
        });

        it('should handle role names with spaces (edge case)', () => {
            const result = getRoleNameFromUrn('urn:li:dataHubRole:custom role');
            expect(result).toEqual('Custom role');
        });

        it('should handle role names with special characters', () => {
            const result = getRoleNameFromUrn('urn:li:dataHubRole:role@123');
            expect(result).toEqual('Role@123');
        });

        it('should handle malformed URNs (missing prefix)', () => {
            const result = getRoleNameFromUrn('admin');
            expect(result).toEqual('Admin');
        });

        it('should handle URNs with different prefix', () => {
            const result = getRoleNameFromUrn('urn:li:role:admin');
            expect(result).toEqual('Urn:li:role:admin');
        });
    });

    describe('mapRoleIcon', () => {
        it('should return SettingOutlined for Admin role', () => {
            const result = mapRoleIcon('Admin');
            expect(result.type).toEqual(SettingOutlined);
        });

        it('should return EditOutlined for Editor role', () => {
            const result = mapRoleIcon('Editor');
            expect(result.type).toEqual(EditOutlined);
        });

        it('should return ReadOutlined for Reader role', () => {
            const result = mapRoleIcon('Reader');
            expect(result.type).toEqual(ReadOutlined);
        });

        it('should return UserOutlined for unknown role names', () => {
            const result = mapRoleIcon('CustomRole');
            expect(result.type).toEqual(UserOutlined);
        });

        it('should return UserOutlined for empty role name', () => {
            const result = mapRoleIcon('');
            expect(result.type).toEqual(UserOutlined);
        });

        it('should return UserOutlined for null role name', () => {
            const result = mapRoleIcon(null);
            expect(result.type).toEqual(UserOutlined);
        });

        it('should return UserOutlined for undefined role name', () => {
            const result = mapRoleIcon(undefined);
            expect(result.type).toEqual(UserOutlined);
        });

        it('should be case-sensitive for role matching', () => {
            const result = mapRoleIcon('admin'); // lowercase
            expect(result.type).toEqual(UserOutlined);
        });

        it('should handle numeric role names', () => {
            const result = mapRoleIcon('123');
            expect(result.type).toEqual(UserOutlined);
        });

        it('should handle role names with special characters', () => {
            const result = mapRoleIcon('Admin@123');
            expect(result.type).toEqual(UserOutlined);
        });
    });

    describe('shouldShowGlossary', () => {
        it('should return true when canManageGlossary is true and hideGlossary is false', () => {
            const result = shouldShowGlossary(true, false);
            expect(result).toEqual(true);
        });

        it('should return true when canManageGlossary is true and hideGlossary is true', () => {
            const result = shouldShowGlossary(true, true);
            expect(result).toEqual(true);
        });

        it('should return true when canManageGlossary is false and hideGlossary is false', () => {
            const result = shouldShowGlossary(false, false);
            expect(result).toEqual(true);
        });

        it('should return false when canManageGlossary is false and hideGlossary is true', () => {
            const result = shouldShowGlossary(false, true);
            expect(result).toEqual(false);
        });
    });

    describe('integration tests', () => {
        it('should properly handle the complete flow from URN to icon for Admin', () => {
            const roleName = getRoleNameFromUrn('urn:li:dataHubRole:admin');
            const icon = mapRoleIcon(roleName);
            expect(roleName).toEqual('Admin');
            expect(icon.type).toEqual(SettingOutlined);
        });

        it('should properly handle the complete flow from URN to icon for Editor', () => {
            const roleName = getRoleNameFromUrn('urn:li:dataHubRole:editor');
            const icon = mapRoleIcon(roleName);
            expect(roleName).toEqual('Editor');
            expect(icon.type).toEqual(EditOutlined);
        });

        it('should properly handle the complete flow from URN to icon for Reader', () => {
            const roleName = getRoleNameFromUrn('urn:li:dataHubRole:reader');
            const icon = mapRoleIcon(roleName);
            expect(roleName).toEqual('Reader');
            expect(icon.type).toEqual(ReadOutlined);
        });

        it('should properly handle the complete flow from URN to icon for custom role', () => {
            const roleName = getRoleNameFromUrn('urn:li:dataHubRole:customRole');
            const icon = mapRoleIcon(roleName);
            expect(roleName).toEqual('Customrole');
            expect(icon.type).toEqual(UserOutlined);
        });
    });
});
