import { render } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { getRoleDisplayName, mapRoleToPhosphorIcon } from '@app/identity/user/PhosphorRoleUtils';

// Mock the Icon component since it depends on Alchemy components
vi.mock('@components', () => ({
    Icon: ({ icon, source, size }: { icon: string; source: string; size: string }) => (
        <span data-testid={`icon-${icon}-${source}-${size}`}>{icon}</span>
    ),
}));

describe('PhosphorRoleUtils', () => {
    describe('mapRoleToPhosphorIcon', () => {
        it('should return Gear icon for Admin role', () => {
            const result = mapRoleToPhosphorIcon('Admin');
            const { getByTestId } = render(<div>{result}</div>);
            expect(getByTestId('icon-Gear-phosphor-xl')).toBeTruthy();
        });

        it('should return PencilSimple icon for Editor role', () => {
            const result = mapRoleToPhosphorIcon('Editor');
            const { getByTestId } = render(<div>{result}</div>);
            expect(getByTestId('icon-PencilSimple-phosphor-xl')).toBeTruthy();
        });

        it('should return BookOpen icon for Reader role', () => {
            const result = mapRoleToPhosphorIcon('Reader');
            const { getByTestId } = render(<div>{result}</div>);
            expect(getByTestId('icon-BookOpen-phosphor-xl')).toBeTruthy();
        });

        it('should return User icon for unknown role names', () => {
            const result = mapRoleToPhosphorIcon('CustomRole');
            const { getByTestId } = render(<div>{result}</div>);
            expect(getByTestId('icon-User-phosphor-xl')).toBeTruthy();
        });

        it('should return User icon for empty role name', () => {
            const result = mapRoleToPhosphorIcon('');
            const { getByTestId } = render(<div>{result}</div>);
            expect(getByTestId('icon-User-phosphor-xl')).toBeTruthy();
        });

        it('should be case-sensitive for role matching', () => {
            const result = mapRoleToPhosphorIcon('admin'); // lowercase
            const { getByTestId } = render(<div>{result}</div>);
            expect(getByTestId('icon-User-phosphor-xl')).toBeTruthy();
        });

        it('should handle role names with special characters', () => {
            const result = mapRoleToPhosphorIcon('Admin@123');
            const { getByTestId } = render(<div>{result}</div>);
            expect(getByTestId('icon-User-phosphor-xl')).toBeTruthy();
        });
    });

    describe('getRoleDisplayName', () => {
        it('should return provided role name when available', () => {
            const result = getRoleDisplayName('urn:li:dataHubRole:admin', 'Admin');
            expect(result).toEqual('Admin');
        });

        it('should extract and capitalize role name from URN when name not provided', () => {
            const result = getRoleDisplayName('urn:li:dataHubRole:admin');
            expect(result).toEqual('Admin');
        });

        it('should handle Editor role URN', () => {
            const result = getRoleDisplayName('urn:li:dataHubRole:editor');
            expect(result).toEqual('Editor');
        });

        it('should handle Reader role URN', () => {
            const result = getRoleDisplayName('urn:li:dataHubRole:reader');
            expect(result).toEqual('Reader');
        });

        it('should handle custom role URNs', () => {
            const result = getRoleDisplayName('urn:li:dataHubRole:customRole');
            expect(result).toEqual('CustomRole');
        });

        it('should handle role URNs with underscores', () => {
            const result = getRoleDisplayName('urn:li:dataHubRole:custom_role');
            expect(result).toEqual('Custom_role');
        });

        it('should prefer provided role name over URN parsing', () => {
            const result = getRoleDisplayName('urn:li:dataHubRole:admin', 'System Administrator');
            expect(result).toEqual('System Administrator');
        });
    });
});
