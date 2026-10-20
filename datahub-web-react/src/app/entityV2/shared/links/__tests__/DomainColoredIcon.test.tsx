import { render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { DomainColoredIcon } from '@app/entityV2/shared/links/DomainColoredIcon';

import { Domain, EntityType, IconLibrary } from '@types';

vi.mock('@app/sharedV2/colors/colorUtils', () => ({
    useGenerateDomainColorFromPalette: () => () => '#abcdef',
}));

const getLazyIconMock = vi.fn((name: string, _props?: unknown) => <div data-testid={`lazy-${name}`} />);

vi.mock('@app/mfeframework/lazyIconRegistry', () => ({
    getLazyIcon: (name: string, props?: unknown) => getLazyIconMock(name, props),
}));

const theme = {
    colors: {
        text: '#111',
        bg: '#fff',
        icon: '#333',
        border: '#ddd',
        borderBrand: '#00a',
    },
};

function renderIcon(domain: Domain) {
    return render(
        <ThemeProvider theme={theme as never}>
            <DomainColoredIcon domain={domain} />
        </ThemeProvider>,
    );
}

function domainWithIcon(name: string, library: IconLibrary = IconLibrary.Material): Domain {
    return {
        urn: 'urn:li:domain:test',
        type: EntityType.Domain,
        id: 'test',
        properties: { name: 'Analytics' },
        displayProperties: {
            icon: {
                iconLibrary: library,
                name,
                style: library === IconLibrary.Material ? 'Outlined' : 'regular',
            },
        },
    } as Domain;
}

describe('DomainColoredIcon', () => {
    beforeEach(() => {
        getLazyIconMock.mockClear();
    });

    it('maps a legacy Material icon name to a Phosphor lazy icon', () => {
        renderIcon(domainWithIcon('AccountCircle', IconLibrary.Material));
        expect(getLazyIconMock).toHaveBeenCalledWith('UserCircle', expect.objectContaining({ color: 'currentColor' }));
        expect(screen.getByTestId('lazy-UserCircle')).toBeInTheDocument();
    });

    it('renders a stored Phosphor icon name directly', () => {
        renderIcon(domainWithIcon('RocketLaunch', IconLibrary.Phosphor));
        expect(getLazyIconMock).toHaveBeenCalledWith('RocketLaunch', expect.any(Object));
        expect(screen.getByTestId('lazy-RocketLaunch')).toBeInTheDocument();
    });

    it('does not remap Phosphor names that collide with Material synonyms', () => {
        // Material "Bookmark" maps to BookmarkSimple; a Phosphor pick of Bookmark must stay Bookmark.
        renderIcon(domainWithIcon('Bookmark', IconLibrary.Phosphor));
        expect(getLazyIconMock).toHaveBeenCalledWith('Bookmark', expect.any(Object));
        expect(screen.getByTestId('lazy-Bookmark')).toBeInTheDocument();
    });

    it('falls back to the domain initial when no icon is stored', () => {
        renderIcon({
            urn: 'urn:li:domain:test',
            type: EntityType.Domain,
            id: 'test',
            properties: { name: 'Analytics' },
        } as Domain);
        expect(getLazyIconMock).not.toHaveBeenCalled();
        expect(screen.getByText('A')).toBeInTheDocument();
    });
});
