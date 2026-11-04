import React, { useState } from 'react';
import styled from 'styled-components';

import themes from '@conf/theme/themes';

import { CopyButton } from './CopyButton';

// The theme also carries palette arrays (glossaryPalette, domainPalette); only single colors are tokens.
function colorTokens(colors: object): Record<string, string> {
    return Object.fromEntries(Object.entries(colors).filter(([, value]) => typeof value === 'string'));
}

const LIGHT_COLORS = colorTokens(themes.themeV2.colors);
const DARK_COLORS = colorTokens(themes.themeV2Dark.colors);
const TOKENS = Object.keys(LIGHT_COLORS).sort();

const Table = styled.table`
    width: 100%;
    border-collapse: collapse;

    th {
        text-align: left;
    }

    td,
    th {
        padding: 6px 8px;
        border-bottom: 1px solid ${(props) => props.theme.colors.border};
    }
`;

const Value = styled.span`
    display: inline-flex;
    align-items: center;
    gap: 8px;
    font-family: monospace;
`;

const Swatch = styled.span<{ $color: string }>`
    width: 24px;
    height: 24px;
    border-radius: 4px;
    background: ${(props) => props.$color};
    border: 1px solid ${(props) => props.theme.colors.border};
`;

const Search = styled.input`
    width: 100%;
    padding: 0.5rem;
    margin-bottom: 0.5rem;
`;

function ColorValue({ color }: { color?: string }) {
    if (!color) return <Value>—</Value>;
    return (
        <Value>
            <Swatch $color={color} />
            {color}
        </Value>
    );
}

export const ColorTokenTable = () => {
    const [search, setSearch] = useState('');
    const filtered = TOKENS.filter((token) => token.toLowerCase().includes(search.toLowerCase()));

    return (
        <>
            <Search
                type="search"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Filter tokens, e.g. text, bgSurface, icon…"
            />
            <span>
                Showing {filtered.length} of {TOKENS.length}
            </span>
            <Table>
                <thead>
                    <tr>
                        <th>Token</th>
                        <th>Light</th>
                        <th>Dark</th>
                    </tr>
                </thead>
                <tbody>
                    {filtered.map((token) => (
                        <tr key={token}>
                            <td>
                                <Value>
                                    {token} <CopyButton text={`theme.colors.${token}`} />
                                </Value>
                            </td>
                            <td>
                                <ColorValue color={LIGHT_COLORS[token]} />
                            </td>
                            <td>
                                <ColorValue color={DARK_COLORS[token]} />
                            </td>
                        </tr>
                    ))}
                </tbody>
            </Table>
        </>
    );
};
