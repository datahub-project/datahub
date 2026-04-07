import { Remirror, useRemirror } from '@remirror/react';
import React, { useMemo } from 'react';
import { HelmetProvider } from 'react-helmet-async';
import { MemoryRouter } from 'react-router';
import { ItalicExtension, UnderlineExtension } from 'remirror/extensions';

import UserContextProvider from '@app/context/UserContextProvider';
import EntityRegistry from '@app/entityV2/EntityRegistry';
import { LineageExplorerContext } from '@app/lineage/utils/LineageExplorerContext';
import { CLIENT_AUTH_COOKIE } from '@conf/Global';
import AppConfigProvider from '@src/AppConfigProvider';
import CustomThemeProvider from '@src/CustomThemeProvider';
import { EntityRegistryContext } from '@src/entityRegistryContext';

type Props = {
    children: React.ReactNode;
    initialEntries?: string[];
};

// Lazy singleton — entity classes are only imported on first call, not at module load time.
// This avoids forcing Vitest to resolve all 30 entity modules during the collect phase
// for every test file that imports TestPageContainer.
let cachedRegistry: EntityRegistry | undefined;
export function getTestEntityRegistry(): EntityRegistry {
    if (!cachedRegistry) {
        // eslint-disable-next-line global-require, @typescript-eslint/no-var-requires
        const { default: buildEntityRegistryV2 } = require('@app/buildEntityRegistryV2');
        cachedRegistry = buildEntityRegistryV2();
    }
    return cachedRegistry;
}

export default ({ children, initialEntries }: Props) => {
    const entityRegistry = useMemo(() => getTestEntityRegistry(), []);
    Object.defineProperty(window.document, 'cookie', {
        writable: true,
        value: `${CLIENT_AUTH_COOKIE}=urn:li:corpuser:2`,
    });
    vi.mock('js-cookie', () => ({ default: { get: () => 'urn:li:corpuser:2' } }));

    // mock remirror
    const extensions = () => [new ItalicExtension(), new UnderlineExtension()];
    const { manager, state } = useRemirror({
        extensions,
    });

    return (
        <HelmetProvider>
            <CustomThemeProvider>
                <MemoryRouter initialEntries={initialEntries}>
                    <EntityRegistryContext.Provider value={entityRegistry}>
                        <UserContextProvider>
                            <AppConfigProvider>
                                <Remirror manager={manager} state={state}>
                                    <LineageExplorerContext.Provider
                                        value={{
                                            expandTitles: false,
                                            showColumns: false,
                                            collapsedColumnsNodes: {},
                                            setCollapsedColumnsNodes: null,
                                            fineGrainedMap: {},
                                            selectedField: null,
                                            setSelectedField: () => {},
                                            highlightedEdges: [],
                                            setHighlightedEdges: () => {},
                                            visibleColumnsByUrn: {},
                                            setVisibleColumnsByUrn: () => {},
                                            columnsByUrn: {},
                                            setColumnsByUrn: () => {},
                                            refetchCenterNode: () => {},
                                        }}
                                    >
                                        {children}
                                    </LineageExplorerContext.Provider>
                                </Remirror>
                            </AppConfigProvider>
                        </UserContextProvider>
                    </EntityRegistryContext.Provider>
                </MemoryRouter>
            </CustomThemeProvider>
        </HelmetProvider>
    );
};
