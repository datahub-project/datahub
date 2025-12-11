/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Button, Icon } from '@components';
import React from 'react';
import styled, { useTheme } from 'styled-components';

import { SearchBarV2 } from '@app/searchV2/searchBarV2/SearchBarV2';
import useGoToSearchPage from '@app/searchV2/useGoToSearchPage';
import useSearchViewAll from '@app/searchV2/useSearchViewAll';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

const Container = styled.div`
    display: flex;
    flex-direction: column;
`;

const ViewAllContainer = styled.div`
    display: flex;
    justify-content: flex-end;
`;

const StyledButton = styled(Button)`
    padding: 0 8px;
`;

export default function SearchBar() {
    const entityRegistry = useEntityRegistryV2();
    const searchViewAll = useSearchViewAll();
    const search = useGoToSearchPage(null);
    const themeConfig = useTheme();

    return (
        <Container>
            <SearchBarV2
                placeholderText={themeConfig.content.search.searchbarMessage}
                onSearch={search}
                entityRegistry={entityRegistry}
                width="100%"
                fixAutoComplete
                viewsEnabled
                viewsInPopover={false}
                isShowNavBarRedesign
                showViewAllResults
                combineSiblings
                showCommandK
            />
            <ViewAllContainer>
                <StyledButton variant="text" color="gray" size="sm" onClick={searchViewAll}>
                    Discover <Icon icon="ArrowRight" source="phosphor" size="sm" />
                </StyledButton>
            </ViewAllContainer>
        </Container>
    );
}
