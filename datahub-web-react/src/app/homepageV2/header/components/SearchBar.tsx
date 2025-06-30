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
    const search = useGoToSearchPage(null, true);
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
