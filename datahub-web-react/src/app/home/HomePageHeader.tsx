import React, { useMemo } from 'react';
import { useHistory } from 'react-router';
import { Typography, Image, Row, Button, Tag } from 'antd';
import styled, { useTheme } from 'styled-components';

import { ManageAccount } from '../shared/ManageAccount';
import { useGetAuthenticatedUser } from '../useGetAuthenticatedUser';
import { useEntityRegistry } from '../useEntityRegistry';
import { navigateToSearchUrl } from '../search/utils/navigateToSearchUrl';
import { SearchBar } from '../search/SearchBar';
import {
    useGetAutoCompleteMultipleResultsLazyQuery,
    useGetSearchResultsForMultipleQuery,
} from '../../graphql/search.generated';
import { EntityType } from '../../types.generated';
import analytics, { EventType } from '../analytics';
import { AdminHeaderLinks } from '../shared/admin/AdminHeaderLinks';
import { ANTD_GRAY } from '../entity/shared/constants';
import { useAppConfig } from '../useAppConfig';

const Background = styled.div`
    width: 100%;
    background-image: linear-gradient(
        ${(props) => props.theme.styles['homepage-background-upper-fade']},
        75%,
        ${(props) => props.theme.styles['homepage-background-lower-fade']}
    );
`;

const WelcomeText = styled(Typography.Text)`
    font-size: 16px;
    color: ${(props) =>
        props.theme.styles['homepage-text-color'] || props.theme.styles['homepage-background-lower-fade']};
`;

const styles = {
    navBar: { padding: '24px' },
    searchContainer: { width: '100%', marginTop: '40px' },
    logoImage: { width: 140 },
    searchBox: { width: '40vw', minWidth: 400, margin: '40px 0px', marginBottom: '12px' },
    subtitle: { marginTop: '28px', color: '#FFFFFF', fontSize: 12 },
};

const HeaderContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    margin-top: 20px;
    margin-bottom: 20px;
`;

const NavGroup = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
`;

const SuggestionsContainer = styled.div`
    padding: 0px 30px;
    max-width: 540px;
    display: flex;
    flex-direction: column;
    justify-content: left;
    align-items: start;
`;

const SuggestionTagContainer = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
    flex-wrap: wrap;
    > div {
        margin-bottom: 12px;
    }
`;

const SuggestionButton = styled(Button)`
    padding: 0px;
    margin-bottom: 16px;
`;

const SuggestionTag = styled(Tag)`
    font-weight: 500;
    font-size: 12px;
    margin-bottom: 20px;
    && {
        padding: 8px 16px;
    }
`;

const SuggestedQueriesText = styled(Typography.Text)`
    margin-left: 12px;
    margin-bottom: 12px;
    color: ${ANTD_GRAY[8]};
`;

const SearchBarContainer = styled.div`
    text-align: center;
`;

function truncate(input, length) {
    if (input.length > length) {
        return `${input.substring(0, length)}...`;
    }
    return input;
}

function sortRandom() {
    return 0.5 - Math.random();
}

export const HomePageHeader = () => {
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const [getAutoCompleteResultsForMultiple, { data: suggestionsData }] = useGetAutoCompleteMultipleResultsLazyQuery();
    const user = useGetAuthenticatedUser()?.corpUser;
    const themeConfig = useTheme();
    const appConfig = useAppConfig();

    const onSearch = (query: string, type?: EntityType) => {
        if (!query || query.trim().length === 0) {
            return;
        }
        analytics.event({
            type: EventType.SearchEvent,
            query,
            pageNumber: 1,
            originPath: window.location.pathname,
        });
        navigateToSearchUrl({
            type,
            query,
            history,
        });
    };

    const onAutoComplete = (query: string) => {
        if (query && query.trim() !== '') {
            getAutoCompleteResultsForMultiple({
                variables: {
                    input: {
                        query,
                        limit: 30,
                    },
                },
            });
        }
    };

    // Fetch results
    const { data: searchResultsData } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types: [],
                query: '*',
                start: 0,
                count: 20,
                filters: [],
            },
        },
    });

    const searchResultsToShow = useMemo(() => {
        let result: string[] | undefined = [];
        if (searchResultsData) {
            const entities = searchResultsData?.searchAcrossEntities?.searchResults.map((searchResult) => {
                return searchResult?.entity;
            });

            result = entities?.map((entity) => {
                return entityRegistry.getDisplayName(entity.type, entity);
            });
        }
        return result?.sort(sortRandom);
    }, [searchResultsData, entityRegistry]);

    return (
        <Background>
            <Row justify="space-between" style={styles.navBar}>
                <WelcomeText>
                    {!!user && (
                        <>
                            Welcome back, <b>{entityRegistry.getDisplayName(EntityType.CorpUser, user)}</b>.
                        </>
                    )}
                </WelcomeText>
                <NavGroup>
                    <AdminHeaderLinks />
                    <ManageAccount
                        urn={user?.urn || ''}
                        pictureLink={user?.editableProperties?.pictureLink || ''}
                        name={(user && entityRegistry.getDisplayName(EntityType.CorpUser, user)) || undefined}
                    />
                </NavGroup>
            </Row>
            <HeaderContainer>
                <Image
                    src={appConfig.config.visualConfig.logoUrl || themeConfig.assets.logoUrl}
                    preview={false}
                    style={styles.logoImage}
                />
                {!!themeConfig.content.subtitle && (
                    <Typography.Text style={styles.subtitle}>{themeConfig.content.subtitle}</Typography.Text>
                )}
                <SearchBarContainer>
                    <SearchBar
                        placeholderText={themeConfig.content.search.searchbarMessage}
                        suggestions={suggestionsData?.autoCompleteForMultiple?.suggestions || []}
                        onSearch={onSearch}
                        onQueryChange={onAutoComplete}
                        autoCompleteStyle={styles.searchBox}
                        entityRegistry={entityRegistry}
                    />
                    {searchResultsToShow && searchResultsToShow.length > 0 && (
                        <SuggestionsContainer>
                            <SuggestedQueriesText strong>Try searching for</SuggestedQueriesText>
                            <SuggestionTagContainer>
                                {searchResultsToShow.slice(0, 3).map((suggestion) => (
                                    <SuggestionButton
                                        key={suggestion}
                                        type="link"
                                        onClick={() =>
                                            navigateToSearchUrl({
                                                type: undefined,
                                                query: `"${suggestion}"`,
                                                history,
                                            })
                                        }
                                    >
                                        <SuggestionTag>{truncate(suggestion, 40)}</SuggestionTag>
                                    </SuggestionButton>
                                ))}
                            </SuggestionTagContainer>
                        </SuggestionsContainer>
                    )}
                </SearchBarContainer>
            </HeaderContainer>
        </Background>
    );
};
