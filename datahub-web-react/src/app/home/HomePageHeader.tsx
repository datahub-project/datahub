import React, { useMemo } from 'react';
import { useHistory } from 'react-router';
import { Typography, Image, Row, Button, Carousel } from 'antd';
import styled, { useTheme } from 'styled-components';

import { ManageAccount } from '../shared/ManageAccount';
import { useGetAuthenticatedUser } from '../useGetAuthenticatedUser';
import { useEntityRegistry } from '../useEntityRegistry';
import { navigateToSearchUrl } from '../search/utils/navigateToSearchUrl';
import { SearchBar } from '../search/SearchBar';
import { GetSearchResultsQuery, useGetAutoCompleteMultipleResultsLazyQuery } from '../../graphql/search.generated';
import { useGetAllEntitySearchResults } from '../../utils/customGraphQL/useGetAllEntitySearchResults';
import { EntityType } from '../../types.generated';
import analytics, { EventType } from '../analytics';
import { AdminHeaderLinks } from '../shared/admin/AdminHeaderLinks';

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

const SubHeaderText = styled(Typography.Text)`
    font-size: 20px;
    color: ${(props) =>
        props.theme.styles['homepage-text-color'] || props.theme.styles['homepage-background-lower-fade']};
`;

const SubHeaderLabelText = styled(Typography.Text)`
    font-size: 12px;
    color: ${(props) =>
        props.theme.styles['homepage-text-color'] || props.theme.styles['homepage-background-lower-fade']};
`;

const SubHeaderTextNoResults = styled(Typography.Text)`
    font-size: 20px;
    color: ${(props) =>
        props.theme.styles['homepage-text-color'] || props.theme.styles['homepage-background-lower-fade']};
    margin-bottom: 108px;
`;

const styles = {
    navBar: { padding: '24px' },
    searchContainer: { width: '100%', marginTop: '40px' },
    logoImage: { width: 140 },
    searchBox: { width: 540, margin: '40px 0px' },
    subtitle: { marginTop: '28px', color: '#FFFFFF', fontSize: 12 },
};

const CarouselElement = styled.div`
    height: 120px;
    color: ${(props) =>
        props.theme.styles['homepage-text-color'] || props.theme.styles['homepage-background-lower-fade']};
    line-height: 120px;
    text-align: center;
`;

const CarouselContainer = styled.div`
    margin-top: -24px;
    padding-bottom: 40px;
    .ant-carousel .slick-dots li button {
        opacity: 0.4;
        background-color: ${(props) =>
            props.theme.styles['homepage-text-color'] || props.theme.styles['homepage-background-lower-fade']};
    }

    .ant-carousel .slick-dots li.slick-active button {
        opacity: 1;
        background-color: ${(props) =>
            props.theme.styles['homepage-text-color'] || props.theme.styles['homepage-background-lower-fade']};
    }
`;

const HeaderContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
`;

const NavGroup = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
`;

const SuggestionsContainer = styled.div`
    height: 140px;
`;

const SearchBarContainer = styled.div`
    text-align: center;
`;

function getSuggestionFieldsFromResult(result: GetSearchResultsQuery | undefined): string[] {
    return (
        (result?.search?.searchResults
            ?.map((searchResult) => searchResult.entity)
            ?.map((entity) => {
                switch (entity.__typename) {
                    case 'Dataset':
                        return entity.name.split('.').slice(-1)[0];
                    case 'CorpUser':
                        return entity.username;
                    case 'Chart':
                        return entity.info?.name;
                    case 'Dashboard':
                        return entity.info?.name;
                    default:
                        return undefined;
                }
            })
            .filter(Boolean) as string[]) || []
    );
}

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
            entityRegistry,
        });
    };

    const onAutoComplete = (query: string) => {
        if (query && query !== '') {
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

    // fetch some results from each entity to display search suggestions
    const allSearchResultsByType = useGetAllEntitySearchResults({
        query: '*',
        start: 0,
        count: 20,
        filters: [],
    });

    const suggestionsLoading = Object.keys(allSearchResultsByType).some((type) => {
        return allSearchResultsByType[type].loading;
    });

    const suggestionsToShow = useMemo(() => {
        let result: string[] = [];
        if (!suggestionsLoading) {
            [EntityType.Dashboard, EntityType.Chart, EntityType.Dataset].forEach((type) => {
                const suggestionsToShowForEntity = getSuggestionFieldsFromResult(
                    allSearchResultsByType[type]?.data,
                ).sort(sortRandom);
                const suggestionToAddToFront = suggestionsToShowForEntity?.pop();
                result = [...result, ...suggestionsToShowForEntity];
                if (suggestionToAddToFront) {
                    result.splice(0, 0, suggestionToAddToFront);
                }
            });
        }
        return result;
    }, [suggestionsLoading, allSearchResultsByType]);

    return (
        <Background>
            <Row justify="space-between" style={styles.navBar}>
                <WelcomeText>
                    {!!user && (
                        <>
                            Welcome back, <b>{user.info?.firstName || user.username}</b>.
                        </>
                    )}
                </WelcomeText>
                <NavGroup>
                    <AdminHeaderLinks />
                    <ManageAccount
                        urn={user?.urn || ''}
                        pictureLink={user?.editableInfo?.pictureLink || ''}
                        name={user?.info?.firstName || user?.username || undefined}
                    />
                </NavGroup>
            </Row>
            <HeaderContainer>
                <Image src={themeConfig.assets.logoUrl} preview={false} style={styles.logoImage} />
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
                </SearchBarContainer>
            </HeaderContainer>
            <SuggestionsContainer>
                <HeaderContainer>
                    {suggestionsToShow.length === 0 && !suggestionsLoading && (
                        <SubHeaderTextNoResults>{themeConfig.content.homepage.homepageMessage}</SubHeaderTextNoResults>
                    )}
                    {suggestionsToShow.length > 0 && !suggestionsLoading && (
                        <SubHeaderLabelText>Try searching for...</SubHeaderLabelText>
                    )}
                </HeaderContainer>
                {suggestionsToShow.length > 0 && !suggestionsLoading && (
                    <CarouselContainer>
                        <Carousel autoplay effect="fade">
                            {suggestionsToShow.length > 0 &&
                                suggestionsToShow.slice(0, 3).map((suggestion) => (
                                    <CarouselElement key={suggestion}>
                                        <Button
                                            type="text"
                                            onClick={() =>
                                                navigateToSearchUrl({
                                                    type: undefined,
                                                    query: suggestion,
                                                    history,
                                                    entityRegistry,
                                                })
                                            }
                                        >
                                            <SubHeaderText>{truncate(suggestion, 40)}</SubHeaderText>
                                        </Button>
                                    </CarouselElement>
                                ))}
                        </Carousel>
                    </CarouselContainer>
                )}
            </SuggestionsContainer>
        </Background>
    );
};
