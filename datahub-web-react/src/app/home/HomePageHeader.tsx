import React, { useMemo } from 'react';
import { useHistory } from 'react-router';
import { Typography, Image, AutoComplete, Input, Row, Button, Carousel } from 'antd';
import styled, { useTheme } from 'styled-components';

import { ManageAccount } from '../shared/ManageAccount';
import { useGetAuthenticatedUser } from '../useGetAuthenticatedUser';
import { useEntityRegistry } from '../useEntityRegistry';
import { navigateToSearchUrl } from '../search/utils/navigateToSearchUrl';
import { GetSearchResultsQuery, useGetAutoCompleteResultsLazyQuery } from '../../graphql/search.generated';
import { useGetAllEntitySearchResults } from '../../utils/customGraphQL/useGetAllEntitySearchResults';
import { EntityType } from '../../types.generated';

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
    color: ${(props) => props.theme.styles['homepage-background-lower-fade']};
`;

const SubHeaderText = styled(Typography.Text)`
    font-size: 20px;
    color: ${(props) => props.theme.styles['homepage-background-lower-fade']};
`;

const SubHeaderTextNoResults = styled(Typography.Text)`
    font-size: 20px;
    color: ${(props) => props.theme.styles['homepage-background-lower-fade']};
    margin-bottom: 108px;
`;

const styles = {
    navBar: { padding: '24px' },
    searchContainer: { width: '100%', marginTop: '40px' },
    logoImage: { width: 140 },
    searchBox: { width: 540, margin: '40px 0px' },
    subHeaderLabel: { marginTop: '-16px', color: '#FFFFFF', fontSize: 12 },
};

const CarouselElement = styled.div`
    height: 120px;
    color: #fff;
    line-height: 120px;
    text-align: center;
`;

const CarouselContainer = styled.div`
    margin-top: -24px;
    padding-bottom: 40px;
`;

const HeaderContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
`;

function getSuggestionFieldsFromResult(result: GetSearchResultsQuery): string[] {
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
    const { data } = useGetAuthenticatedUser();
    const [getAutoCompleteResults, { data: suggestionsData }] = useGetAutoCompleteResultsLazyQuery();
    const themeConfig = useTheme();

    const onSearch = (query: string) => {
        navigateToSearchUrl({
            query,
            history,
            entityRegistry,
        });
    };

    const onAutoComplete = (query: string) => {
        getAutoCompleteResults({
            variables: {
                input: {
                    type: entityRegistry.getDefaultSearchEntityType(),
                    query,
                },
            },
        });
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
                    {data && (
                        <>
                            Welcome back, <b>{data?.corpUser?.info?.firstName || data?.corpUser?.username}</b>.
                        </>
                    )}
                </WelcomeText>
                <ManageAccount
                    urn={data?.corpUser?.urn || ''}
                    pictureLink={data?.corpUser?.editableInfo?.pictureLink || ''}
                    name={data?.corpUser?.info?.firstName || data?.corpUser?.username || undefined}
                />
            </Row>
            <HeaderContainer>
                <Image src={themeConfig.assets.logoUrl} preview={false} style={styles.logoImage} />
                <AutoComplete
                    style={styles.searchBox}
                    options={suggestionsData?.autoComplete?.suggestions.map((result: string) => ({
                        value: result,
                    }))}
                    onSelect={(value: string) => onSearch(value)}
                    onSearch={(value: string) => onAutoComplete(value)}
                >
                    <Input.Search
                        placeholder={themeConfig.content.search.searchbarMessage}
                        onSearch={(value: string) => onSearch(value)}
                        data-testid="search-input"
                    />
                </AutoComplete>
                {suggestionsToShow.length === 0 && !suggestionsLoading && (
                    <SubHeaderTextNoResults>{themeConfig.content.homepage.homepageMessage}</SubHeaderTextNoResults>
                )}
                {suggestionsToShow.length > 0 && !suggestionsLoading && (
                    <Typography.Text style={styles.subHeaderLabel}>Try searching for...</Typography.Text>
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
        </Background>
    );
};
