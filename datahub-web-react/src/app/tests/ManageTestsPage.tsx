import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { message, Typography } from 'antd';
import { useLocation } from 'react-router';
import * as QueryString from 'query-string';
import { ANTD_GRAY } from '../entity/shared/constants';
import { Tests } from './Tests';
import { DEFAULT_TESTS_PAGE_SIZE, METADATA_TESTS_DOC_URL } from './constants';
import { useListTestsQuery } from '../../graphql/test.generated';
import { Message } from '../shared/Message';
import TabToolbar from '../entity/shared/components/styled/TabToolbar';
import { useEntityRegistry } from '../useEntityRegistry';
import { filterTests } from './utils';
import { NewTestButton } from './NewTestButton';
import { useShowNavBarRedesign } from '../useShowNavBarRedesign';
import { PageTitle, SearchBar } from '@components';
import { ButtonContainer, PageContainer, HeaderContainer, HeaderContent } from '../govern/structuredProperties/styledComponents';

const Container = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    padding-top: 20px;
    background-color: #fff;
    height: 100%;
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        border-radius: ${props.theme.styles['border-radius-navbar-redesign']};
        margin: 5px;
        overflow: hidden;
        box-shadow: ${props.theme.styles['box-shadow-navbar-redesign']};
    `}
`;

const testSearchStyle = {
    maxWidth: 330,
    padding: 0,
};

const testSearchInputStyle = {
    height: 32,
    fontSize: 12,
};

export const ManageTestsPage = () => {
    const entityRegistry = useEntityRegistry();
    const location = useLocation();
    const isShowNavBarRedesign = useShowNavBarRedesign();

    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const paramsQuery = (params?.query as string) || undefined;
    const [filterText, setFilterText] = useState<undefined | string>(undefined);
    useEffect(() => setFilterText(paramsQuery), [paramsQuery]);

    /**
     * We always fetch 1,000 tests initially to make the following experience snappier.
     */
    const { loading, error, data } = useListTestsQuery({
        variables: {
            input: {
                start: 0,
                count: DEFAULT_TESTS_PAGE_SIZE,
            },
        },
        fetchPolicy: 'cache-first',
    });
    const tests = data?.listTests?.tests || [];
    const filteredTests = filterTests(filterText, tests as any);

    return (
        <PageContainer $isShowNavBarRedesign={isShowNavBarRedesign}>
            {!data && loading && <Message type="loading" content="Loading tests..." />}
            {error && message.error({ content: `Failed to load Tests! An unexpected error occurred.`, duration: 3 })}
            <HeaderContainer>
                <HeaderContent>
                    <PageTitle
                        title="Metadata Tests"
                        subTitle="Discover & monitor data assets matching a set of logical conditions."
                    />
                </HeaderContent>
                <ButtonContainer>
                    <NewTestButton />
                </ButtonContainer>
            </HeaderContainer>

            <SearchBar
                initialQuery=""
                placeholderText="Search by name, description, category..."
                suggestions={[]}
                style={testSearchStyle}
                inputStyle={testSearchInputStyle}
                onSearch={() => null}
                onQueryChange={(q) => setFilterText(q)}
                entityRegistry={entityRegistry}
                hideRecommendations
            />
            <Tests tests={filteredTests} />
        </PageContainer>
    );
};
