import { Button, SearchBar, Tooltip } from '@components';
import analytics, { EventType } from '@src/app/analytics';
import { useUserContext } from '@src/app/context/useUserContext';
import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { EntityType } from '@src/types.generated';
import React, { useState } from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';
import { PageRoutes } from '../../../../conf/Global';
import { REDESIGN_COLORS } from '../../../entityV2/shared/constants';
import FormsTable from './FormsTable';

const Container = styled.div`
    display: flex;
    margin: 20px;
    overflow: auto;
    height: calc(100% - 40px);
`;

const SectionHeader = styled.div`
    display: flex;
    justify-content: space-between;
`;
const HeaderText = styled.div`
    display: flex;
    color: ${REDESIGN_COLORS.TEXT_HEADING_SUB_LINK};
    font-size: 18px;
    font-weight: 700;
`;

const FormsSection = styled.div`
    display: flex;
    flex-direction: column;
    width: 100%;
    gap: 20px;
`;

const FormsContainer = styled.div`
    display: flex;
    overflow: auto;
    flex: 1;
`;

const FormsTab = () => {
    const history = useHistory();
    const me = useUserContext();
    const canEditForms = me.platformPrivileges?.manageDocumentationForms;

    const [searchQuery, setSearchQuery] = useState<string>('');

    const inputs = {
        types: [EntityType.Form],
        query: '*',
        start: 0,
        count: 200,
        searchFlags: { skipCache: true },
    };

    // Execute search
    const {
        data: searchData,
        loading,
        refetch,
        networkStatus,
    } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: inputs,
        },
        fetchPolicy: 'cache-first',
        notifyOnNetworkStatusChange: true,
    });

    const handleSearch = (value) => {
        setSearchQuery(value);
    };

    return (
        <Container>
            <FormsSection>
                <SectionHeader>
                    <HeaderText>Your Forms</HeaderText>
                    <Tooltip
                        showArrow={false}
                        title={
                            !canEditForms
                                ? 'Must have permission to manage forms. Ask your DataHub administrator.'
                                : null
                        }
                    >
                        <>
                            <Button
                                icon="Add"
                                onClick={() => {
                                    analytics.event({
                                        type: EventType.CreateFormClickEvent,
                                    });
                                    history.push(PageRoutes.NEW_FORM, {
                                        inputs,
                                        searchAcrossEntities: searchData?.searchAcrossEntities,
                                    });
                                }}
                                disabled={!canEditForms}
                            >
                                Create
                            </Button>
                        </>
                    </Tooltip>
                </SectionHeader>
                <SearchBar placeholder="Search" value={searchQuery} onChange={(value) => handleSearch(value)} />
                <FormsContainer>
                    <FormsTable
                        searchQuery={searchQuery}
                        searchData={searchData}
                        loading={loading}
                        networkStatus={networkStatus}
                        refetch={refetch}
                        inputs={inputs}
                    />
                </FormsContainer>
            </FormsSection>
        </Container>
    );
};

export default FormsTab;
