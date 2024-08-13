import { NetworkStatus } from '@apollo/client';
import { Skeleton } from 'antd';
import React, { useEffect } from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';
import { PageRoutes } from '../../../../conf/Global';
import { useGetSearchResultsForMultipleQuery } from '../../../../graphql/search.generated';
import { EntityType } from '../../../../types.generated';
import { REDESIGN_COLORS } from '../../../entityV2/shared/constants';
import { HorizontalListSkeletons } from '../../../homeV2/content/HorizontalListSkeletons';
import { StyledButton } from '../../../shared/share/v2/styledComponents';
import FormCard from './FormCard';

const Container = styled.div`
    display: flex;
    justify-content: space-between;
    margin: 20px;
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
`;

const FormsList = styled.div`
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
    gap: 30px;
    margin: 20px 0;
`;

const SkeletonCard = styled(Skeleton.Button)`
    &&& {
        height: 150px;
        width: 260px;
    }
`;

const SkeletonContainer = styled.div`
    display: flex;
    flex-direction: column;
    margin: 20px;
`;

const FormsTab = () => {
    const history = useHistory();
    const inputs = {
        types: [EntityType.Form],
        query: '*',
        start: 0,
        count: 100,
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

    const formsData = searchData?.searchAcrossEntities?.searchResults || [];

    const isLoading = loading || networkStatus === NetworkStatus.refetch;

    useEffect(() => {
        refetch();
    }, [refetch]);

    return (
        <Container>
            <FormsSection>
                <SectionHeader>
                    <HeaderText>All Forms</HeaderText>
                    <StyledButton
                        $color={REDESIGN_COLORS.TITLE_PURPLE}
                        $type="filled"
                        onClick={() => history.push(PageRoutes.NEW_FORM)}
                    >
                        Create Form
                    </StyledButton>
                </SectionHeader>
                {isLoading ? (
                    <SkeletonContainer>
                        <HorizontalListSkeletons Component={SkeletonCard} showHeader={false} count={4} />
                        <HorizontalListSkeletons Component={SkeletonCard} showHeader={false} count={4} />
                    </SkeletonContainer>
                ) : (
                    <FormsList>
                        {formsData.map((form) => {
                            const formEntity = form.entity as any;
                            return <FormCard formData={formEntity} />;
                        })}
                    </FormsList>
                )}
            </FormsSection>
        </Container>
    );
};

export default FormsTab;
