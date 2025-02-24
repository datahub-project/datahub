import { useGetDefaultLineageStartTimeMillis } from '@app/lineage/utils/useGetLineageTimeParams';
import { Divider } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { ErrorRounded } from '@mui/icons-material';
import { useSearchAcrossLineageQuery } from '../../../../../graphql/search.generated';
import { Dataset, EntityType, FilterOperator, LineageDirection } from '../../../../../types.generated';
import {
    HAS_ACTIVE_INCIDENTS_FILTER_NAME,
    HAS_FAILING_ASSERTIONS_FILTER_NAME,
} from '../../../../search/utils/constants';
import { useAppConfig } from '../../../../useAppConfig';
import { useEntityData } from '../../../../entity/shared/EntityContext';
import { DATASET_COUNT } from './utils';
import UpstreamEntitiesList from './UpstreamEntitiesList';
import { CTAWrapper, StyledArrow } from '../../containers/profile/sidebar/FormInfo/components';

export const StyledDivider = styled(Divider)`
    margin: 16px 0;
`;

const Title = styled.div`
    font-size: 14px;
    font-weight: 600;
    display: flex;
    align-items: center;
`;

const Header = styled.div`
    display: flex;
    gap: 8px;
    align-items: center;
`;

const TitleWrapper = styled.div<{ isOpen?: boolean }>`
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0 10px;
    margin-bottom: ${(props) => (props.isOpen ? '10px' : '12px')};
    cursor: pointer;
    text-wrap: auto;
`;

const Container = styled.div`
    margin-bottom: 10px;
`;

export default function UpstreamHealth() {
    const { entityData } = useEntityData();

    const [isOpen, setIsOpen] = useState(false);
    const [directUpstreamEntities, setDirectUpstreamEntities] = useState<Dataset[]>([]);
    const [indirectUpstreamEntities, setIndirectUpstreamEntities] = useState<Dataset[]>([]);

    const [directUpstreamsDataStart, setDirectUpstreamsDataStart] = useState(0);
    const [indirectUpstreamsDataStart, setIndirectUpstreamsDataStart] = useState(0);

    const [directUpstreamsDataTotal, setDirectUpstreamsDataTotal] = useState(0);
    const [indirectUpstreamsDataTotal, setIndirectUpstreamsDataTotal] = useState(0);

    const appConfig = useAppConfig();
    const lineageEnabled: boolean = appConfig?.config?.chromeExtensionConfig?.lineageEnabled || false;
    const startTimeMillis = useGetDefaultLineageStartTimeMillis();

    const urn = entityData?.urn || '';

    const generateQueryVariables = ({ degree, start }) => {
        return {
            skip: !lineageEnabled,
            variables: {
                input: {
                    urn,
                    query: '*',
                    startTimeMillis,
                    types: [EntityType.Dataset],
                    count: DATASET_COUNT,
                    start,
                    direction: LineageDirection.Upstream,
                    orFilters: [
                        {
                            and: [
                                {
                                    field: 'degree',
                                    condition: FilterOperator.Equal,
                                    values: degree,
                                },
                                {
                                    field: HAS_ACTIVE_INCIDENTS_FILTER_NAME,
                                    condition: FilterOperator.Equal,
                                    values: ['true'],
                                },
                            ],
                        },
                        {
                            and: [
                                {
                                    field: 'degree',
                                    condition: FilterOperator.Equal,
                                    values: degree,
                                },
                                {
                                    field: HAS_FAILING_ASSERTIONS_FILTER_NAME,
                                    condition: FilterOperator.Equal,
                                    values: ['true'],
                                },
                            ],
                        },
                    ],
                },
                includeAssertions: false,
                includeIncidents: false,
            },
        };
    };

    const { data: directUpstreamData, fetchMore: fetchMoreDirectUpstreamData } = useSearchAcrossLineageQuery(
        generateQueryVariables({ degree: ['1'], start: directUpstreamsDataStart }),
    );

    const { data: indirectUpstreamData, fetchMore: fetchMoreIndirectUpstreamData } = useSearchAcrossLineageQuery(
        generateQueryVariables({ degree: ['2', '3+'], start: indirectUpstreamsDataStart }),
    );

    useEffect(() => {
        if (directUpstreamData?.searchAcrossLineage?.searchResults?.length && !directUpstreamEntities.length) {
            setDirectUpstreamEntities(
                directUpstreamData.searchAcrossLineage.searchResults.map((result) => result.entity as Dataset),
            );
            setDirectUpstreamsDataTotal(directUpstreamData.searchAcrossLineage.total);
        }
    }, [
        directUpstreamData?.searchAcrossLineage?.searchResults,
        directUpstreamEntities.length,
        directUpstreamData?.searchAcrossLineage?.total,
    ]);

    useEffect(() => {
        if (indirectUpstreamData?.searchAcrossLineage?.searchResults?.length && !indirectUpstreamEntities.length) {
            setIndirectUpstreamEntities(
                indirectUpstreamData.searchAcrossLineage.searchResults.map((result) => result.entity as Dataset),
            );
            setIndirectUpstreamsDataTotal(indirectUpstreamData.searchAcrossLineage.total);
        }
    }, [
        indirectUpstreamData?.searchAcrossLineage?.searchResults,
        indirectUpstreamEntities.length,
        indirectUpstreamData?.searchAcrossLineage?.total,
    ]);

    function loadMoreDirectUpstreamData() {
        const newStart = directUpstreamsDataStart + DATASET_COUNT;
        fetchMoreDirectUpstreamData(
            generateQueryVariables({
                degree: ['1'],
                start: newStart,
            }),
        ).then((result) => {
            if (result.data.searchAcrossLineage?.searchResults) {
                setDirectUpstreamEntities([
                    ...directUpstreamEntities,
                    ...result.data.searchAcrossLineage.searchResults.map((r) => r.entity as Dataset),
                ]);
            }
        });
        setDirectUpstreamsDataStart(newStart);
    }

    function loadMoreIndirectUpstreamData() {
        const newStart = indirectUpstreamsDataStart + DATASET_COUNT;
        fetchMoreIndirectUpstreamData(
            generateQueryVariables({
                degree: ['2', '3+'],
                start: newStart,
            }),
        ).then((result) => {
            if (result.data.searchAcrossLineage?.searchResults) {
                setIndirectUpstreamEntities([
                    ...indirectUpstreamEntities,
                    ...result.data.searchAcrossLineage.searchResults.map((r) => r.entity as Dataset),
                ]);
            }
        });
        setIndirectUpstreamsDataStart(newStart);
    }

    const hasUnhealthyUpstreams = directUpstreamEntities.length || indirectUpstreamEntities.length;

    if (!hasUnhealthyUpstreams) return null;

    return (
        <Container>
            <CTAWrapper backgroundColor="#FBF3EF" borderColor="#FBF3EF" padding="10px 0 0 0">
                <TitleWrapper isOpen={isOpen} onClick={() => setIsOpen(!isOpen)}>
                    <Header>
                        <ErrorRounded style={{ color: '#E54D1F', fontSize: '18' }} />
                        <Title>Some upstreams are unhealthy</Title>
                    </Header>
                    <StyledArrow isOpen={isOpen} />
                </TitleWrapper>
                {isOpen && (
                    <UpstreamEntitiesList
                        directEntities={directUpstreamEntities}
                        loadMoreDirectEntities={loadMoreDirectUpstreamData}
                        remainingDirectEntities={Math.max(
                            directUpstreamsDataTotal - (directUpstreamsDataStart + DATASET_COUNT),
                            0,
                        )}
                        indirectEntities={indirectUpstreamEntities}
                        loadMoreIndirectEntities={loadMoreIndirectUpstreamData}
                        remainingIndirectEntities={Math.max(
                            indirectUpstreamsDataTotal - (indirectUpstreamsDataStart + DATASET_COUNT),
                            0,
                        )}
                    />
                )}
            </CTAWrapper>
        </Container>
    );
}
