import { useGetDefaultLineageStartTimeMillis } from '@app/lineage/utils/useGetLineageTimeParams';
import { Divider } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { ErrorRounded } from '@mui/icons-material';
import { isDeprecated, isUnhealthy } from '@src/app/shared/health/healthUtils';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { GenericEntityProperties } from '@src/app/entity/shared/types';
import { useSearchAcrossLineageQuery } from '../../../../../graphql/search.generated';
import { FilterOperator, LineageDirection } from '../../../../../types.generated';
import {
    HAS_ACTIVE_INCIDENTS_FILTER_NAME,
    HAS_FAILING_ASSERTIONS_FILTER_NAME,
    IS_DEPRECATED_FILTER_NAME,
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
    const entityRegistry = useEntityRegistry();

    const [isOpen, setIsOpen] = useState(false);
    const [directUpstreamEntities, setDirectUpstreamEntities] = useState<GenericEntityProperties[]>([]);
    const [indirectUpstreamEntities, setIndirectUpstreamEntities] = useState<GenericEntityProperties[]>([]);

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
                    searchFlags: {
                        skipCache: true,
                    },
                    urn,
                    query: '*',
                    startTimeMillis,
                    types: [],
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
                        {
                            and: [
                                {
                                    field: 'degree',
                                    condition: FilterOperator.Equal,
                                    values: degree,
                                },
                                {
                                    field: IS_DEPRECATED_FILTER_NAME,
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
                directUpstreamData.searchAcrossLineage.searchResults
                    .map((result) => entityRegistry.getGenericEntityProperties(result.entity.type, result.entity))
                    .filter((e) => e !== null) as GenericEntityProperties[],
            );
            setDirectUpstreamsDataTotal(directUpstreamData.searchAcrossLineage.total);
        }
    }, [
        directUpstreamData?.searchAcrossLineage?.searchResults,
        directUpstreamEntities.length,
        directUpstreamData?.searchAcrossLineage?.total,
        entityRegistry,
    ]);

    useEffect(() => {
        if (indirectUpstreamData?.searchAcrossLineage?.searchResults?.length && !indirectUpstreamEntities.length) {
            setIndirectUpstreamEntities(
                indirectUpstreamData.searchAcrossLineage.searchResults
                    .map((result) => entityRegistry.getGenericEntityProperties(result.entity.type, result.entity))
                    .filter((e) => e !== null) as GenericEntityProperties[],
            );
            setIndirectUpstreamsDataTotal(indirectUpstreamData.searchAcrossLineage.total);
        }
    }, [
        indirectUpstreamData?.searchAcrossLineage?.searchResults,
        indirectUpstreamEntities.length,
        indirectUpstreamData?.searchAcrossLineage?.total,
        entityRegistry,
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
                    ...(result.data.searchAcrossLineage.searchResults
                        .map((r) => entityRegistry.getGenericEntityProperties(r.entity.type, r.entity))
                        .filter((e) => e !== null) as GenericEntityProperties[]),
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
                    ...(result.data.searchAcrossLineage.searchResults
                        .map((r) => entityRegistry.getGenericEntityProperties(r.entity.type, r.entity))
                        .filter((e) => e !== null) as GenericEntityProperties[]),
                ]);
            }
        });
        setIndirectUpstreamsDataStart(newStart);
    }

    const unhealthyDirectUpstreams = directUpstreamEntities.filter(
        (e) => (e.health && isUnhealthy(e.health)) || isDeprecated(e),
    );
    const unhealthyIndirectUpstreams = indirectUpstreamEntities.filter(
        (e) => (e.health && isUnhealthy(e.health)) || isDeprecated(e),
    );

    const hasUnhealthyUpstreams = unhealthyDirectUpstreams.length || unhealthyIndirectUpstreams.length;

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
                        directEntities={unhealthyDirectUpstreams}
                        loadMoreDirectEntities={loadMoreDirectUpstreamData}
                        remainingDirectEntities={Math.max(
                            directUpstreamsDataTotal - (directUpstreamsDataStart + DATASET_COUNT),
                            0,
                        )}
                        indirectEntities={unhealthyIndirectUpstreams}
                        loadMoreIndirectEntities={loadMoreIndirectUpstreamData}
                        remainingIndirectEntities={Math.max(
                            indirectUpstreamsDataTotal - (indirectUpstreamsDataStart + DATASET_COUNT),
                            0,
                        )}
                        showDeprecatedIcon
                        showHealthIcon
                    />
                )}
            </CTAWrapper>
        </Container>
    );
}
