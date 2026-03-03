import { green } from '@ant-design/colors';
import { CheckCircleFilled, LoadingOutlined } from '@ant-design/icons';
import Icon from '@ant-design/icons/lib/components/Icon';
import React from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import FailingInputs from '@app/entity/shared/embed/UpstreamHealth/FailingInputs';
import { extractUpstreamSummary } from '@app/entity/shared/embed/UpstreamHealth/utils';

import { useSearchAcrossLineageQuery } from '@graphql/search.generated';
import { Entity, EntityType, FilterOperator, LineageDirection } from '@types';

import SubtractIcon from '@images/subtractIcon.svg?react';

const LoadingWrapper = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 16px;
`;

const TextWrapper = styled.span`
    font-size: 16px;
    line-height: 24px;
    margin-left: 8px;
`;

const UnknownText = styled.span`
    font-size: 14px;
    line-height: 20px;
    margin-left: 8px;
`;

const StyledIcon = styled(Icon)`
    color: ${ANTD_GRAY[7]};
`;

const StyledCheck = styled(CheckCircleFilled)`
    color: ${green[6]};
    font-size: 14px;
`;

export default function UpstreamHealth() {
    const { entityData } = useEntityData();
    const { data, loading } = useSearchAcrossLineageQuery({
        variables: {
            input: {
                urn: entityData?.urn || '',
                query: '*',
                types: [EntityType.Dataset],
                start: 0,
                count: 1000,
                direction: LineageDirection.Upstream,
                orFilters: [{ and: [{ field: 'degree', condition: FilterOperator.Equal, values: ['1', '2', '3+'] }] }],
            },
            includeAssertions: true,
        },
    });

    const upstreams: Entity[] | undefined = data?.searchAcrossLineage?.searchResults?.map((result) => result.entity);
    const upstreamSummary = extractUpstreamSummary(upstreams || []);
    const { passingUpstreams, failingUpstreams } = upstreamSummary;

    if (loading) {
        return (
            <LoadingWrapper>
                <LoadingOutlined />
            </LoadingWrapper>
        );
    }

    if (!data) return null;

    if (failingUpstreams > 0) {
        return <FailingInputs upstreamSummary={upstreamSummary} />;
    }

    if (passingUpstreams > 0) {
        return (
            <div>
                <StyledCheck />
                <TextWrapper>All data inputs are healthy</TextWrapper>
            </div>
        );
    }

    return (
        <div>
            <StyledIcon component={SubtractIcon} />
            <UnknownText>Unknown data input health</UnknownText>
        </div>
    );
}
