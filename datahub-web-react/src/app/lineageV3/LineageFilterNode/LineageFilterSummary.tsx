import React from 'react';
import styled from 'styled-components';

import { PlatformAggregate, SubtypeAggregate } from '@app/lineageV3/LineageFilterNode/useFetchFilterNodeContents';
import { getFilterIconAndLabel } from '@app/searchV2/filters/utils';
import { ENTITY_SUB_TYPE_FILTER_NAME, PLATFORM_FILTER_NAME } from '@app/searchV2/utils/constants';
import { useAppConfig } from '@app/useAppConfig';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

const PillsWrapper = styled.div`
    display: flex;
    flex-direction: column;
    margin-top: 6px;
`;

const PillColumn = styled.div`
    color: ${(props) => props.theme.colors.textSecondary};
    display: flex;
    flex-direction: row;
`;

interface Props {
    platforms?: PlatformAggregate[];
    subtypes?: SubtypeAggregate[];
}

/** Summary of a node's filtered children: their platforms and subtypes, with counts. */
export default function LineageFilterSummary({ platforms, subtypes }: Props) {
    const appConfig = useAppConfig();
    const { showLineageFilterNodes } = appConfig.config.featureFlags;

    return (
        <PillsWrapper>
            <PillColumn>
                {platforms?.map((agg, index) => <PlatformEntry agg={agg} key={agg[0]} index={index} />)}
            </PillColumn>
            {showLineageFilterNodes && (
                <PillColumn>
                    {subtypes
                        ?.filter(([filterValue]) => !filterValue.toLocaleLowerCase().endsWith('query'))
                        .map((agg, index) => <SubtypeEntry agg={agg} key={agg[0]} index={index} />)}
                </PillColumn>
            )}
        </PillsWrapper>
    );
}

interface EntryProps<T> {
    agg: T;
    index: number;
}

function PlatformEntry({ agg, index }: EntryProps<PlatformAggregate>) {
    return LineageFilterEntry(PLATFORM_FILTER_NAME, agg, index, 'platform');
}

function SubtypeEntry({ agg, index }: EntryProps<SubtypeAggregate>) {
    return LineageFilterEntry(ENTITY_SUB_TYPE_FILTER_NAME, agg, index, 'subtype');
}

const EntryWrapper = styled.span<{ includeBefore: boolean }>`
    align-items: center;
    display: flex;

    ${({ includeBefore }) =>
        includeBefore &&
        `::before {
             content: ',';
             margin-right: 4px;
         }`})
`;

const CountWrapper = styled.span`
    margin-left: 4px;
`;

function LineageFilterEntry(
    filterName: string,
    [filterValue, count, entity]: PlatformAggregate | SubtypeAggregate,
    index: number,
    dataTestIdPrefix: string,
) {
    const entityRegistry = useEntityRegistryV2();
    const { icon, label } = getFilterIconAndLabel(filterName, filterValue, entityRegistry, entity || null, 12);

    return (
        <EntryWrapper title={label} includeBefore={index > 0}>
            {icon}
            <CountWrapper data-testid={`filter-counter-${dataTestIdPrefix}-${label}`}>{count}</CountWrapper>
        </EntryWrapper>
    );
}
