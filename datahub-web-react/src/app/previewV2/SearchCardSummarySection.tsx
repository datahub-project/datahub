import React, { useState } from 'react';
import { Divider } from 'antd';
import styled from 'styled-components';

import { Pill } from '@src/alchemy-components';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { GenericEntityProperties } from '@app/entity/shared/types';
import { getSearchSummarySourceUrn, getSearchSummaryLabel } from '@src/app/useAppConfig';
import { StringValue } from '@types';

const Container = styled.div<{ expanded: boolean }>`
    display: flex;
    align-items: ${(p) => (p.expanded ? 'flex-start' : 'center')};
    gap: 8px;
    margin-top: 8px;
    width: 100%;
`;

const DescriptionWrapper = styled.div`
    flex: 1;
    min-width: 0;
`;

const Description = styled.div`
    font-size: 12px;
    color: #5f6685;
    line-height: 18px;
`;

const ReadMoreButton = styled.button`
    background: none;
    border: none;
    padding: 0;
    font-size: 12px;
    color: ${REDESIGN_COLORS.BLUE};
    cursor: pointer;
    display: inline;

    &:hover {
        text-decoration: underline;
    }
`;

const HorizontalDivider = styled(Divider)`
    color: ${REDESIGN_COLORS.FOUNDATION_BLUE_2};
    margin-top: 14px;
    margin-bottom: 8px;
    width: calc(100% + 40px) !important;
    margin-left: -20px;
`;

const CHAR_THRESHOLD = 120;

interface Props {
    data: GenericEntityProperties | null;
}

export default function SummarySection({ data }: Props) {
    const [expanded, setExpanded] = useState(false);

    const summaryPropertyUrn = getSearchSummarySourceUrn();
    const summaryLabel = getSearchSummaryLabel();
    const summary = summaryPropertyUrn
        ? (data?.structuredProperties?.properties
              ?.find((p) => p.structuredProperty.urn === summaryPropertyUrn)
              ?.values?.[0] as StringValue)?.stringValue
        : undefined;

    if (!summary) return null;

    const isLong = summary.length > CHAR_THRESHOLD;

    let content: React.ReactNode;
    if (isLong && !expanded) {
        content = <>{summary.slice(0, CHAR_THRESHOLD)}&hellip;{' '}<ReadMoreButton onClick={() => setExpanded(true)}>Read more</ReadMoreButton></>;
    } else if (isLong) {
        content = <>{summary}{' '}<ReadMoreButton onClick={() => setExpanded(false)}>Show less</ReadMoreButton></>;
    } else {
        content = summary;
    }

    return (
        <>
            <HorizontalDivider />
            <Container expanded={expanded}>
                <Pill label={summaryLabel} size="sm" color="violet" leftIcon="Sparkle" iconSource="phosphor" clickable={false} showLabel/>
                <DescriptionWrapper>
                    <Description>{content}</Description>
                </DescriptionWrapper>
            </Container>
        </>
    );
}