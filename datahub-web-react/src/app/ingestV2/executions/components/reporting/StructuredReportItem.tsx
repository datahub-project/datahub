/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Card, Icon, Text, colors } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';

import { StructuredReportItemContext } from '@app/ingestV2/executions/components/reporting/StructuredReportItemContext';
import { StructuredReportLogEntry } from '@app/ingestV2/executions/components/reporting/types';

const StyledCard = styled(Card)`
    padding: 8px;
    width: 100%;
`;

const Content = styled.div`
    border-radius: 8px;
    margin-top: 8px;
    background-color: white;
    padding: 8px;
`;

const HeaderContainer = styled.div`
    display: flex;
    align-items: center;
    cursor: pointer;
    gap: 8px;
`;

const ChevronIcon = styled(Icon)`
    color: ${colors.gray[400]};
    font-size: 12px;
`;

interface Props {
    item: StructuredReportLogEntry;
    color: string;
    textColor?: string;
    icon?: string;
    defaultActiveKey?: string;
}

export function StructuredReportItem({ item, color, textColor, icon, defaultActiveKey }: Props) {
    const [isExpanded, setIsExpanded] = useState(defaultActiveKey === '0');

    const toggleExpanded = () => {
        setIsExpanded(!isExpanded);
    };

    return (
        <StyledCard
            style={{ backgroundColor: color }}
            onClick={toggleExpanded}
            icon={
                <HeaderContainer>
                    {icon && <Icon icon={icon} source="phosphor" style={{ color: textColor }} size="md" />}
                    <ChevronIcon
                        icon={isExpanded ? 'CaretUp' : 'CaretDown'}
                        source="phosphor"
                        style={{ color: textColor }}
                        size="md"
                    />
                </HeaderContainer>
            }
            title={
                <Text style={{ color: textColor }} weight="semiBold" size="md" lineHeight="normal">
                    {item.title}
                </Text>
            }
            subTitle={
                <Text style={{ color: textColor }} size="sm">
                    {item.message}
                </Text>
            }
            width="100%"
            isCardClickable
        >
            {isExpanded && (
                <Content onClick={(e) => e.stopPropagation()}>
                    <StructuredReportItemContext item={item} />
                </Content>
            )}
        </StyledCard>
    );
}
