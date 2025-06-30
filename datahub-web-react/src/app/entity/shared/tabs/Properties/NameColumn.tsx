import { Tooltip, Typography } from 'antd';
import React from 'react';
import Highlight from 'react-highlighter';
import styled from 'styled-components';

import ChildCountLabel from '@app/entity/shared/tabs/Dataset/Schema/components/ChildCountLabel';
import PropertyTypeLabel from '@app/entity/shared/tabs/Dataset/Schema/components/PropertyTypeLabel';
import CardinalityLabel from '@app/entity/shared/tabs/Properties/CardinalityLabel';
import StructuredPropertyTooltip from '@app/entity/shared/tabs/Properties/StructuredPropertyTooltip';
import { PropertyRow } from '@app/entity/shared/tabs/Properties/types';

const ParentNameText = styled(Typography.Text)`
    color: #373d44;
    font-size: 16px;
    font-family: Manrope;
    font-weight: 600;
    line-height: 20px;
    word-wrap: break-word;
    padding-left: 16px;
    display: flex;
    align-items: center;
`;

const ChildNameText = styled(Typography.Text)`
    align-self: stretch;
    color: #373d44;
    font-size: 14px;
    font-family: Manrope;
    font-weight: 500;
    line-height: 18px;
    word-wrap: break-word;
    padding-left: 16px;
    display: flex;
    align-items: center;
`;

const NameLabelWrapper = styled.span`
    display: inline-flex;
    align-items: center;
    flex-wrap: wrap;
`;

interface Props {
    propertyRow: PropertyRow;
    filterText?: string;
}

export default function NameColumn({ propertyRow, filterText }: Props) {
    const { structuredProperty } = propertyRow;
    return (
        <>
            {propertyRow.children ? (
                <NameLabelWrapper>
                    <ParentNameText>
                        <Highlight search={filterText}>{propertyRow.displayName}</Highlight>
                    </ParentNameText>
                    {propertyRow.childrenCount ? <ChildCountLabel count={propertyRow.childrenCount} /> : <span />}
                </NameLabelWrapper>
            ) : (
                <NameLabelWrapper>
                    <Tooltip
                        color="#373D44"
                        placement="topRight"
                        title={
                            structuredProperty ? (
                                <StructuredPropertyTooltip structuredProperty={structuredProperty} />
                            ) : (
                                ''
                            )
                        }
                    >
                        <ChildNameText>
                            <Highlight search={filterText}>{propertyRow.displayName}</Highlight>
                        </ChildNameText>
                    </Tooltip>
                    {propertyRow.type ? (
                        <PropertyTypeLabel type={propertyRow.type} dataType={propertyRow.dataType} />
                    ) : (
                        <span />
                    )}
                    {structuredProperty?.definition?.allowedValues && (
                        <CardinalityLabel structuredProperty={structuredProperty} />
                    )}
                </NameLabelWrapper>
            )}
        </>
    );
}
