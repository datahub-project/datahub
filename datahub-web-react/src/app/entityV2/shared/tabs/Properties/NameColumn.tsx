import { Typography } from 'antd';
import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';
import Highlight from 'react-highlighter';
import { PropertyRow } from './types';
import StructuredPropertyTooltip from './StructuredPropertyTooltip';

const ParentNameText = styled(Typography.Text)`
    color: #373d44;
    font-size: 14px;
    font-family: Manrope;
    font-weight: 600;
    line-height: 20px;
    word-wrap: break-word;
    padding-left: 4px;
    display: flex;
    align-items: center;
`;

const ChildNameText = styled(Typography.Text)`
    align-self: stretch;
    color: #373d44;
    font-size: 12px;
    font-family: Manrope;
    font-weight: 500;
    word-wrap: break-word;
    display: flex;
    align-items: center;
`;

const NameLabelWrapper = styled.span`
    display: inline-flex;
    align-items: center;
    flex-wrap: wrap;
`;

const ChildCountText = styled.span`
    color: #373d44;
    font-size: 12px;
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
                    {propertyRow.childrenCount ? (
                        <ChildCountText>&nbsp;({propertyRow.childrenCount})</ChildCountText>
                    ) : (
                        <span />
                    )}
                </NameLabelWrapper>
            ) : (
                <NameLabelWrapper>
                    <Tooltip
                        color="#373D44"
                        placement="topRight"
                        overlayStyle={{ minWidth: 'min-content' }}
                        title={structuredProperty ? <StructuredPropertyTooltip propertyRow={propertyRow} /> : ''}
                    >
                        <ChildNameText>
                            <Highlight search={filterText}>{propertyRow.displayName}</Highlight>
                        </ChildNameText>
                    </Tooltip>
                </NameLabelWrapper>
            )}
        </>
    );
}
