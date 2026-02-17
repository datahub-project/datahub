import { FolderOutlined, RightOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import { Typography } from 'antd';
import React from 'react';
import styled, { useTheme } from 'styled-components';

import useContentTruncation from '@app/shared/useContentTruncation';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType, GlossaryNode } from '@types';

export const StyledRightOutlined = styled(RightOutlined)`
    color: ${(props) => props.theme.colors.textTertiary};
    font-size: 8px;
    margin: 0 4px;
`;

// must display content in reverse to have ellipses at the beginning of content
export const ParentNodesWrapper = styled.div`
    align-items: center;
    white-space: nowrap;
    text-overflow: ellipsis;
    flex-direction: row-reverse;
    display: flex;
`;

export const Ellipsis = styled.span`
    color: ${(props) => props.theme.colors.textTertiary};
    margin-right: 2px;
`;

export const StyledTooltip = styled(Tooltip)`
    display: flex;
    white-space: nowrap;
    overflow: hidden;
`;

const GlossaryNodeText = styled(Typography.Text)<{ color: string }>`
    color: ${(props) => props.theme.colors.textSecondary};

    font-family: Mulish;
    font-size: 10px;
    font-style: normal;
    font-weight: 500;
    line-height: normal;
`;

const GlossaryNodeIcon = styled(FolderOutlined)<{ color: string }>`
    color: ${(props) => props.color};

    &&& {
        font-size: 12px;
        margin-right: 4px;
    }
`;

interface Props {
    parentNodes?: GlossaryNode[] | null;
}

export default function ParentNodesView({ parentNodes }: Props) {
    const entityRegistry = useEntityRegistry();
    const theme = useTheme();
    const { contentRef, isContentTruncated } = useContentTruncation(parentNodes);

    return (
        <StyledTooltip
            title={
                <>
                    {[...(parentNodes || [])]?.reverse()?.map((parentNode, idx) => (
                        <>
                            <GlossaryNodeIcon color="white" />
                            <GlossaryNodeText color="white">
                                {entityRegistry.getDisplayName(EntityType.GlossaryNode, parentNode)}
                            </GlossaryNodeText>
                            {idx + 1 !== parentNodes?.length && <StyledRightOutlined data-testid="right-arrow" />}
                        </>
                    ))}
                </>
            }
            overlayStyle={isContentTruncated ? {} : { display: 'none' }}
        >
            {isContentTruncated && <Ellipsis>...</Ellipsis>}
            <ParentNodesWrapper ref={contentRef}>
                {[...(parentNodes || [])]?.map((parentNode, idx) => (
                    <>
                        hihihihi
                        <GlossaryNodeText color={theme.colors.textTertiary}>
                            {entityRegistry.getDisplayName(EntityType.GlossaryNode, parentNode)}
                        </GlossaryNodeText>
                        <GlossaryNodeIcon color={theme.colors.textTertiary} />
                        {idx + 1 !== parentNodes?.length && <StyledRightOutlined data-testid="right-arrow" />}
                    </>
                ))}
            </ParentNodesWrapper>
        </StyledTooltip>
    );
}
