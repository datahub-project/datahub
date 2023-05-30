import React from 'react';
import styled from 'styled-components';
import { Typography, Tooltip } from 'antd';
import { FolderOutlined, RightOutlined } from '@ant-design/icons';
import { ANTD_GRAY } from '../../../../constants';
import { EntityType, GlossaryNode } from '../../../../../../../types.generated';
import useContentTruncation from '../../../../../../shared/useContentTruncation';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';

export const StyledRightOutlined = styled(RightOutlined)`
    color: ${ANTD_GRAY[7]};
    font-size: 8px;
    margin: 0 10px;
`;

// must display content in reverse to have ellipses at the beginning of content
export const ParentNodesWrapper = styled.div`
    align-items: center;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    flex-direction: row-reverse;
    display: flex;
`;

export const Ellipsis = styled.span`
    color: ${ANTD_GRAY[7]};
    margin-right: 2px;
`;

export const StyledTooltip = styled(Tooltip)`
    display: flex;
    white-space: nowrap;
    overflow: hidden;
`;

const GlossaryNodeText = styled(Typography.Text)<{ color: string }>`
    font-size: 12px;
    line-height: 20px;
    color: ${(props) => props.color};
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
                        <GlossaryNodeText color={ANTD_GRAY[7]}>
                            {entityRegistry.getDisplayName(EntityType.GlossaryNode, parentNode)}
                        </GlossaryNodeText>
                        <GlossaryNodeIcon color={ANTD_GRAY[7]} />
                        {idx + 1 !== parentNodes?.length && <StyledRightOutlined data-testid="right-arrow" />}
                    </>
                ))}
            </ParentNodesWrapper>
        </StyledTooltip>
    );
}
