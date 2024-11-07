import { DownOutlined, RightOutlined } from '@ant-design/icons';
import { Typography } from 'antd';
import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';
import RowIcon from '../../../../../../../images/row-icon.svg?react';
import { REDESIGN_COLORS, SEARCH_COLORS } from '../../../../constants';

const Prefix = styled.div<{ padding: number }>`
    position: absolute;
    min-height: 100%;
    margin-bottom: -1px;
    top: -1px;
`;

const IconContainer = styled.div`
    vertical-align: middle;
    display: inline-flex;
    gap: 5px;
`;

const Padding = styled.span<{ padding: number }>`
    margin-left: ${(props) => props.padding}px;
`;

const Down = styled(DownOutlined)<{ $isCompact?: boolean }>`
    :hover {
        color: ${SEARCH_COLORS.TITLE_PURPLE};
        stroke: ${SEARCH_COLORS.TITLE_PURPLE};
        stroke-width: 140px;
    }
    color: ${REDESIGN_COLORS.DARK_GREY};
    stroke: ${REDESIGN_COLORS.DARK_GREY};
    stroke-width: 100px;
    padding-right: 5px;
    ${(props) =>
        props.$isCompact &&
        `
        font-size: 8px;
        
    `}
`;

const Right = styled(RightOutlined)<{ isCompact?: boolean }>`
    :hover {
        stroke: ${SEARCH_COLORS.TITLE_PURPLE};
        color: ${SEARCH_COLORS.TITLE_PURPLE};
        stroke-width: 140px;
    }
    color: ${REDESIGN_COLORS.DARK_GREY};
    stroke: ${REDESIGN_COLORS.DARK_GREY};
    stroke-width: 100px;
    padding-right: 5px;
    ${(props) =>
        props.isCompact &&
        `
        font-size: 8px;
    `}
`;

const RowIconContainer = styled.div`
    position: relative;
    display: flex;
    align-items: center;
`;

const DepthContainer = styled.div<{ multipleDigits?: boolean }>`
    height: ${(props) => (props.multipleDigits ? '20px' : '13px')};
    width: ${(props) => (props.multipleDigits ? '20px' : '13px')};
    border-radius: 50%;
    background: ${REDESIGN_COLORS.PRIMARY_PURPLE};
    margin-left: -7px;
    margin-top: -12px;
    display: flex;
    align-items: center;
`;

const DepthNumber = styled(Typography.Text)`
    margin-left: 4px;
    background: transparent;
    color: ${REDESIGN_COLORS.WHITE};
    font-size: 10px;
    font-weight: 400;
`;

const StyledTooltip = styled(Tooltip)`
    .ant-tooltip-inner {
        border-radius: 3px;
        background: #e5eff1;
        font-size: 10px;
        font-weight: 400;
        line-height: 24px;
        color: ${REDESIGN_COLORS.DARK_GREY};
    }
`;

const DEPTH_PADDING = 15;

type Props = {
    expanded: boolean;
    onExpand: any;
    expandable: boolean;
    record: any;
    isCompact?: boolean;
};

export default function ExpandIcon(props: Props) {
    const { expanded, onExpand, expandable, record, isCompact = false } = props;

    function toggleExpand(e: React.MouseEvent<HTMLSpanElement, MouseEvent>) {
        e.stopPropagation();
        onExpand(record, e);
    }

    return (
        <>
            <IconContainer className="row-icon-container">
                {!isCompact && (
                    <>
                        {Array.from({ length: record.depth }, (_, k) => (
                            <Prefix padding={5 + DEPTH_PADDING * (k + 1)} />
                        ))}
                        <Padding padding={DEPTH_PADDING * (record.depth + 1)} />
                        <StyledTooltip
                            placement="bottom"
                            title={`${record.depth + 1} level${record.depth === 0 ? '' : 's'} nested`}
                            getPopupContainer={(triggerNode) => triggerNode}
                            showArrow={false}
                            className="row-icon-tooltip"
                        >
                            <RowIconContainer className="row-icon">
                                <RowIcon height={16} width={16} />
                                <DepthContainer multipleDigits={record.depth >= 9} className="depth-container">
                                    <DepthNumber className="depth-text">{record.depth + 1}</DepthNumber>
                                </DepthContainer>
                            </RowIconContainer>
                        </StyledTooltip>
                    </>
                )}
                {expandable &&
                    record.children !== undefined &&
                    (expanded ? (
                        <Down onClick={toggleExpand} $isCompact={isCompact} />
                    ) : (
                        <Right onClick={toggleExpand} isCompact={isCompact} />
                    ))}
            </IconContainer>
        </>
    );
}
