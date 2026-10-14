import { Tooltip } from '@components';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import { Rows } from '@phosphor-icons/react/dist/csr/Rows';
import { Typography } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

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

const Down = styled(CaretDown).attrs<{ $isCompact?: boolean }>(({ $isCompact }) => ({
    size: $isCompact ? 8 : 14,
    weight: 'bold' as const,
}))<{ $isCompact?: boolean }>`
    :hover {
        color: ${(props) => props.theme.colors.textHover};
    }
    color: ${(props) => props.theme.colors.textSecondary};
    padding-right: 5px;
    cursor: pointer;
`;

const Right = styled(CaretRight).attrs<{ isCompact?: boolean }>(({ isCompact }) => ({
    size: isCompact ? 8 : 14,
    weight: 'bold' as const,
}))<{ isCompact?: boolean }>`
    :hover {
        color: ${(props) => props.theme.colors.textHover};
    }
    color: ${(props) => props.theme.colors.textSecondary};
    padding-right: 5px;
    cursor: pointer;
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
    background: ${(props) => props.theme.colors.bgSurfaceBrand};
    margin-left: -7px;
    margin-top: -12px;
    display: flex;
    align-items: center;
`;

const DepthNumber = styled(Typography.Text)`
    margin-left: 4px;
    background: transparent;
    color: ${(props) => props.theme.colors.textOnFillDefault};
    font-size: 10px;
    font-weight: 400;
`;

const StyledTooltip = styled(Tooltip)`
    .alchemy-floating-overlay-inner {
        border-radius: 3px;
        background: ${(props) => props.theme.colors.bgSurface};
        font-size: 10px;
        font-weight: 400;
        line-height: 24px;
        color: ${(props) => props.theme.colors.textSecondary};
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
    const { t } = useTranslation('entity.profile.schema');
    const { expanded, onExpand, expandable, record, isCompact = false } = props;

    function toggleExpand(e: React.MouseEvent) {
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
                            title={t('expandIcon.levelsNested', { count: record.depth + 1 })}
                            getPopupContainer={(triggerNode) => triggerNode}
                            showArrow={false}
                            className="row-icon-tooltip"
                        >
                            <RowIconContainer className="row-icon">
                                <Rows size={16} />
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
                        <Down onClick={toggleExpand} $isCompact={isCompact} data-testid="schema-expand-icon-down" />
                    ) : (
                        <Right onClick={toggleExpand} isCompact={isCompact} data-testid="schema-expand-icon-right" />
                    ))}
            </IconContainer>
        </>
    );
}
