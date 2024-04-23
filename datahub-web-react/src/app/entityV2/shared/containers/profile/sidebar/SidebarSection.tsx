import React from 'react';
import styled from 'styled-components';
import { Collapse } from 'antd';
import { KeyboardArrowDown, KeyboardArrowRight } from '@mui/icons-material';

import { REDESIGN_COLORS } from '../../../constants';
import { CountStyle } from '../../../SidebarStyledComponents';

const Container = styled.div`
    margin-left: 7px;
    width: 100%;
`;

const StyledCollapse = styled(Collapse)`
    .ant-collapse-header {
        padding: 0px 0px !important;
        align-items: center;
    }

    .ant-collapse-content-box {
        padding-top: 4px !important;
        padding-bottom: 0px !important;
    }

    .ant-collapse-arrow {
        margin-right: 5px !important;
        line-height: 32px;
    }

    .ant-collapse-expand-icon {
        height: 22px;
    }

    .ant-collapse-item-disabled > .ant-collapse-header {
        cursor: default;

        > .ant-collapse-extra {
            cursor: pointer;
        }
    }
`;

const SectionHeader = styled.div<{ collapsible?: boolean }>`
    display: flex;
    align-items: center;
    color: ${REDESIGN_COLORS.DARK_GREY};
    font-weight: 700;
    line-height: 20px;
    font-size: 14px;
    ${(props) => !props.collapsible && 'margin-left: 8px;'}
`;

const StyledIcon = styled.div`
    svg {
        height: 18px;
        width: 18px;
        color: ${REDESIGN_COLORS.SECONDARY_LIGHT_GREY};
        stroke: ${REDESIGN_COLORS.SECONDARY_LIGHT_GREY};
        stroke-width: 1px;
    }
`;

type Props = {
    title: string;
    content: React.ReactNode;
    extra?: React.ReactNode;
    count?: number;
    collapsedContent?: React.ReactNode;
    collapsible?: boolean;
    expandedByDefault?: boolean;
};

export const SidebarSection = ({
    title,
    content,
    extra,
    count = 0,
    collapsedContent,
    collapsible = true,
    expandedByDefault = true,
}: Props) => {
    return (
        <StyledCollapse
            ghost
            expandIcon={({ isActive }) => (
                <StyledIcon>{isActive ? <KeyboardArrowDown /> : <KeyboardArrowRight />} </StyledIcon>
            )}
            defaultActiveKey={expandedByDefault ? title : ''}
        >
            <Collapse.Panel
                header={
                    <>
                        <SectionHeader collapsible={collapsible}>
                            {title} {count > 0 && <CountStyle> {count > 10 ? '10+' : count}</CountStyle>}
                        </SectionHeader>
                        {collapsedContent}
                    </>
                }
                key={title}
                extra={extra}
                collapsible={!collapsible ? 'disabled' : undefined}
                showArrow={collapsible}
            >
                <Container>{content}</Container>
            </Collapse.Panel>
        </StyledCollapse>
    );
};
