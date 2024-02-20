import React from 'react';
import styled from 'styled-components';
import { Collapse } from 'antd';
import { KeyboardArrowDown, KeyboardArrowRight } from '@mui/icons-material';

import { REDESIGN_COLORS } from '../../../constants';

const Container = styled.div`
    display: flex;
`;

const Content = styled.div`
    flex: 1;
    margin-left: 7px;
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
    }
    .ant-collapse-expand-icon {
        height: 22px;
    }
`;

const SectionHeader = styled.div`
    display: flex;
    align-items: center;
    color: ${REDESIGN_COLORS.DARK_GREY};
    font-weight: 700;
    line-height: 20px;
    font-size: 14px;
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
};

export const SidebarSection = ({ title, content, extra }: Props) => {
    return (
        <StyledCollapse
            ghost
            expandIcon={({ isActive }) => (
                <StyledIcon>{isActive ? <KeyboardArrowDown /> : <KeyboardArrowRight />} </StyledIcon>
            )}
            defaultActiveKey={title}
        >
            <Collapse.Panel header={<SectionHeader>{title}</SectionHeader>} key={title} extra={extra}>
                <Container>
                    <Content>{content}</Content>
                </Container>
            </Collapse.Panel>
        </StyledCollapse>
    );
};
