import { Timeline as AntdTimeline } from 'antd';
import styled from 'styled-components';

export const StyledAntdTimeline = styled(AntdTimeline)`
    .ant-timeline-item-head {
        padding: 0;
    }

    /* Antd defaults undecorated dots to its "blue" head, which the global theme maps to the
       informational palette. Brand is the correct default for an alchemy timeline. */
    .ant-timeline-item-head-blue {
        border-color: ${({ theme }) => theme.colors.borderBrand};
        color: ${({ theme }) => theme.colors.iconBrand};
    }

    .ant-timeline-item-tail {
        border-width: 1px;
        border-color: ${({ theme }) => theme.colors.border};
    }
` as typeof AntdTimeline;
