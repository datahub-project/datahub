import { Timeline as AntdTimeline } from 'antd';
import styled from 'styled-components';

export const StyledAntdTimeline = styled(AntdTimeline)`
    .ant-timeline-item-head {
        padding: 0;
    }

    .ant-timeline-item-tail {
        border-width: 1px;
        border-color: ${({ theme }) => theme.colors.border};
    }
` as typeof AntdTimeline;
