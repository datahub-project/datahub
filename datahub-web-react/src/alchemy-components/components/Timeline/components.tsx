import { colors } from '@src/alchemy-components/theme';
import { Timeline as AntdTimeline } from 'antd';
import styled from 'styled-components';

export const StyledAntdTimeline = styled(AntdTimeline)`
    .ant-timeline-item-head {
        padding: 0;
    }

    .ant-timeline-item-tail {
        border-width: 1px;
        border-color: ${colors.gray[100]};
    }
` as typeof AntdTimeline;
