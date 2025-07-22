import { Typography } from 'antd';
import styled from 'styled-components';

export const SectionBase = styled.div`
    padding: 16px 30px 0;
`;

export const SectionHeader = styled(Typography.Title)`
    &&&& {
        padding: 0px;
        margin: 0px;
        margin-bottom: 12px;
    }
`;
