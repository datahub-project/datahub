import { Typography } from 'antd';
import styled from 'styled-components';

import colors from '@src/alchemy-components/theme/foundations/colors';

export const SectionBase = styled.div`
    padding: 16px 20px 16px 0;
`;

export const SectionHeader = styled(Typography.Title)`
    &&&& {
        padding: 0px;
        margin: 0px;
    }
`;

export const DetailsContainer = styled.div`
    margin-top: 12px;

    pre {
        background-color: ${colors.gray[1500]};
        border: 1px solid ${colors.gray[1400]};
        border-radius: 8px;
        padding: 16px;
        margin: 0;
        color: ${colors.gray[1700]};
        overflow-y: auto;
    }
`;

export const ScrollableDetailsContainer = styled(DetailsContainer)`
    pre {
        max-height: 300px;
        overflow-y: scroll;
    }

    pre::-webkit-scrollbar-track {
        background: rgba(193, 196, 208, 0.3) !important;
        border-radius: 10px;
    }

    pre::-webkit-scrollbar-thumb {
        background: rgba(193, 196, 208, 0.8) !important;
        border-radius: 10px;
    }
`;
