import { Typography } from 'antd';
import styled from 'styled-components';


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
        background-color: ${(props) => props.theme.colors.bgSurface};
        border: 1px solid ${(props) => props.theme.colors.border};
        border-radius: 8px;
        padding: 16px;
        margin: 0;
        color: ${(props) => props.theme.colors.textSecondary};
        overflow-y: auto;
    }
`;

export const ScrollableDetailsContainer = styled(DetailsContainer)`
    pre {
        max-height: 300px;
        overflow-y: auto;

        scrollbar-width: none;
    }

    pre::-webkit-scrollbar {
        width: 0;
    }

    pre:hover {
        scrollbar-width: thin;
        scrollbar-color: rgba(193, 196, 208, 0.8) transparent;
    }

    pre:hover::-webkit-scrollbar {
        width: 8px;
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
