import { borders } from '@components';
import styled from 'styled-components';

import { ModuleHeader } from '@app/homeV3/module/components/LargeModule';

export const SummaryModuleHeader = styled(ModuleHeader)`
    &:hover {
        background: transparent;
        border-bottom: ${borders['1px']} ${(props) => props.theme.colors.bg};
    }
`;

export const SummaryModuleContent = styled.div<{ $hasFooter?: boolean }>`
    display: flex;
    flex-direction: column;
    margin: 0 0 8px 8px;
    padding-right: 5px;
    overflow-y: auto;
    scrollbar-gutter: stable;
    height: ${(props) => (props.$hasFooter ? '234px' : '246px')};

    &::-webkit-scrollbar {
        width: 6px;
    }
    &::-webkit-scrollbar-thumb {
        background: ${(props) => props.theme.colors.scrollbarThumb};
        border-radius: 3px;
    }
    scrollbar-width: thin;
    scrollbar-color: ${(props) => props.theme.colors.scrollbarThumb} transparent;
`;
