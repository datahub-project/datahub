import { createGlobalStyle } from 'styled-components';

import { typography } from '@components/theme';

export const DatePickerGlobalStyles = createGlobalStyle`
    .ant-picker-dropdown {
        font-family: ${typography.fonts.body} !important;
    }
`;
