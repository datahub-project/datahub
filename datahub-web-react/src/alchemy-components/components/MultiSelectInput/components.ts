import styled from 'styled-components';

import { Button } from '@components/components/Button';
import { inputPlaceholderTextStyles, inputValueTextStyles } from '@components/components/commonStyles';
import { spacing } from '@components/theme';

export const InputWrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: ${spacing.xxsm};
`;

export const Label = styled.label(
    ({ theme }) => `
    font-size: 14px;
    font-weight: 600;
    color: ${theme.colors.text};
`,
);

export const InputContainer = styled.div<{ isInvalid?: boolean; $width?: string | number }>(
    ({ isInvalid, theme, $width }) => `
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: ${spacing.xsm};
    padding-left: ${spacing.sm};
    padding-top: ${spacing.xsm};
    padding-bottom: ${spacing.xsm};
    padding-right: calc(${spacing.md} + 32px);
    width: ${typeof $width === 'number' ? `${$width}px` : $width};
    min-height: 36px;
    border: 1px solid ${isInvalid ? theme.colors.borderError : theme.colors.borderInput};
    border-radius: 8px;
    background-color: ${theme.colors.bg};
    box-shadow: ${theme.colors.shadowXs};
    outline: none;
    transition: border-color 0.15s ease, background-color 0.15s ease;
    position: relative;

    &:focus-within {
        border-color: ${isInvalid ? theme.colors.borderError : theme.colors.borderBrandFocused};
        outline: 1px solid ${isInvalid ? theme.colors.borderError : theme.colors.borderBrandFocused};
    }
`,
);

export const TagsWrapper = styled.div`
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: ${spacing.xsm};
    width: 100%;
    min-width: 0;
`;

export const NativeInput = styled.input(
    ({ theme }) => `
    border: none;
        font-size: 14px;

    background-color: transparent;
    flex: 1 1 10%;
    min-width: 40px;
    padding: 0;
    outline: none;
    box-shadow: none;
    color: ${theme.colors.text};
    vertical-align: middle;
    line-height: 1.5;
    ${inputValueTextStyles()}

    &::placeholder {
        ${inputPlaceholderTextStyles}
        color: ${theme.colors.textPlaceholder};
    }

    &:focus {
        outline: none;
        box-shadow: none;
    }

    &:disabled {
        color: ${theme.colors.textDisabled};
        cursor: not-allowed;
    }
`,
);

export const ErrorMessage = styled.span(
    ({ theme }) => `
    font-size: 12px;
    color: ${theme.colors.iconError};
`,
);

export const HelperText = styled.span(
    ({ theme }) => `
    font-size: 12px;
    color: ${theme.colors.textSecondary};
`,
);

export const ClearButton = styled(Button)`
    position: absolute;
    right: ${spacing.sm};
    top: 50%;
    transform: translateY(-50%);
    margin-left: ${spacing.md};
`;
