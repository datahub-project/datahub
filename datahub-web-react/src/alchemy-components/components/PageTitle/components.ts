import styled from 'styled-components';

import { getHeaderSubtitleStyles, getHeaderTitleStyles } from '@components/components/PageTitle/utils';
import { typography } from '@components/theme';

const titleStyles = {
    display: 'flex',
    alignItems: 'center',
    gap: 8,
    fontWeight: typography.fontWeights.bold,
};

const subTitleStyles = {
    fontWeight: typography.fontWeights.normal,
};

const baseStyles = {
    fontFamily: typography.fonts.body,
    margin: 0,
    maxWidth: '100%',
};

export const Container = styled.div`
    display: flex;
    flex-direction: column;
    align-items: start;
    justify-content: start;
`;

export const Title = styled.div<{ variant: string }>(({ variant, theme }) => ({
    ...baseStyles,
    ...titleStyles,
    color: theme.colors.text,
    ...getHeaderTitleStyles(variant),
    '& a': {
        color: theme.colors.hyperlinks,
        textDecoration: 'none',
        transition: 'color 0.15s ease',
        '&:hover': {
            color: theme.colors.textBrand,
        },
    },
}));

export const SubTitle = styled.div<{ variant: string }>(({ variant, theme }) => ({
    ...baseStyles,
    ...subTitleStyles,
    color: theme.colors.textSecondary,
    ...getHeaderSubtitleStyles(variant),
    '& a': {
        color: theme.colors.hyperlinks,
        textDecoration: 'none',
        transition: 'color 0.15s ease',
        '&:hover': {
            color: theme.colors.textBrand,
        },
    },
}));
