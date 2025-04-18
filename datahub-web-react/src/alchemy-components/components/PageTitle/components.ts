import styled from 'styled-components';
import { typography, colors } from '@components/theme';
import { getHeaderSubtitleStyles, getHeaderTitleStyles } from './utils';

// Text Styles
const titleStyles = {
    display: 'flex',
    alignItems: 'center',
    gap: 8,
    fontWeight: typography.fontWeights.bold,
    color: colors.gray[600],
};

const subTitleStyles = {
    fontWeight: typography.fontWeights.normal,
    color: colors.gray[1700],
};

// Default styles
const baseStyles = {
    fontFamily: typography.fonts.body,
    margin: 0,

    '& a': {
        color: colors.violet[400],
        textDecoration: 'none',
        transition: 'color 0.15s ease',

        '&:hover': {
            color: colors.violet[500],
        },
    },
};

export const Container = styled.div`
    display: flex;
    flex-direction: column;
    align-items: start;
    justify-content: start;
`;

export const Title = styled.div<{ variant: string }>(({ variant }) => ({
    ...baseStyles,
    ...titleStyles,
    ...getHeaderTitleStyles(variant),
}));

export const SubTitle = styled.div<{ variant: string }>(({ variant }) => ({
    ...baseStyles,
    ...subTitleStyles,
    ...getHeaderSubtitleStyles(variant),
}));
