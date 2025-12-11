/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import styled from 'styled-components';

import { getHeaderSubtitleStyles, getHeaderTitleStyles } from '@components/components/PageTitle/utils';
import { colors, typography } from '@components/theme';

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
    maxWidth: '100%',

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
