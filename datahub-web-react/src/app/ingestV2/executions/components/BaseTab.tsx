import React from 'react';
import styled from 'styled-components';

import { TextProps } from '@components/components/Text/types';

import { Heading, Text } from '@src/alchemy-components';

export const SectionBase = styled.div`
    padding: 16px 20px 16px 0;
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
        overflow: auto;
        font-family: 'Roboto Mono', monospace;
        /* Long unwrapped lines (stack traces, YAML) otherwise force this box to its
           content's natural width, which propagates up through every ancestor flex
           container (Tabs, PageLayout) and pushes layout siblings off-screen. This
           forces the box to respect its container's width instead. */
        width: 0;
        min-width: 100%;
        box-sizing: border-box;
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
        scrollbar-color: ${(props) => props.theme.colors.scrollbarThumb} transparent;
    }

    pre:hover::-webkit-scrollbar {
        width: 8px;
    }

    pre::-webkit-scrollbar-track {
        background: ${(props) => props.theme.colors.scrollbarTrack} !important;
        border-radius: 10px;
    }

    pre::-webkit-scrollbar-thumb {
        background: ${(props) => props.theme.colors.scrollbarThumb} !important;
        border-radius: 10px;
    }
`;

type SectionHeadingProps = {
    title: string;
};

export const SectionHeading: React.FC<SectionHeadingProps> = ({ title }) => (
    <Heading type="h4" size="lg" weight="bold">
        {title}
    </Heading>
);

type SectionSecondaryTextProps = {
    title: string;
    color: TextProps['color'];
    colorLevel: TextProps['colorLevel'];
};

export const SectionSecondaryText: React.FC<SectionSecondaryTextProps> = ({ title, color, colorLevel }) => (
    <Text color={color} colorLevel={colorLevel}>
        {title}
    </Text>
);
