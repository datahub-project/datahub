import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { IngestionSource } from '../../types.generated';

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 12px;
`;

const SectionHeader = styled(Typography.Text)`
    &&&& {
        padding: 0px;
        margin: 0px;
        margin-bottom: 4px;
    }
`;

const SectionParagraph = styled(Typography.Paragraph)`
    &&&& {
        padding: 0px;
        margin: 0px;
    }
`;

type Props = {
    source: IngestionSource;
};

export const RecipeSourceDetails = ({ source }: Props) => {
    const recipeJson = source.config.recipe;
    return (
        <>
            <Section>
                <SectionHeader strong>Recipe</SectionHeader>
                <SectionParagraph>{recipeJson}</SectionParagraph>
            </Section>
        </>
    );
};
