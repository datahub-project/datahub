import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import YAML from 'yamljs';

import { SectionBase, SectionHeader } from '@app/ingestV2/executions/components/BaseTab';
import colors from '@src/alchemy-components/theme/foundations/colors';

import { GetIngestionExecutionRequestQuery } from '@graphql/ingestion.generated';

const SectionSubHeader = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const SubHeaderParagraph = styled(Typography.Paragraph)`
    margin-bottom: 0px;
`;

const RecipeSection = styled(SectionBase)`
    border-top: 1px solid ${colors.gray[1400]};
`;

export const RecipeTab = ({ data }: { data: GetIngestionExecutionRequestQuery | undefined }) => {
    const recipeJson = data?.executionRequest?.input?.arguments?.find((arg) => arg.key === 'recipe')?.value;
    let recipeYaml: string;
    try {
        recipeYaml = recipeJson && YAML.stringify(JSON.parse(recipeJson), 8, 2).trim();
    } catch (e) {
        recipeYaml = '';
    }

    return (
        <RecipeSection>
            <SectionHeader level={5}>Recipe</SectionHeader>
            <SectionSubHeader>
                <SubHeaderParagraph type="secondary">
                    The configurations used for this sync with the data source.
                </SubHeaderParagraph>
            </SectionSubHeader>
            <Typography.Paragraph ellipsis>
                <pre>{recipeYaml || 'No recipe found.'}</pre>
            </Typography.Paragraph>
        </RecipeSection>
    );
};
