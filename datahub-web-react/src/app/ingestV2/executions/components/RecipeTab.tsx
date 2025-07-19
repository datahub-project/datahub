import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import YAML from 'yamljs';

import { ANTD_GRAY } from '@app/entity/shared/constants';

import { GetIngestionExecutionRequestQuery } from '@graphql/ingestion.generated';

const SectionHeader = styled(Typography.Title)`
    &&&& {
        padding: 0px;
        margin: 0px;
        margin-bottom: 12px;
    }
`;

const SectionSubHeader = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const SubHeaderParagraph = styled(Typography.Paragraph)`
    margin-bottom: 0px;
`;

const RecipeSection = styled.div`
    border-top: 1px solid ${ANTD_GRAY[4]};
    padding-top: 16px;
    padding-left: 30px;
    padding-right: 30px;
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
