import { DownloadOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';
import YAML from 'yamljs';

import { SectionBase, SectionHeader } from '@app/ingestV2/executions/components/BaseTab';
import { downloadFile } from '@app/search/utils/csvUtils';
import { Button, Text, Tooltip } from '@src/alchemy-components';
import colors from '@src/alchemy-components/theme/foundations/colors';

import { GetIngestionExecutionRequestQuery } from '@graphql/ingestion.generated';

const SectionSubHeader = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const SubHeaderParagraph = styled(Text)`
    margin-bottom: 0px;
`;

const RecipeSection = styled(SectionBase)`
    border-top: 1px solid ${colors.gray[1400]};
`;

const DetailsContainer = styled.div`
    margin-top: 12px;

    pre {
        background-color: ${colors.gray[1500]};
        border: 1px solid ${colors.gray[1400]};
        border-radius: 8px;
        padding: 16px;
        margin: 0;
        color: ${colors.gray[1700]};
        overflow-y: auto;
    }
`;

export const RecipeTab = ({ urn, data }: { urn: string; data: GetIngestionExecutionRequestQuery | undefined }) => {
    const recipeJson = data?.executionRequest?.input?.arguments?.find((arg) => arg.key === 'recipe')?.value;
    let recipeYaml: string;
    try {
        recipeYaml = recipeJson && YAML.stringify(JSON.parse(recipeJson), 8, 2).trim();
    } catch (e) {
        recipeYaml = '';
    }

    const downloadRecipe = () => {
        downloadFile(recipeYaml, `recipe-${urn}.yaml`);
    };

    return (
        <RecipeSection>
            <SectionHeader level={5}>Recipe</SectionHeader>
            <SectionSubHeader>
                <SubHeaderParagraph color="gray" colorLevel={600}>
                    The configurations used for this sync with the data source.
                </SubHeaderParagraph>
                <Tooltip title="Download Recipe">
                    <Button variant="text" onClick={downloadRecipe}>
                        <DownloadOutlined />
                    </Button>
                </Tooltip>
            </SectionSubHeader>
            <DetailsContainer>
                <Text size="sm">
                    <pre>{recipeYaml || 'No recipe found.'}</pre>
                </Text>
            </DetailsContainer>
        </RecipeSection>
    );
};
