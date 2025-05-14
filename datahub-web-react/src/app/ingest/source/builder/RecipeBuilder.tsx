import { CodeOutlined, FormOutlined } from '@ant-design/icons';
import { Typography, message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components/macro';
import YAML from 'yamljs';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { CSVInfo } from '@app/ingest/source/builder/CSVInfo';
import { IngestionDocumentationHint } from '@app/ingest/source/builder/IngestionDocumentationHint';
import { LookerWarning } from '@app/ingest/source/builder/LookerWarning';
import RecipeForm from '@app/ingest/source/builder/RecipeForm/RecipeForm';
import { YamlEditor } from '@app/ingest/source/builder/YamlEditor';
import { CSV, LOOKER, LOOK_ML } from '@app/ingest/source/builder/constants';
import { SourceBuilderState, SourceConfig } from '@app/ingest/source/builder/types';
import { Button } from '@src/alchemy-components';

export const ControlsContainer = styled.div`
    display: flex;
    justify-content: space-between;
    margin-top: 8px;
`;

const BorderedSection = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 16px;
    border: solid ${ANTD_GRAY[4]} 0.5px;
`;

const StyledButton = styled(Button)<{ $isSelected: boolean }>`
    ${(props) =>
        props.$isSelected &&
        `
        color: #1890ff;
        &:focus {
            color: #1890ff;
        }    
    `}
`;

const Title = styled(Typography.Title)`
    display: flex;
    align-items: center;
    justify-content: flex-start;
`;

const ButtonsWrapper = styled.div`
    display: flex;
    justify-content: flex-end;
    gap: 16px;
`;

const HeaderContainer = styled.div`
    display: flex;
    justify-content: space-between;
    margin-bottom: 10px;
`;

interface Props {
    state: SourceBuilderState;
    isEditing: boolean;
    displayRecipe: string;
    sourceConfigs?: SourceConfig;
    setStagedRecipe: (recipe: string) => void;
    onClickNext: () => void;
    goToPrevious?: () => void;
}

function RecipeBuilder(props: Props) {
    const { state, isEditing, displayRecipe, sourceConfigs, setStagedRecipe, onClickNext, goToPrevious } = props;
    const { type } = state;
    const [isViewingForm, setIsViewingForm] = useState(true);
    const [hideDocsHint, setHideDocsHint] = useState(false);

    function switchViews(isFormView: boolean) {
        try {
            YAML.parse(displayRecipe);
            setIsViewingForm(isFormView);
        } catch (e) {
            const messageText = (e as any).parsedLine
                ? `Fix line ${(e as any).parsedLine} in your recipe`
                : 'Please fix your recipe';
            message.warn(`Found invalid YAML. ${messageText} in order to switch views.`);
        }
    }

    return (
        <div>
            {!hideDocsHint && isViewingForm && sourceConfigs ? (
                <IngestionDocumentationHint onHide={() => setHideDocsHint(true)} sourceConfigs={sourceConfigs} />
            ) : null}
            {(type === LOOKER || type === LOOK_ML) && <LookerWarning type={type} />}
            {type === CSV && <CSVInfo />}
            <HeaderContainer>
                <Title style={{ marginBottom: 0 }} level={5}>
                    {sourceConfigs?.displayName} Details
                </Title>
                <ButtonsWrapper>
                    <StyledButton
                        variant="text"
                        color="gray"
                        $isSelected={isViewingForm}
                        onClick={() => switchViews(true)}
                        data-testid="recipe-builder-form-button"
                    >
                        <FormOutlined /> Form
                    </StyledButton>
                    <StyledButton
                        variant="text"
                        color="gray"
                        $isSelected={!isViewingForm}
                        onClick={() => switchViews(false)}
                        data-testid="recipe-builder-yaml-button"
                    >
                        <CodeOutlined /> YAML
                    </StyledButton>
                </ButtonsWrapper>
            </HeaderContainer>
            {isViewingForm && (
                <RecipeForm
                    state={state}
                    isEditing={isEditing}
                    displayRecipe={displayRecipe}
                    sourceConfigs={sourceConfigs}
                    setStagedRecipe={setStagedRecipe}
                    onClickNext={onClickNext}
                    goToPrevious={goToPrevious}
                />
            )}
            {!isViewingForm && (
                <>
                    <BorderedSection>
                        <YamlEditor initialText={displayRecipe} onChange={setStagedRecipe} />
                    </BorderedSection>
                    <ControlsContainer>
                        <Button variant="outline" color="gray" disabled={isEditing} onClick={goToPrevious}>
                            Previous
                        </Button>
                        <Button data-testid="recipe-builder-next-button" onClick={onClickNext}>
                            Next
                        </Button>
                    </ControlsContainer>
                </>
            )}
        </div>
    );
}

export default RecipeBuilder;
