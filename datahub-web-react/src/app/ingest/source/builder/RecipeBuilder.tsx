import React, { useState } from 'react';
import { Button, message, Typography } from 'antd';
import YAML from 'yamljs';
import { CodeOutlined, FormOutlined } from '@ant-design/icons';
import styled from 'styled-components/macro';
import { ANTD_GRAY } from '../../../entity/shared/constants';
import { YamlEditor } from './YamlEditor';
import RecipeForm from './RecipeForm/RecipeForm';
import { SourceBuilderState, SourceConfig } from './types';
import { LOOKER, LOOK_ML } from './constants';
import { LookerWarning } from './LookerWarning';

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

const StyledButton = styled(Button)<{ isSelected: boolean }>`
    ${(props) =>
        props.isSelected &&
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
            {(type === LOOKER || type === LOOK_ML) && <LookerWarning type={type} />}
            <HeaderContainer>
                <Title style={{ marginBottom: 0 }} level={5}>
                    {sourceConfigs?.displayName} Recipe
                </Title>
                <ButtonsWrapper>
                    <StyledButton type="text" isSelected={isViewingForm} onClick={() => switchViews(true)}>
                        <FormOutlined /> Form
                    </StyledButton>
                    <StyledButton type="text" isSelected={!isViewingForm} onClick={() => switchViews(false)}>
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
                        <Button disabled={isEditing} onClick={goToPrevious}>
                            Previous
                        </Button>
                        <Button onClick={onClickNext}>Next</Button>
                    </ControlsContainer>
                </>
            )}
        </div>
    );
}

export default RecipeBuilder;
