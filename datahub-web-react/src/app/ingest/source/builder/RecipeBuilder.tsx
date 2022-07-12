import { Button } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { ANTD_GRAY } from '../../../entity/shared/constants';
import { YamlEditor } from './YamlEditor';
import RecipeForm from './RecipeForm';

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

interface Props {
    type: string;
    isEditing: boolean;
    displayRecipe: string;
    setStagedRecipe: (recipe: string) => void;
    onClickNext: () => void;
    goToPrevious?: () => void;
}

function RecipeBuilder(props: Props) {
    const { type, isEditing, displayRecipe, setStagedRecipe, onClickNext, goToPrevious } = props;

    const [isViewingForm, setIsViewingForm] = useState(false);

    function switchViews() {
        setIsViewingForm(!isViewingForm);
    }

    return (
        <div>
            <button type="button" onClick={switchViews}>
                Switch Between YAML
            </button>
            {isViewingForm && (
                <RecipeForm
                    type={type}
                    isEditing={isEditing}
                    displayRecipe={displayRecipe}
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
