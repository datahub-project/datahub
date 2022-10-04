import { Button, Row, Col, message, Typography, Alert } from 'antd';
import React, { useState } from 'react';
import YAML from 'yamljs';
import { CodeOutlined, FormOutlined } from '@ant-design/icons';
import styled from 'styled-components/macro';
import { ANTD_GRAY } from '../../../entity/shared/constants';
import { YamlEditor } from './YamlEditor';
import RecipeForm from './RecipeForm/RecipeForm';
import { SourceBuilderState, SourceConfig } from './types';

const LOOKML_DOC_LINK = 'https://datahubproject.io/docs/generated/ingestion/sources/looker#module-lookml';
const LOOKER_DOC_LINK = 'https://datahubproject.io/docs/generated/ingestion/sources/looker#module-looker';

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

    let link: React.ReactNode;
    if (type === 'looker') {
        link = (
            <a href={LOOKER_DOC_LINK} target="_blank" rel="noopener noreferrer">
                DataHub looker module
            </a>
        );
    } else if (type === 'lookml') {
        link = (
            <a href={LOOKML_DOC_LINK} target="_blank" rel="noopener noreferrer">
                DataHub lookml module
            </a>
        );
    }

    return (
        <div>
            {(type === 'looker' || type === 'lookml') && (
                <Alert
                    type="warning"
                    banner
                    message={
                        <>
                            <big>
                                <i>
                                    <b>You must acknowledge this message to proceed!</b>
                                </i>
                            </big>
                            <br />
                            <br />
                            To get complete Looker metadata integration (including Looker views and lineage to the
                            underlying warehouse tables), you must <b>also</b> use the {link}.
                        </>
                    }
                />
            )}
            <Row style={{ marginBottom: '10px' }}>
                <Col xl={8} lg={8} md={8} sm={24} xs={24}>
                    <Title level={5}>{sourceConfigs?.displayName} Recipe</Title>
                </Col>
                <Col xl={16} lg={16} md={16} sm={24} xs={24}>
                    <ButtonsWrapper>
                        <StyledButton type="text" isSelected={isViewingForm} onClick={() => switchViews(true)}>
                            <FormOutlined /> Form
                        </StyledButton>
                        <StyledButton type="text" isSelected={!isViewingForm} onClick={() => switchViews(false)}>
                            <CodeOutlined /> YAML
                        </StyledButton>
                    </ButtonsWrapper>
                </Col>
            </Row>
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
