import styled from 'styled-components';
import React from 'react';
import { Button } from 'antd';
import { AssertionTypeOption } from './AssertionTypeOption';
import { AssertionBuilderStep, StepProps } from '../types';
import { getAssertionTypesForEntityType } from '../../../acrylUtils';
import { AssertionType, EntityType } from '../../../../../../../../../types.generated';
import {
    DEFAULT_DATASET_FRESHNESS_ASSERTION_STATE,
    DEFAULT_DATASET_FRESHNESS_ASSERTION_PARAMETERS_STATE,
} from '../constants';

const Step = styled.div`
    height: 100%;
    display: flex;
    flex-direction: column;
    justify-content: space-between;
`;

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 12px;
`;

const TypeListContainer = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
    flex-wrap: wrap;
`;

const CancelButton = styled(Button)`
    && {
        margin-left: 12px;
    }
    max-width: 100px;
`;

/**
 * Step for selecting the type of assertion
 */
export const SelectTypeStep = ({ state, updateState, goTo, cancel }: StepProps) => {
    const filteredTypes = getAssertionTypesForEntityType(state.entityType as EntityType).filter((type) => type.visible);

    const selectAssertionType = (type: AssertionType) => {
        let newState = { ...state };

        // Init the default fields per assertion type.
        if (type === AssertionType.Freshness) {
            newState = {
                ...newState,
                assertion: {
                    type,
                    freshnessAssertion: DEFAULT_DATASET_FRESHNESS_ASSERTION_STATE,
                },
                parameters: DEFAULT_DATASET_FRESHNESS_ASSERTION_PARAMETERS_STATE,
            };
        }

        updateState({
            ...newState,
        });

        switch (type) {
            case AssertionType.Freshness:
                goTo(AssertionBuilderStep.CONFIGURE_DATASET_FRESHNESS_ASSERTION);
                return;
            default:
                // Do nothing.
                console.error(`Attempted to select unsupported assertion type ${type}`);
        }
    };

    return (
        <Step>
            <Section>
                <TypeListContainer>
                    {filteredTypes.map((type) => (
                        <AssertionTypeOption
                            key={type.type}
                            name={type.name}
                            description={type.description}
                            icon={type.icon}
                            enabled={type.enabled}
                            onClick={(type.type && (() => selectAssertionType(type.type))) || (() => null)}
                        />
                    ))}
                </TypeListContainer>
            </Section>
            <Section>
                <CancelButton onClick={cancel}>Cancel</CancelButton>
            </Section>
        </Step>
    );
};
