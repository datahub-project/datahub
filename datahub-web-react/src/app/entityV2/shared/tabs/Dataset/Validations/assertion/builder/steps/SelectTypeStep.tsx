import styled from 'styled-components';
import React from 'react';
import { Button } from 'antd';
import { AssertionTypeOption } from './AssertionTypeOption';
import { AssertionBuilderStep, StepProps } from '../types';
import { getAssertionTypesForEntityType, useConnectionForEntityExists } from '../../../acrylUtils';
import { AssertionType, EntityType } from '../../../../../../../../../types.generated';
import {
    DEFAULT_DATASET_FRESHNESS_ASSERTION_STATE,
    DEFAULT_DATASET_SQL_ASSERTION_PARAMETERS_STATE,
    DEFAULT_DATASET_SQL_ASSERTION_STATE,
    DEFAULT_DATASET_VOLUME_ASSERTION_STATE,
} from '../constants';
import { getDefaultDatasetFreshnessAssertionParametersState } from '../utils';
import { getDefaultDatasetVolumeAssertionParametersState } from './volume/utils';
import { getDefaultDatasetFieldAssertionParametersState, getDefaultDatasetFieldAssertionState } from './field/utils';

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
    const connectionForEntityExists = useConnectionForEntityExists(state.entityUrn as string);
    const filteredTypes = getAssertionTypesForEntityType(
        state.entityType as EntityType,
        connectionForEntityExists,
    ).filter((type) => type.visible);

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
                parameters: getDefaultDatasetFreshnessAssertionParametersState(
                    state.platformUrn as string,
                    connectionForEntityExists,
                ),
            };
        } else if (type === AssertionType.Volume) {
            newState = {
                ...newState,
                assertion: {
                    type,
                    volumeAssertion: DEFAULT_DATASET_VOLUME_ASSERTION_STATE,
                },
                parameters: getDefaultDatasetVolumeAssertionParametersState(
                    state.platformUrn as string,
                    connectionForEntityExists,
                ),
            };
        } else if (type === AssertionType.Sql) {
            newState = {
                ...newState,
                assertion: {
                    type,
                    sqlAssertion: DEFAULT_DATASET_SQL_ASSERTION_STATE,
                },
                parameters: DEFAULT_DATASET_SQL_ASSERTION_PARAMETERS_STATE,
            };
        } else if (type === AssertionType.Field) {
            newState = {
                ...newState,
                assertion: {
                    type,
                    fieldAssertion: getDefaultDatasetFieldAssertionState(connectionForEntityExists),
                },
                parameters: getDefaultDatasetFieldAssertionParametersState(connectionForEntityExists),
            };
        }

        updateState({
            ...newState,
        });

        switch (type) {
            case AssertionType.Freshness:
                goTo(AssertionBuilderStep.CONFIGURE_ASSERTION, AssertionType.Freshness);
                return;
            case AssertionType.Volume:
                goTo(AssertionBuilderStep.CONFIGURE_ASSERTION, AssertionType.Volume);
                return;
            case AssertionType.Sql:
                goTo(AssertionBuilderStep.CONFIGURE_ASSERTION, AssertionType.Sql);
                return;
            case AssertionType.Field:
                goTo(AssertionBuilderStep.CONFIGURE_ASSERTION, AssertionType.Field);
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
