import React from 'react';
import styled from 'styled-components';

import {
    getAssertionTypesForEntityType,
    useConnectionForEntityExists,
} from '@app/entity/shared/tabs/Dataset/Validations/acrylUtils';
import {
    DEFAULT_DATASET_FRESHNESS_ASSERTION_STATE,
    DEFAULT_DATASET_SQL_ASSERTION_PARAMETERS_STATE,
    DEFAULT_DATASET_SQL_ASSERTION_STATE,
    DEFAULT_DATASET_VOLUME_ASSERTION_STATE,
} from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/constants';
import { AssertionTypeOption } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/AssertionTypeOption';
import {
    getDefaultDatasetFieldAssertionParametersState,
    getDefaultDatasetFieldAssertionState,
    getDefaultDatasetSchemaAssertionParametersState,
    getDefaultDatasetSchemaAssertionState,
} from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/field/utils';
import { getDefaultDatasetVolumeAssertionParametersState } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/volume/utils';
import { AssertionBuilderStep, StepProps } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/types';
import {
    getDefaultDatasetFreshnessAssertionParametersState,
    isEntityEligibleForAssertionMonitoring,
} from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/utils';
import { useAppConfig } from '@app/useAppConfig';

import { AssertionType, EntityType } from '@types';

const Step = styled.div`
    height: 100%;
    display: flex;
    flex-direction: column;
    justify-content: space-between;
    margin-top: 32px;
`;

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 12px;
`;

const TypeListContainer = styled.div`
    display: flex;
    flex-direction: column;
`;

/**
 * Step for selecting the type of assertion
 */
export const SelectTypeStep = ({ state, updateState, goTo }: StepProps) => {
    const connectionForEntityExists = useConnectionForEntityExists(state.entityUrn as string);
    const isConnectionSupportedByMonitors = isEntityEligibleForAssertionMonitoring(state.platformUrn);
    const monitorsConnectionForEntityExists = connectionForEntityExists && isConnectionSupportedByMonitors;
    const appConfig = useAppConfig();
    const isSchemaAssertionEnabled = !!appConfig?.config?.featureFlags?.schemaAssertionMonitorsEnabled;

    const filteredTypes = getAssertionTypesForEntityType(
        state.entityType as EntityType,
        monitorsConnectionForEntityExists,
    )
        .filter((type) => type.visible)
        .filter((type) => type.type !== AssertionType.DataSchema || isSchemaAssertionEnabled);

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
                    monitorsConnectionForEntityExists,
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
                    monitorsConnectionForEntityExists,
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
        } else if (type === AssertionType.DataSchema) {
            newState = {
                ...newState,
                assertion: {
                    type,
                    schemaAssertion: getDefaultDatasetSchemaAssertionState(),
                },
                parameters: getDefaultDatasetSchemaAssertionParametersState(),
            };
        }

        console.log(newState);

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
            case AssertionType.DataSchema:
                goTo(AssertionBuilderStep.CONFIGURE_ASSERTION, AssertionType.DataSchema);
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
        </Step>
    );
};
