import React from 'react';
import styled from 'styled-components';

import {
    getAssertionTypesForEntityType,
    useConnectionForEntityExists,
} from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { AssertionTypeOption } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/AssertionTypeOption';
import getInitBuilderStateByAssertionType from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/utils';
import { AssertionBuilderStep, StepProps } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';
import { isEntityEligibleForAssertionMonitoring } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/utils';
import { useAppConfig } from '@src/app/useAppConfig';
import { AssertionType, EntityType } from '@src/types.generated';

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
        // Init the default fields per assertion type.
        const newState = getInitBuilderStateByAssertionType(
            state,
            type,
            connectionForEntityExists,
            monitorsConnectionForEntityExists,
        );
        updateState({ ...newState });

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
