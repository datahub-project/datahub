import { InfoCircleOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import { Typography } from 'antd';
import React, { useMemo } from 'react';
import styled from 'styled-components';

import { EntityCapabilityType } from '@app/entity/Entity';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import { LogicalPredicateBuilder } from '@app/tests/builder/steps/definition/builder/LogicalPredicateBuilder';
import { EntityTypeSelect } from '@app/tests/builder/steps/definition/builder/property/input/EntityTypeSelect';
import { getPropertiesForEntityTypes } from '@app/tests/builder/steps/definition/builder/property/utils';
import {
    convertLogicalPredicateToTestPredicate,
    convertTestPredicateToLogicalPredicate,
} from '@app/tests/builder/steps/definition/builder/utils';
import { deserializeTestDefinition, serializeTestDefinition } from '@app/tests/builder/steps/definition/utils';
import { YamlStep } from '@app/tests/builder/steps/definition/yaml/YamlStep';
import { entityTypesToGraphNames, graphNamesToEntityTypes } from '@app/tests/builder/steps/select/utils';
import { StepProps, TestBuilderStep } from '@app/tests/builder/types';
import { ValidationWarning } from '@app/tests/builder/validation/ValidationWarning';
import { validateCompleteTestDefinition } from '@app/tests/builder/validation/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

const Section = styled.div`
    margin-top: 20px;
    margin-bottom: 10px;
`;

const BuilderWrapper = styled.div`
    margin-bottom: 28px;
    margin-top: 12px;
`;

const SubTitle = styled(Typography.Paragraph)`
    font-size: 16px;
`;

const AdditionalFilters = styled.div`
    font-size: 14px;
    margin-bottom: 8px;
    margin-top: 12px;
    display: flex;
    align-items: center;
    justify-content: left;
`;

const AdditionalFiltersTitle = styled.div`
    margin-right: 4px;
`;

const StyledInfoOutlined = styled(InfoCircleOutlined)`
    margin-left: 4px;
    font-size: 12px;
    color: ${ANTD_GRAY[7]};
`;

export const SelectStep = ({ state, updateState, goTo }: StepProps) => {
    const entityRegistry = useEntityRegistry();
    const testDefinition = useMemo(() => deserializeTestDefinition(state?.definition?.json || '{}'), [state]);

    const onClickNext = () => {
        goTo(TestBuilderStep.RULES);
    };

    const onChangeTypes = (newTypes) => {
        const newDefinition = {
            ...testDefinition,
            on: {
                types: entityTypesToGraphNames(newTypes, entityRegistry),
                conditions: testDefinition.on?.conditions,
            },
        };
        const newState = {
            ...state,
            definition: {
                json: serializeTestDefinition(newDefinition),
            },
        };
        updateState(newState);
    };

    const onChangePredicate = (newPredicate) => {
        const newDefinition = {
            ...testDefinition,
            on: {
                types: testDefinition.on?.types || [],
                conditions: convertLogicalPredicateToTestPredicate(newPredicate),
            },
        };
        const newState = {
            ...state,
            definition: {
                json: serializeTestDefinition(newDefinition),
            },
        };
        updateState(newState);
    };

    const onResetConditions = () => {
        const newDefinition = {
            ...testDefinition,
            on: {
                types: testDefinition.on?.types || [],
                conditions: [], // Clear all conditions
            },
        };
        const newState = {
            ...state,
            definition: {
                json: serializeTestDefinition(newDefinition),
            },
        };
        updateState(newState);
    };

    const testEntities: EntityType[] = Array.from(
        entityRegistry.getTypesWithSupportedCapabilities(EntityCapabilityType.TEST),
    );
    const selectedEntityTypes = graphNamesToEntityTypes(testDefinition.on?.types || [], entityRegistry);

    // Get validation warnings for current configuration (memoized for proper re-evaluation)
    const validationWarnings = useMemo(() => {
        return validateCompleteTestDefinition(selectedEntityTypes, testDefinition);
    }, [selectedEntityTypes, testDefinition]);

    return (
        <>
            <YamlStep
                state={state}
                updateState={updateState}
                onNext={onClickNext}
                nextDisabled={!selectedEntityTypes?.length}
            >
                <Typography.Title level={4}>Select your data assets</Typography.Title>
                <SubTitle type="secondary">Which data assets do you want to test?</SubTitle>
                <EntityTypeSelect
                    selectedTypes={selectedEntityTypes}
                    entityTypes={testEntities}
                    onChangeTypes={onChangeTypes}
                />

                {/* Show validation warnings if any */}
                {validationWarnings.length > 0 ? (
                    <ValidationWarning
                        key={`validation-${selectedEntityTypes.join('-')}-${validationWarnings.length}`}
                        warnings={validationWarnings}
                        onResetFilters={onResetConditions}
                        showResetFilters
                    />
                ) : null}

                {selectedEntityTypes.length > 0 ? (
                    <Section>
                        <AdditionalFilters>
                            <AdditionalFiltersTitle>
                                <b>Additional Filters</b>
                            </AdditionalFiltersTitle>
                            <Typography.Text type="secondary">(Optional)</Typography.Text>
                            <Tooltip
                                placement="right"
                                title="Continue to narrow your selection set based an asset's Data Platform, Domains, Glossary Terms, Owners, Usage statistics, & more."
                            >
                                <StyledInfoOutlined />
                            </Tooltip>
                        </AdditionalFilters>
                        <BuilderWrapper>
                            <LogicalPredicateBuilder
                                selectedPredicate={convertTestPredicateToLogicalPredicate(
                                    testDefinition.on.conditions || [],
                                )}
                                onChangePredicate={onChangePredicate}
                                properties={getPropertiesForEntityTypes(selectedEntityTypes)}
                                disabled={!testDefinition.on?.types || testDefinition.on?.types?.length === 0}
                                options={{
                                    predicateDisplayName: 'filter',
                                }}
                            />
                        </BuilderWrapper>
                    </Section>
                ) : null}
            </YamlStep>
        </>
    );
};
