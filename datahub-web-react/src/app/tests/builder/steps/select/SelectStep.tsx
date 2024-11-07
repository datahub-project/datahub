import React, { useMemo } from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { Tooltip } from '@components';
import { InfoCircleOutlined } from '@ant-design/icons';
import { EntityCapabilityType } from '../../../../entity/Entity';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import { EntityTypeSelect } from '../definition/builder/property/input/EntityTypeSelect';
import { LogicalPredicateBuilder } from '../definition/builder/LogicalPredicateBuilder';
import { LogicalPredicate } from '../definition/builder/types';
import { YamlStep } from '../definition/yaml/YamlStep';
import { entityTypesToGraphNames, graphNamesToEntityTypes } from './utils';
import { deserializeTestDefinition, serializeTestDefinition } from '../definition/utils';
import {
    convertLogicalPredicateToTestPredicate,
    convertTestPredicateToLogicalPredicate,
} from '../definition/builder/utils';
import { StepProps, TestBuilderStep } from '../../types';
import { getPropertiesForEntityTypes } from '../definition/builder/property/utils';
import { ANTD_GRAY } from '../../../../entity/shared/constants';
import { EntityType } from '../../../../../types.generated';

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

    const testEntities: EntityType[] = Array.from(
        entityRegistry.getTypesWithSupportedCapabilities(EntityCapabilityType.TEST),
    );
    const selectedEntityTypes = graphNamesToEntityTypes(testDefinition.on?.types || [], entityRegistry);

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
                {selectedEntityTypes.length > 0 && (
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
                                selectedPredicate={
                                    convertTestPredicateToLogicalPredicate(
                                        testDefinition.on.conditions || [],
                                    ) as LogicalPredicate
                                }
                                onChangePredicate={onChangePredicate}
                                properties={getPropertiesForEntityTypes(selectedEntityTypes)}
                                disabled={!testDefinition.on?.types || testDefinition.on?.types.length === 0}
                                options={{
                                    predicateDisplayName: 'filter',
                                }}
                            />
                        </BuilderWrapper>
                    </Section>
                )}
            </YamlStep>
        </>
    );
};
