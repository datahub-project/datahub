import { InfoCircleOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import { Collapse, Typography } from 'antd';
import React, { useMemo, useState } from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { ActionsStep } from '@app/tests/builder/steps/actions/ActionsStep';
import { LogicalPredicateBuilder } from '@app/tests/builder/steps/definition/builder/LogicalPredicateBuilder';
import { getPropertiesForEntityTypes } from '@app/tests/builder/steps/definition/builder/property/utils';
import {
    convertLogicalPredicateToTestPredicate,
    convertTestPredicateToLogicalPredicate,
} from '@app/tests/builder/steps/definition/builder/utils';
import { deserializeTestDefinition, serializeTestDefinition } from '@app/tests/builder/steps/definition/utils';
import { YamlStep } from '@app/tests/builder/steps/definition/yaml/YamlStep';
import { graphNamesToEntityTypes } from '@app/tests/builder/steps/select/utils';
import { ValidateTestModal } from '@app/tests/builder/steps/validate/ValidateTestModal';
import { StepProps, TestBuilderStep } from '@app/tests/builder/types';
import { ValidationWarning } from '@app/tests/builder/validation/ValidationWarning';
import { validateCompleteTestDefinition } from '@app/tests/builder/validation/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

const BuilderWrapper = styled.div`
    margin-bottom: 28px;
`;

const StyledCollapse = styled(Collapse)`
    && {
        margin: 0px;
        padding: 0px;
        margin-bottom: 28px;
    }
`;

const Title = styled(Typography.Title)`
    display: flex;
    align-items: center;
`;

const SubTitle = styled(Typography.Paragraph)`
    font-size: 16px;
`;

const StyledInfoOutlined = styled(InfoCircleOutlined)`
    margin-left: 8px;
    font-size: 12px;
    color: ${ANTD_GRAY[7]};
`;

export const RulesStep = ({ state, updateState, prev, goTo }: StepProps) => {
    const entityRegistry = useEntityRegistry();
    const testDefinition = useMemo(() => deserializeTestDefinition(state?.definition?.json || '{}'), [state]);

    const [showTestModal, setShowTestModal] = useState(false);

    const onClickNext = () => {
        goTo(TestBuilderStep.NAME);
    };

    const onClickTest = () => {
        setShowTestModal(true);
    };

    const onChangePredicate = (newPredicate) => {
        const newDefinition = {
            ...testDefinition,
            rules: convertLogicalPredicateToTestPredicate(newPredicate),
        };
        const newState = {
            ...state,
            definition: {
                json: serializeTestDefinition(newDefinition),
            },
        };
        updateState(newState);
    };

    const onResetRules = () => {
        const newDefinition = {
            ...testDefinition,
            rules: [], // Clear all rules
        };
        const newState = {
            ...state,
            definition: {
                json: serializeTestDefinition(newDefinition),
            },
        };
        updateState(newState);
    };

    const onResetActions = () => {
        const newDefinition = {
            ...testDefinition,
            actions: {
                passing: [],
                failing: [],
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
                onPrev={prev}
                actionTitle="Test Conditions"
                actionTip="Try out your selection set and test conditions on real data assets."
                onAction={onClickTest}
            >
                <Title level={4}>
                    Define your test conditions
                    <Tooltip
                        placement="right"
                        title="If you do not provide any conditions, all assets in the selection criteria will be considered passing."
                    >
                        <StyledInfoOutlined />
                    </Tooltip>
                </Title>
                <SubTitle type="secondary">What criteria must each selected asset meet?</SubTitle>

                {/* Show validation warnings if any */}
                {validationWarnings.length > 0 ? (
                    <ValidationWarning
                        key={`validation-rules-${selectedEntityTypes.join('-')}-${validationWarnings.length}`}
                        warnings={validationWarnings}
                        onResetFilters={onResetRules}
                        onResetActions={onResetActions}
                        showResetFilters
                        showResetActions
                    />
                ) : null}

                <BuilderWrapper>
                    <LogicalPredicateBuilder
                        selectedPredicate={convertTestPredicateToLogicalPredicate(testDefinition.rules)}
                        onChangePredicate={onChangePredicate}
                        properties={getPropertiesForEntityTypes(selectedEntityTypes)}
                        options={{
                            predicateDisplayName: 'condition',
                        }}
                    />
                </BuilderWrapper>
                <StyledCollapse>
                    <Collapse.Panel
                        header={<Typography.Text type="secondary">Advanced - Add actions</Typography.Text>}
                        key="1"
                    >
                        <ActionsStep state={state} updateState={updateState} />
                    </Collapse.Panel>
                </StyledCollapse>
            </YamlStep>
            {showTestModal && <ValidateTestModal state={state} onClose={() => setShowTestModal(false)} />}
        </>
    );
};
