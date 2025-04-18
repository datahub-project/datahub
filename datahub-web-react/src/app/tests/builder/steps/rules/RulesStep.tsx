import React, { useMemo, useState } from 'react';
import styled from 'styled-components';
import { Collapse, Typography } from 'antd';
import { Tooltip } from '@components';
import { InfoCircleOutlined } from '@ant-design/icons';
import { LogicalPredicateBuilder } from '../definition/builder/LogicalPredicateBuilder';
import { LogicalPredicate } from '../definition/builder/types';
import { serializeTestDefinition, deserializeTestDefinition } from '../definition/utils';
import { ValidateTestModal } from '../validate/ValidateTestModal';
import { YamlStep } from '../definition/yaml/YamlStep';
import {
    convertLogicalPredicateToTestPredicate,
    convertTestPredicateToLogicalPredicate,
} from '../definition/builder/utils';
import { StepProps, TestBuilderStep } from '../../types';
import { getPropertiesForEntityTypes } from '../definition/builder/property/utils';
import { graphNamesToEntityTypes } from '../select/utils';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import { ActionsStep } from '../actions/ActionsStep';
import { ANTD_GRAY } from '../../../../entity/shared/constants';

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

    const selectedEntityTypes = graphNamesToEntityTypes(testDefinition.on?.types || [], entityRegistry);

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
                <BuilderWrapper>
                    <LogicalPredicateBuilder
                        selectedPredicate={
                            convertTestPredicateToLogicalPredicate(testDefinition.rules) as LogicalPredicate
                        }
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
