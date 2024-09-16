/*
 * Condition Selector
 * User can select a set of conditions to filter the entities
 */

import React from 'react';

import { Input, Select } from 'antd';
import { Delete, Add } from '@mui/icons-material';
import uniqid from 'uniqid';

import { PrimaryButton, SecondaryButton, TextButton, DeleteButton } from '@app/automations/sharedComponents';
import { SortableList } from './SortableList';

import { predicateOptions, operatorOptions, transformConditions } from './utils';

// import { OPERATORS } from './operators';
// import { entityProperties } from './properties';

import {
    EmptyStateContainer,
    ConditionsContainer,
    RuleSelectorContainer,
    OperatorSelectorContainer,
    ConditionsActionsContainer,
    ConditionGroupContainer,
    RuleSelectorActionsContainer,
    GroupAddContainer,
    MatchingAssetText,
} from './components';

type ConditionType = {
    id: number;
    type: string;
    assetTypes: any[];
    props: {
        conditions: ConditionType[];
        showAssetCount: boolean;
        operator?: string;
        field?: any;
        predicate?: string;
        value?: string;
        properties?: any[];
    };
    groupId?: number;
    removeCondition?: any;
    addCondition?: any;
    updateOperator?: any;
    updateField?: any;
    updatePredicate?: any;
    updateValue?: any;
};

// Condition Selector
const RuleSelector = ({ ...props }: any) => {
    const hideValueInput = props.predicate !== 'exists';

    return (
        <RuleSelectorContainer noInput={!hideValueInput}>
            <SortableList.DragHandle />
            <div>
                <Select
                    placeholder="Field…"
                    value={props.field}
                    onChange={(value: any) => props.updateField(value, props.id, props.groupId)}
                />
            </div>
            <div>
                <Select
                    options={predicateOptions}
                    defaultValue={props.predicate || predicateOptions[0].value}
                    showSearch={false}
                    onChange={(value: any) => props.updatePredicate(value, props.id, props.groupId)}
                />
            </div>
            {hideValueInput && (
                <div>
                    <Input
                        placeholder="Condition string…"
                        value={props.value}
                        onChange={(e) => props.updateValue(e.target.value, props.id, props.groupId)}
                    />
                </div>
            )}
            <div>
                <DeleteButton shape="circle" icon={<Delete />} onClick={props.removeCondition} />
            </div>
        </RuleSelectorContainer>
    );
};

// Predicate Selector
const OperatorSelector = ({ ...props }: any) => {
    if (props.hideOperator) return null;

    return (
        <OperatorSelectorContainer>
            <Select
                options={operatorOptions}
                defaultValue={props.operator || operatorOptions[0].value}
                onChange={(value: any) => props.updateOperator(value, props.id, props.groupId)}
                showSearch={false}
            />
        </OperatorSelectorContainer>
    );
};

const Condition = ({ ...props }: any) => {
    return (
        <div>
            <OperatorSelector {...props} />
            <RuleSelector {...props} />
            {props.showAssetCount && (
                <RuleSelectorActionsContainer>
                    <MatchingAssetText>- matching assets</MatchingAssetText>
                    <TextButton disabled>View Assets</TextButton>
                </RuleSelectorActionsContainer>
            )}
        </div>
    );
};

const ConditionGroup = ({ ...props }: any) => {
    return (
        <div>
            <OperatorSelector {...props} />
            <ConditionGroupContainer>
                {props.children}
                <GroupAddContainer>
                    <TextButton icon={<Add />} onClick={props.addCondition}>
                        Add Condition
                    </TextButton>
                </GroupAddContainer>
            </ConditionGroupContainer>
            {props.showAssetCount && (
                <RuleSelectorActionsContainer>
                    <MatchingAssetText>- matching assets</MatchingAssetText>
                    <TextButton>View Assets</TextButton>
                </RuleSelectorActionsContainer>
            )}
        </div>
    );
};

export const ConditionSelector = ({
    selectedAssetTypes,
    initialConditions,
}: // selectedConditions,
// setConditionSelection
any) => {
    // Initial single condition
    const singleCondition: ConditionType = {
        // Unique ID for the condition (unrelated to backend id's or urns)
        id: uniqid(),
        // Type of condition (group or single)
        type: 'single',
        // Asset Types for the condition
        assetTypes: selectedAssetTypes,
        // Props for the condition components
        props: {
            // Component Reqs
            conditions: [],
            showAssetCount: true,
            // Initial condition
            operator: 'and',
            field: undefined,
            predicate: predicateOptions[0].value,
            value: undefined,
        },
    };

    const formatedInitalConditions = transformConditions(initialConditions, selectedAssetTypes);

    // List of conditions
    const [conditions, setConditions] = React.useState<ConditionType[]>([]);

    // Set initial conditions
    React.useEffect(() => {
        if (formatedInitalConditions.length > 0 && conditions.length === 0) {
            setConditions(formatedInitalConditions);
        }
    }, [formatedInitalConditions, conditions]);

    // Util to find the index of a condition
    const findConditionIndex = (list: ConditionType[], id: number) => list.findIndex((c: any) => c.id === id);

    // Add a new blank condition or group of conditions
    const addCondition = (type: any, groupId: any) => {
        // If no group id is provided, add a new condition (type group or single)
        if (!groupId) {
            setConditions([
                ...conditions,
                {
                    ...singleCondition,
                    id: uniqid(),
                    type,
                    props: {
                        ...singleCondition.props,
                        conditions:
                            type === 'group'
                                ? [
                                      {
                                          ...singleCondition,
                                          props: {
                                              ...singleCondition.props,
                                              showAssetCount: false,
                                          },
                                      },
                                  ]
                                : [],
                        showAssetCount: true,
                    },
                },
            ]);
        } else {
            // If a group id is provided, add a new condition to the group (type single)
            const groupIndex = findConditionIndex(conditions, groupId);
            conditions[groupIndex].props.conditions.push({
                ...singleCondition,
                id: uniqid(),
                props: {
                    ...singleCondition.props,
                    showAssetCount: false,
                },
            });
            setConditions([...conditions]);
        }
    };

    // Remove a condition or group
    const removeCondition = (id: number, groupId: any) => {
        const updatedConditions = [...conditions];

        if (!groupId) {
            // Remove condition from the list of conditions
            const index = findConditionIndex(updatedConditions, id);
            if (index !== -1) {
                updatedConditions.splice(index, 1);
                setConditions(updatedConditions);
            }
        } else {
            // Remove condition from the group
            const groupIndex = findConditionIndex(updatedConditions, groupId);
            if (groupIndex !== -1) {
                const group = updatedConditions[groupIndex];
                group.props.conditions = group.props.conditions.filter((c: any) => c.id !== id);

                // Remove the group if it becomes empty and update hideOperator for the last condition in the group
                if (group.props.conditions.length === 0) {
                    updatedConditions.splice(groupIndex, 1);
                }

                setConditions(updatedConditions);
            }
        }
    };

    // Util to set conditions in group (sortable list in a sortable list)
    const setConditionsInGroup = (newConditions: ConditionType[], groupId: any) => {
        const groupIndex = findConditionIndex(conditions, groupId);
        conditions[groupIndex].props.conditions = newConditions;
        setConditions([...conditions]);
    };

    // Util to hide the operator for the first condition
    const hideOperator = (idx: number) => idx === 0;

    // Util to DRY update condition functions
    const helperUpdateCondition = (id: any, field: any, value: any, groupId?: any) => {
        // If no group id is provided, update the condition
        if (!groupId) {
            const index = findConditionIndex(conditions, id);
            conditions[index].props[field] = value;
            setConditions([...conditions]);
        } else {
            // If a group id is provided, update the condition in the group
            const groupIndex = findConditionIndex(conditions, groupId);
            const groupConditions = conditions[groupIndex].props.conditions;
            const conditionIndex = findConditionIndex(groupConditions, id);
            groupConditions[conditionIndex].props[field] = value;
            setConditions([...conditions]);
        }
    };

    // Function to update a conditions operator
    const updateOperator = (operator: any, id: any, groupId?: any) =>
        helperUpdateCondition(id, 'operator', operator, groupId);

    // Function to update a conditions field
    const updateField = (field: any, id: any, groupId?: any) => helperUpdateCondition(id, 'field', field, groupId);

    // Function to update a conditions predicate
    const updatePredicate = (predicate: any, id: any, groupId?: any) =>
        helperUpdateCondition(id, 'predicate', predicate, groupId);

    // Function to update a conditions value
    const updateValue = (value: any, id: any, groupId?: any) => helperUpdateCondition(id, 'value', value, groupId);

    // Update to pass to the condition components as props
    // (simplifies component definition)
    const updateProps = {
        updateOperator,
        updateField,
        updatePredicate,
        updateValue,
    };

    // Prepare conditions for backend
    const prepareConditions = (cndts: ConditionType[]) => {
        const preparedConditions = cndts.map((c: any, idx: number) => {
            if (c.type === 'group') {
                return {
                    [idx !== 0 ? c.props.operator : null]: prepareConditions(c.props.conditions),
                };
            }
            return {
                [idx !== 0 ? c.props.operator : 'and']: {
                    property: c.props.field,
                    operator: c.props.predicate,
                    value: c.props.value,
                },
            };
        });

        return preparedConditions;
    };

    // Prepare conditions for backend
    const preparedConditions = prepareConditions(conditions);

    // TODO: Send preparedConditions to parent component
    console.log('preparedConditions', preparedConditions);

    if (conditions.length === 0) {
        return (
            <EmptyStateContainer>
                <h3>No Conditions Added</h3>
                <p>Click &apos;Add Condition&apos; to get started.</p>
                <ConditionsActionsContainer>
                    <PrimaryButton icon={<Add />} onClick={() => addCondition('single', null)}>
                        Add Condition
                    </PrimaryButton>
                </ConditionsActionsContainer>
            </EmptyStateContainer>
        );
    }

    return (
        <ConditionsContainer>
            <MatchingAssetText>Total Assets Impacted: -</MatchingAssetText>
            <SortableList
                items={conditions}
                onChange={setConditions}
                renderItem={({ id, type, props }) => (
                    <SortableList.Item id={id} isSortable={type !== 'group'}>
                        {type === 'group' ? (
                            <ConditionGroup
                                id={id}
                                hideOperator={hideOperator(findConditionIndex(conditions, id))}
                                addCondition={() => addCondition('single', id)}
                                {...props}
                            >
                                <SortableList
                                    items={props.conditions}
                                    onChange={setConditionsInGroup}
                                    groupId={id}
                                    renderItem={(c: any) => (
                                        <SortableList.Item id={c.id}>
                                            <Condition
                                                id={c.id}
                                                groupId={id}
                                                hideOperator={hideOperator(findConditionIndex(props.conditions, c.id))}
                                                removeCondition={() => removeCondition(c.id, id)}
                                                {...updateProps}
                                                {...c.props}
                                            />
                                        </SortableList.Item>
                                    )}
                                />
                            </ConditionGroup>
                        ) : (
                            <Condition
                                id={id}
                                hideOperator={hideOperator(findConditionIndex(conditions, id))}
                                removeCondition={() => removeCondition(id, null)}
                                {...updateProps}
                                {...props}
                            />
                        )}
                    </SortableList.Item>
                )}
            />
            <ConditionsActionsContainer>
                <SecondaryButton icon={<Add />} onClick={() => addCondition('group', null)}>
                    Add Group
                </SecondaryButton>
                <PrimaryButton icon={<Add />} onClick={() => addCondition('single', null)}>
                    Add Condition
                </PrimaryButton>
            </ConditionsActionsContainer>
        </ConditionsContainer>
    );
};
