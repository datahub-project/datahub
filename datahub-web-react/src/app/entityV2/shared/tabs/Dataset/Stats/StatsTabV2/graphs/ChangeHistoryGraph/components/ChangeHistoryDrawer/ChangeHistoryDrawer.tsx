import { Drawer, SelectOption, SimpleSelect } from '@components';
import { OperationType } from '@src/types.generated';
import React, { useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';
import { AnyOperationType, OperationsData } from '../../types';
import TypesSelect from '../TypesSelect';
import ChangeHistoryTimeline from './components/ChangeHistoryTimeline';
import useDebounceFalse from './useDebounceFalse';
import useGetOperations from './useGetOperations';
import useGetUsers from './useGetUsers';
import useSelectUserOptions from './useSelectUserOptions';
import { getUniqueActorsFromOperations } from './utils';

const FlexRow = styled.div`
    display: flex;
    flex-direction: row;
    gap: 8px;
`;

const Controls = styled(FlexRow)`
    align-items: start;
`;

const ControlWrapper = styled.div`
    width: 280px;
`;

const DrawerContent = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

type ChangeHistoryDrawerProps = {
    urn: string;
    selectedDay?: string;
    onClose: () => void;
    open: boolean;
    operationTypesOptions: SelectOption[];
    value?: OperationsData;
};

export const ChangeHistoryDrawer = ({
    urn,
    selectedDay,
    open,
    onClose,
    operationTypesOptions,
    value,
}: ChangeHistoryDrawerProps) => {
    const keysFromValue = useMemo(
        () =>
            Object.entries(value?.operations || {})
                .filter(([_, operation]) => operation.value > 0)
                .map(([_, operation]) => operation.key),
        [value?.operations],
    );

    const [selectedActors, setSelectedActors] = useState<string[]>([]);

    const filteredOperationTypesOptions = useMemo(() => {
        return operationTypesOptions.filter((option) => keysFromValue.includes(option.value));
    }, [operationTypesOptions, keysFromValue]);
    const [selectedOperationTypes, setSelectedOperationTypes] = useState<AnyOperationType[]>(
        filteredOperationTypesOptions.map((option) => option.value),
    );

    const { operations, loading: operationsLoading } = useGetOperations(
        urn,
        selectedDay,
        selectedOperationTypes,
        selectedActors,
    );
    const actors = useMemo(() => getUniqueActorsFromOperations(operations), [operations]);
    const { users, loading: usersLoading } = useGetUsers(actors);
    // FYI: add 150ms offset before turning loading to false
    // because of a little gap between operations and users requests
    // that makes some blinks of loading state on the timeline
    const loading = useDebounceFalse(150, operationsLoading, usersLoading);
    const selectUsersOptions = useSelectUserOptions(users, loading);

    useEffect(() => {
        if (
            !selectedActors.every((actor) => selectUsersOptions.filter((option) => option.value === actor).length > 0)
        ) {
            setSelectedActors([]);
        }
    }, [selectedActors, selectUsersOptions]);

    return (
        <Drawer title="Change History Details" open={open} onClose={onClose} maskTransparent>
            <DrawerContent>
                <Controls>
                    <ControlWrapper>
                        <SimpleSelect
                            placeholder="User"
                            options={selectUsersOptions}
                            onUpdate={(values) => setSelectedActors(values)}
                            width="full"
                            isDisabled={users.length === 0 || loading || selectUsersOptions.length === 0}
                        />
                    </ControlWrapper>
                    <ControlWrapper>
                        <TypesSelect
                            options={filteredOperationTypesOptions}
                            values={selectedOperationTypes}
                            onUpdate={(values) => setSelectedOperationTypes(values as OperationType[])}
                            loading={loading}
                        />
                    </ControlWrapper>
                </Controls>

                <ChangeHistoryTimeline
                    selectedDay={selectedDay}
                    operations={operations}
                    users={users}
                    loading={loading}
                />
            </DrawerContent>
        </Drawer>
    );
};
