import { Drawer, SelectOption } from '@components';
import { OperationType } from '@src/types.generated';
import React, { useMemo, useState } from 'react';
import styled from 'styled-components';
import { AnyOperationType, OperationsData } from '../../types';
import TypesSelect from '../TypesSelect';
import ChangeHistoryTimeline from './components/ChangeHistoryTimeline';
import useDebounceFalse from './useDebounceFalse';
import useGetOperations from './useGetOperations';
import useGetUsers from './useGetUsers';
import useSelectUserOptions from './useSelectUserOptions';
import { getUniqueActorsFromOperations } from './utils';
import UsersSelect from './components/UsersSelect';

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
    selectedDay?: string | null;
    onClose: () => void;
    open: boolean;
    operationTypesOptions: SelectOption[];
    value?: OperationsData | null;
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
    const [isUsersSelectUpdated, setIsUsersSelectUpdated] = useState<boolean>(false);

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
        isUsersSelectUpdated ? selectedActors : undefined,
    );

    const actors = useMemo(() => getUniqueActorsFromOperations(operations), [operations]);
    const { users, loading: usersLoading } = useGetUsers(actors);
    // FYI: add 150ms offset before turning loading to false
    // because of a little gap between operations and users requests
    // that makes some blinks of loading state on the timeline
    const loading = useDebounceFalse(150, operationsLoading, usersLoading);
    const selectUsersOptions = useSelectUserOptions(users, loading);

    const initialSelectedUsers = useMemo(() => selectUsersOptions.map((option) => option.value), [selectUsersOptions]);

    return (
        <Drawer title="Change History Details" open={open} onClose={onClose} maskTransparent>
            <DrawerContent>
                <Controls>
                    <ControlWrapper>
                        <UsersSelect
                            options={selectUsersOptions}
                            values={isUsersSelectUpdated ? selectedActors : initialSelectedUsers}
                            onUpdate={(values) => {
                                setSelectedActors(values);
                                setIsUsersSelectUpdated(true);
                            }}
                            isDisabled={loading || selectUsersOptions.length === 0}
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
