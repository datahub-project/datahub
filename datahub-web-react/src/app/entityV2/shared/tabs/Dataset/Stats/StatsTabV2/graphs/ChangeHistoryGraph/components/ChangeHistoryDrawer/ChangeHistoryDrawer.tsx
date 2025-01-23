import { Drawer, SelectOption } from '@components';
import { OperationType } from '@src/types.generated';
import React, { useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';
import { AnyOperationType, OperationsData } from '../../types';
import TypesSelect from '../TypesSelect';
import ChangeHistoryTimeline from './components/ChangeHistoryTimeline';
import DateSwitcher from './components/DateSwitcher';
import UsersSelect from './components/UsersSelect';
import useDebounceFalse from './useDebounceFalse';
import useGetOperations from './useGetOperations';
import useGetUsers from './useGetUsers';
import useUsersSelectOptions from './useUsersSelectOptions';
import { getUniqueActorsFromOperations } from './utils';

const FlexRow = styled.div`
    display: flex;
    flex-direction: row;
    gap: 8px;
`;

const Controls = styled(FlexRow)`
    justify-content: flex-end;
`;

const ControlWrapper = styled.div`
    max-width: 280px;
`;

const DrawerContent = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

type ChangeHistoryDrawerProps = {
    urn: string;
    selectedDate?: string | null;
    setSelectedDate?: (value: string | null | undefined) => void;
    onClose: () => void;
    open: boolean;
    allOperationTypesOptions: SelectOption[];
    operationsData?: OperationsData | null;
};

export const ChangeHistoryDrawer = ({
    urn,
    operationsData,
    selectedDate,
    setSelectedDate,
    open,
    onClose,
    allOperationTypesOptions,
}: ChangeHistoryDrawerProps) => {
    const [selectedOperationTypes, setSelectedOperationTypes] = useState<AnyOperationType[]>([]);
    const operationTypesOptions = useMemo(() => {
        const operationKeys = Object.entries(operationsData?.operations || {})
            .filter(([_, operation]) => operation.value > 0)
            .map(([_, operation]) => operation.key);
        return allOperationTypesOptions?.filter((option) => operationKeys.includes(option.value)) ?? [];
    }, [operationsData, allOperationTypesOptions]);
    useEffect(
        () => setSelectedOperationTypes(operationTypesOptions.map((option) => option.value)),
        [operationTypesOptions],
    );

    const [selectedActors, setSelectedActors] = useState<string[]>([]);
    const [isUsersSelectUpdated, setIsUsersSelectUpdated] = useState<boolean>(false);
    useEffect(() => setIsUsersSelectUpdated(false), [selectedDate]);

    const { operations, loading: operationsLoading } = useGetOperations(
        urn,
        selectedDate,
        selectedOperationTypes,
        isUsersSelectUpdated ? selectedActors : undefined,
    );

    const actors = useMemo(() => getUniqueActorsFromOperations(operations), [operations]);
    const { users, loading: usersLoading } = useGetUsers(actors);
    // FYI: add 150ms offset before turning loading to false
    // because of a little gap between operations and users requests
    // that makes some blinks of loading state on the timeline
    const loading = useDebounceFalse(150, operationsLoading, usersLoading);

    const usersSelectOptions = useUsersSelectOptions(users, loading, selectedDate);
    const initialSelectedUsers = useMemo(() => usersSelectOptions.map((option) => option.value), [usersSelectOptions]);

    return (
        <Drawer title="Change History Details" open={open} onClose={onClose} maskTransparent>
            <DrawerContent>
                <Controls>
                    <ControlWrapper>
                        <UsersSelect
                            options={usersSelectOptions}
                            values={isUsersSelectUpdated ? selectedActors : initialSelectedUsers}
                            onUpdate={(values) => {
                                setSelectedActors(values);
                                setIsUsersSelectUpdated(true);
                            }}
                            loading={loading}
                        />
                    </ControlWrapper>
                    <ControlWrapper>
                        <TypesSelect
                            options={operationTypesOptions}
                            values={selectedOperationTypes}
                            onUpdate={(values) => setSelectedOperationTypes(values as OperationType[])}
                            loading={loading}
                        />
                    </ControlWrapper>
                    <ControlWrapper>
                        <DateSwitcher value={selectedDate} setValue={(v) => setSelectedDate?.(v)} />
                    </ControlWrapper>
                </Controls>

                <ChangeHistoryTimeline
                    selectedDay={selectedDate}
                    operations={operations}
                    users={users}
                    loading={loading}
                />
            </DrawerContent>
        </Drawer>
    );
};
