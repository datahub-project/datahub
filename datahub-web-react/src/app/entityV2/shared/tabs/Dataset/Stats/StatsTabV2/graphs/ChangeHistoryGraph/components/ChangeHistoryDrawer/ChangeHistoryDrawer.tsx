import { Drawer, SelectOption } from '@components';
import React, { useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';

import ChangeHistoryTimeline from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/components/ChangeHistoryTimeline';
import DateSwitcher from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/components/DateSwitcher';
import UsersSelect from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/components/UsersSelect';
import useDebounceFalse from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/useDebounceFalse';
import useGetOperations from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/useGetOperations';
import useGetUsers from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/useGetUsers';
import useUsersSelectOptions from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/useUsersSelectOptions';
import { getUniqueActorsFromOperations } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/utils';
import TypesSelect from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/TypesSelect';
import {
    AnyOperationType,
    OperationsData,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/types';
import { OperationType } from '@src/types.generated';

const FlexRow = styled.div`
    display: flex;
    flex-direction: row;
    gap: 8px;
`;

const Controls = styled(FlexRow)`
    justify-content: flex-start;
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
        <Drawer
            title="Change History Details"
            open={open}
            onClose={onClose}
            width={542}
            maskTransparent
            dataTestId="change-history-details"
        >
            <DrawerContent>
                <Controls>
                    <ControlWrapper>
                        <DateSwitcher value={selectedDate} setValue={(v) => setSelectedDate?.(v)} />
                    </ControlWrapper>
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
