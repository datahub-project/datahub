import { Drawer, SimpleSelect } from '@components';
import { OperationType } from '@src/types.generated';
import React, { useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';
import ChangeHistoryTimeline from './components/ChangeHistoryTimeline';
import useDebounceFalse from './useDebounceFalse';
import useGetOperations from './useGetOperations';
import useGetUsers from './useGetUsers';
import useSelectUserOptions from './useSelectUserOptions';
import { getUniqueActorsFromOperations } from './utils';
import { OPERATION_TYPE_OPTIONS } from '../../constants';
import TypesSelect from '../TypesSelect';

const FlexRow = styled.div`
    display: flex;
    flex-direction: row;
    gap: 8px;
`;

const Controls = styled(FlexRow)`
    align-items: start;
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
    operationTypes: OperationType[];
};

export const ChangeHistoryDrawer = ({ urn, selectedDay, open, onClose, operationTypes }: ChangeHistoryDrawerProps) => {
    const [selectedOperationTypes, setSelectedOperationTypes] = useState<OperationType[]>(operationTypes);
    const [selectedActors, setSelectedActors] = useState<string[]>([]);

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
                    <SimpleSelect
                        placeholder="User"
                        options={selectUsersOptions}
                        onUpdate={(values) => setSelectedActors(values)}
                        width="full"
                        isDisabled={users.length === 0 || loading || selectUsersOptions.length === 0}
                    />
                    <TypesSelect
                        options={OPERATION_TYPE_OPTIONS}
                        values={selectedOperationTypes}
                        onUpdate={(values) => setSelectedOperationTypes(values as OperationType[])}
                        loading={loading}
                    />
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
