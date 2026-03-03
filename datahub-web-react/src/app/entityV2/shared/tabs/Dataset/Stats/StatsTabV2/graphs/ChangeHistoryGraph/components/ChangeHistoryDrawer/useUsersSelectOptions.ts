import { SelectOption } from '@components';
import { useEffect, useState } from 'react';

import useGetUserName from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/useGetUserName';
import { CorpUser } from '@src/types.generated';

export default function useUsersSelectOptions(
    users: CorpUser[],
    loading: boolean,
    selectedDate: string | null | undefined,
) {
    const [options, setOptions] = useState<SelectOption[]>([]);
    const [isInitialized, setIsInitialized] = useState<boolean>(false);

    const getUserName = useGetUserName();

    // reinitialize when date changed
    useEffect(() => {
        setIsInitialized(false);
        // FYI: clean options in order to case when loading is finished but options have not updated yet
        // and user can see previous options after loading for a while
        setOptions([]);
    }, [selectedDate]);

    useEffect(() => {
        if (!loading && !isInitialized) {
            setOptions(
                users.map((user) => ({
                    value: user.urn,
                    label: getUserName(user),
                })),
            );
            if (users.length > 0) setIsInitialized(true);
        }
    }, [users, getUserName, loading, isInitialized]);

    return options;
}
