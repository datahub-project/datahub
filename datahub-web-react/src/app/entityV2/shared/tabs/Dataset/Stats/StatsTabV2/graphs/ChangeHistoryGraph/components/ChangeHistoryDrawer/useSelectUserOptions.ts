import { SelectOption } from '@components';
import { CorpUser } from '@src/types.generated';
import { useEffect, useState } from 'react';
import useGetUserName from './useGetUserName';

export default function useSelectUserOptions(users: CorpUser[], loading: boolean) {
    const [options, setOptions] = useState<SelectOption[]>([]);
    const [isInitialized, setIsInitialized] = useState<boolean>(false);

    const getUserName = useGetUserName();

    useEffect(() => {
        if (!loading && !isInitialized && users.length) {
            setOptions(
                users.map((user) => ({
                    value: user.urn,
                    label: getUserName(user),
                })),
            );
            setIsInitialized(true);
        }
    }, [users, getUserName, loading, isInitialized]);

    return options;
}
