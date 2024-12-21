import { SelectOption } from '@components';
import { CorpUser } from '@src/types.generated';
import { useEffect, useState } from 'react';
import useGetUserName from './useGetUserName';

export default function useSelectUserOptions(users: CorpUser[], loading: boolean) {
    const [options, setOptions] = useState<SelectOption[]>([]);

    const getUserName = useGetUserName();

    useEffect(() => {
        if (!loading) {
            setOptions(
                users.map((user) => ({
                    value: user.urn,
                    label: getUserName(user),
                })),
            );
        }
    }, [users, getUserName, loading]);

    return options;
}
