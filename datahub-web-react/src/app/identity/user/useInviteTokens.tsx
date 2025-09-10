import { message } from 'antd';
import { useCallback, useEffect, useMemo, useState } from 'react';

import analytics, { EventType } from '@app/analytics';

import { useCreateInviteTokenMutation } from '@graphql/mutations.generated';
import { useGetInviteTokenQuery } from '@graphql/role.generated';
import { DataHubRole } from '@types';

export function useInviteTokens(selectedRole: DataHubRole | undefined) {
    const baseUrl = window.location.origin;
    const [inviteToken, setInviteToken] = useState<string>('');

    const [createInviteTokenMutation] = useCreateInviteTokenMutation();

    const { data: getInviteTokenData } = useGetInviteTokenQuery({
        variables: {
            input: {
                roleUrn: selectedRole?.urn,
            },
        },
        skip: !selectedRole?.urn,
        fetchPolicy: 'cache-and-network',
    });

    useEffect(() => {
        if (getInviteTokenData?.getInviteToken?.inviteToken) {
            setInviteToken(getInviteTokenData.getInviteToken.inviteToken);
        }
    }, [getInviteTokenData]);

    const createInviteToken = useCallback(
        (roleUrn?: string) => {
            createInviteTokenMutation({
                variables: {
                    input: {
                        roleUrn,
                    },
                },
            })
                .then((result) => {
                    analytics.event({
                        type: EventType.CreateInviteLinkEvent,
                        roleUrn: selectedRole?.urn,
                    });
                    if (result.data) {
                        setInviteToken(result.data?.createInviteToken?.inviteToken || '');
                        message.success({
                            content: `Successfully created invite token! Users who join via this token will become: ${selectedRole?.name || 'No Role'}`,
                        });
                    }
                })
                .catch((e: any) => {
                    message.error({
                        content: `Failed to create Invite Token for role ${selectedRole?.name} : \n ${e.message || ''}`,
                    });
                });
        },
        [createInviteTokenMutation, selectedRole],
    );

    const inviteLink = useMemo(() => {
        return `${baseUrl}/signup?invite_token=${inviteToken}&redirect_on_sso=true`;
    }, [baseUrl, inviteToken]);

    const resetInviteToken = useCallback(() => {
        setInviteToken('');
    }, []);

    return {
        // State
        inviteToken,
        inviteLink,

        // Handlers
        createInviteToken,
        resetInviteToken,
    };
}
