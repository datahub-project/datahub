import React, { useCallback, useMemo, useState } from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { getEntityUrnForAssertion } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { AssertionProfileFooter } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/AssertionProfileFooter';
import { AssertionProfileHeader } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/AssertionProfileHeader';
import { AssertionProfileHeaderLoading } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/AssertionProfileHeaderLoading';
import { AssertionTabs } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/AssertionTabs';
import { AssertionSummaryTab } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/AssertionSummaryTab';
import { Maybe } from '@src/types.generated';

import { useGetAssertionQuery } from '@graphql/assertion.generated';
import { useGetDatasetContractQuery } from '@graphql/contract.generated';
import { Assertion, DataContract } from '@types';

enum TabType {
    Summary = 'Summary',
}

type Props = {
    urn: string;
    entity: GenericEntityProperties; // TODO: ideally this would be a field on the assertion itself.
    // TODO: Ideally this would come from the load of the assertion details itself with the GraphQL call.
    // Currently this is a function of privileges on the target dataset!
    canEditAssertions: boolean;
    close: () => void;
};

// TODO: Handling Loading Errors.

export const AssertionProfile = ({ urn, entity: _entity, canEditAssertions, close }: Props) => {
    const [selectedTab, setSelectedTab] = useState<string>(TabType.Summary);
    const {
        data: assertionData,
        loading: assertionLoading,
        refetch: assertionRefetch,
    } = useGetAssertionQuery({ variables: { assertionUrn: urn }, fetchPolicy: 'cache-first' });

    const assertion = assertionData?.assertion as Maybe<Assertion>;
    const result = assertion?.runEvents?.runEvents[0]?.result;
    const assertionEntityUrn = assertion ? getEntityUrnForAssertion(assertion) : undefined;

    const {
        data: contractData,
        refetch: contractRefetch,
        loading: contractLoading,
    } = useGetDatasetContractQuery({
        // Query will be skipped if assertionEntityUrn is undefined
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        variables: { urn: assertionEntityUrn! },
        fetchPolicy: 'cache-first',
        skip: !assertionEntityUrn,
    });

    const fullRefetch = useCallback(async () => {
        try {
            await Promise.allSettled([assertionRefetch(), contractRefetch()]);
        } catch (error) {
            console.error('Error refetching assertion details:', error);
        }
    }, [assertionRefetch, contractRefetch]);

    const contract = contractData?.dataset?.contract as DataContract | null | undefined;
    const isLoading = assertionLoading || contractLoading;

    const tabs = useMemo(
        () => [
            {
                key: TabType.Summary,
                label: 'Summary',
                content: <AssertionSummaryTab loading={isLoading} assertion={assertion} />,
            },
        ],
        [isLoading, assertion],
    );

    return (
        <>
            {isLoading || !assertion ? (
                <AssertionProfileHeaderLoading />
            ) : (
                <AssertionProfileHeader
                    assertion={assertion}
                    contract={contract}
                    result={result || undefined}
                    canEditAssertion={canEditAssertions}
                    refetch={fullRefetch}
                    close={close}
                />
            )}
            <AssertionTabs selectedTab={selectedTab} setSelectedTab={setSelectedTab} tabs={tabs} />
            <AssertionProfileFooter />
        </>
    );
};
