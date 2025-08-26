import React from 'react';

import { AcrylAssertionsTable } from '@app/entity/shared/tabs/Dataset/Validations/AcrylAssertionsTable';

import { Assertion, DataContract } from '@types';

type Props = {
    assertions: Array<Assertion>;
    contract?: DataContract;
    // required for enabling menu/actions
    showMenu?: boolean;
    canEditAssertions: boolean;
    canEditMonitors: boolean;
    canEditSqlAssertions: boolean;
    refetch?: () => void;
    // required for enabling selection logic
    showSelect?: boolean;
    selectedUrns?: string[];
    onSelect?: (assertionUrn: string) => void;
};

/**
 * Acryl-specific list of assertions displaying their most recent run status, their human-readable
 * description, and platform.
 *
 * Currently this component supports rendering Dataset Assertions only.
 */
export const AcrylDatasetAssertionsList = ({
    assertions,
    contract,
    showMenu,
    showSelect,
    selectedUrns,
    canEditAssertions,
    canEditMonitors,
    canEditSqlAssertions,
    onSelect,
    refetch,
}: Props) => {
    return (
        <AcrylAssertionsTable
            assertions={assertions}
            contract={contract}
            onSelect={onSelect}
            showMenu={showMenu}
            showSelect={showSelect}
            selectedUrns={selectedUrns}
            canEditAssertions={canEditAssertions}
            canEditMonitors={canEditMonitors}
            canEditSqlAssertions={canEditSqlAssertions}
            refetch={refetch}
        />
    );
};
