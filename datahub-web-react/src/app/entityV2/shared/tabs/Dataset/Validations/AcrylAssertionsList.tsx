import React from 'react';
import { Assertion, DataContract } from '../../../../../../types.generated';
import { AcrylAssertionsTable } from './AcrylAssertionsTable';

type Props = {
    assertions: Array<Assertion>;
    contract?: DataContract;
    showMenu?: boolean;
    showSelect?: boolean;
    selectedUrns?: string[];
    onSelect?: (assertionUrn: string) => void;
    refetch?: () => void;
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
            refetch={refetch}
        />
    );
};
