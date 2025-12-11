/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { AcrylAssertionsTable } from '@app/entityV2/shared/tabs/Dataset/Validations/AcrylAssertionsTable';

import { Assertion, DataContract } from '@types';

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
