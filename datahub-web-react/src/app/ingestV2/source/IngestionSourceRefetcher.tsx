/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import usePollSource from '@app/ingestV2/source/usePollSource';

import { IngestionSource, ListIngestionSourcesInput } from '@types';

interface Props {
    urn: string;
    setFinalSources: React.Dispatch<React.SetStateAction<IngestionSource[]>>;
    setSourcesToRefetch: React.Dispatch<React.SetStateAction<Set<string>>>;
    setExecutedUrns: React.Dispatch<React.SetStateAction<Set<string>>>;
    queryInputs: ListIngestionSourcesInput;
    isExecutedNow: boolean;
}

const IngestionSourceRefetcher = ({
    urn,
    setFinalSources,
    setSourcesToRefetch,
    setExecutedUrns,
    queryInputs,
    isExecutedNow,
}: Props) => {
    usePollSource({ urn, setFinalSources, setSourcesToRefetch, setExecutedUrns, queryInputs, isExecutedNow });
    return null;
};

export default IngestionSourceRefetcher;
