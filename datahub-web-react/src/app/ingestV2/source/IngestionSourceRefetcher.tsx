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
