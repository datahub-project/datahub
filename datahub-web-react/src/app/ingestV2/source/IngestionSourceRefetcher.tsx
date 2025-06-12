import usePollIngestionSource from '@app/ingestV2/source/usePollSources';

import { IngestionSource, ListIngestionSourcesInput } from '@types';

interface Props {
    urn: string;
    setFinalSources: React.Dispatch<React.SetStateAction<IngestionSource[]>>;
    setSourcesToRefetch: React.Dispatch<React.SetStateAction<Set<string>>>;
    queryInputs: ListIngestionSourcesInput;
}

const IngestionSourceRefetcher = ({ urn, setFinalSources, setSourcesToRefetch, queryInputs }: Props) => {
    usePollIngestionSource({ urn, setFinalSources, setSourcesToRefetch, queryInputs });
    return null;
};

export default IngestionSourceRefetcher;
