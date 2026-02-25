import { useContext } from 'react';

import { StatsSectionsContext } from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/StatsTabContext';

export default function useStatsTabContext() {
    return useContext(StatsSectionsContext);
}
