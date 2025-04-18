import { useContext } from 'react';
import { StatsSectionsContext } from '../StatsTabContext';

export default function useStatsTabContext() {
    return useContext(StatsSectionsContext);
}
