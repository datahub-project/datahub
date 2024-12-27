import useGetTimeseriesCapabilities from '../../hooks/useGetTimeseriesCapabilities';

export default function useChangeHistoryOldestDayOfData(urn?: string) {
    const { data, loading } = useGetTimeseriesCapabilities(urn);

    return { data: data?.oldestOperationTime, loading };
}
