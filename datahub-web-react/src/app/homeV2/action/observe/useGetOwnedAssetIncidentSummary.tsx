export type IncidentSummary = {
    loading: boolean;
    summary: number;
};

export const useGetOwnedAssetIncidentSummary = (): IncidentSummary => {
    return {
        loading: false,
        summary: 0,
    };
};
