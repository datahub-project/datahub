export type AssertionSummary = {
    loading: boolean;
    summary: number;
};

export const useGetOwnedAssetAssertionSummary = (): AssertionSummary => {
    return {
        loading: false,
        summary: 0,
    };
};
