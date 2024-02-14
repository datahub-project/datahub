export type DocumentationRequestSummary = {
    loading: boolean;
    count: number;
};

// TODO: Populate this based on Chris' work.
export const useGetPendingDocumentationRequests = (): DocumentationRequestSummary => {
    return {
        loading: false,
        count: 2,
    };
};
