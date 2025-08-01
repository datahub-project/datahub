export enum TabType {
    Sources = 'Sources',
    RunHistory = 'RunHistory',
    Secrets = 'Secrets',
    RemoteExecutors = 'Executors',
}

export const tabUrlMap = {
    [TabType.Sources]: '/ingestion/sources',
    [TabType.RunHistory]: '/ingestion/run-history',
    [TabType.Secrets]: '/ingestion/secrets',
};
