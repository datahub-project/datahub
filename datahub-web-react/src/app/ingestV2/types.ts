export enum TabType {
    Sources = 'Sources',
    ExecutionLog = 'ExecutionLog',
    Secrets = 'Secrets',
    RemoteExecutors = 'Executors',
}

export const tabUrlMap = {
    [TabType.Sources]: '/ingestion/sources',
    [TabType.ExecutionLog]: '/ingestion/execution-log',
    [TabType.Secrets]: '/ingestion/secrets',
};
