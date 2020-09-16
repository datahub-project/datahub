/**
 * Represents one frequency item in the execution flow
 */
export interface ITimeFrame {
  latestStatus: string;
  time: number;
  latestExeUrl?: string;
  executionsHistory?: {
    flowStatusSummary: Array<IExecutionHistory>;
  };
}

/**
 * Execution history interface from the execution flow
 */
export interface IExecutionHistory {
  execId: string;
  project?: string;
  flowExecutionUrl: string;
  flowExecutionStatus: string;
  flowExecutionLastUpdated: number;
  application: string;
}

/**
 * Execution flow response coming from UMP-Monitor
 */
export interface IExecutionFlow {
  cluster: string;
  dataset: string;
  flowName: string;
  frequency: string;
  hasPermission?: boolean;
  timeFrame: Array<ITimeFrame>;
}

/**
 * UMP-Monitor query reponse container
 */
export interface IExecutionFlowResponse {
  flowStatusResponseDataset: Array<IExecutionFlow>;
}
