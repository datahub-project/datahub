/**
 * This object is a raw response object for a health score detail from the api layer that will
 * be used mainly in our metadata health score table
 */
export interface IHealthScoreObject {
  tier: string | null;
  // score will be a float between 0 and 1 representing a percentage
  score: number;
  description: string;
  weight: number;
  validator: string;
}

/**
 * This is the abstracted response from the api request to datasets/:urn/health
 */
export interface IDatasetHealth {
  // score will be a float between 0 and 1 representing a percetnage
  score: number;
  validations: Array<IHealthScoreObject>;
}

/**
 * This is the raw response from the api layer for a request to datasets/:urn/health
 */
export interface IHealthScoreResponse {
  health: IDatasetHealth;
}

/**
 * Primarily used in the dataset health tab, represents an individual health score entry for the table
 */
export interface IHealthScore {
  category: string;
  description: string;
  score: number;
  severity: string;
}
