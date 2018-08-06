/**
 * Primarily used in the dataset health tab, represents an individual health score entry for the table
 */
export interface IHealthScore {
  category: string;
  description: string;
  score: number;
  severity: string;
}
