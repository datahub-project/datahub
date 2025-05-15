import dayjs from "dayjs";
import { GraphQLRequest } from "./types";

export function hasOperationName(
  req: GraphQLRequest,
  operationName: string,
): boolean {
  const { body } = req;
  return (
    body.hasOwnProperty("operationName") && body.operationName === operationName
  );
}

export function aliasQuery(req: GraphQLRequest, operationName: string): void {
  if (hasOperationName(req, operationName)) {
    req.alias = `gql${operationName}Query`;
  }
}

export function getTimestampMillisNumDaysAgo(numDays: number): number {
  return dayjs().subtract(numDays, "day").valueOf();
}
