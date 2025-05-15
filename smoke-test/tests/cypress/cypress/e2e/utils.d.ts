import { GraphQLRequest } from "../support/types";

export function hasOperationName(
  req: GraphQLRequest,
  operationName: string,
): boolean;
export function aliasQuery(req: GraphQLRequest, operationName: string): void;
