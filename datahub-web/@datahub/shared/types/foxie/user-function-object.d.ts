import { UserFunctionType } from '@datahub/shared/constants/foxie/user-function-type';

/**
 * These objects are triggered by the user actions, or the resulting consequences of those actions, and created and
 * fired from the various UI components by calling an associated method on the Foxie service.
 *
 * @example
 * Such an object could denote an empty response for datasets search:
 * {
 *   functionType: UserFunctionType.ApiResponse
 *   functionTarget: DatasetEntity.displayName
 *   functionContext: 'empty response'
 * }
 */
export interface IUserFunctionObject {
  functionType: UserFunctionType;
  // The target of the function can be a specific route name, component name or some other item that gives context to
  // the function’s intent
  functionTarget: string;
  // Potentially provides additional context for the why or how this object exists
  functionContext?: string;
}
