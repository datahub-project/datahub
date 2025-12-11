/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

/**
 * Custom User Context State - This is a custom user context state and can be overriden in specific fork of DataHub.
 * The below type can be customized with specific object properties as well if needed.
 */
export type CustomUserContextState = Record<string, any>;

export const DEFAULT_CUSTOM_STATE: CustomUserContextState = {};
