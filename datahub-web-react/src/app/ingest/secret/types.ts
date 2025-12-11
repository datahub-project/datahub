/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

/**
 * The object represents the state of the Ingestion Source Builder form.
 */
export interface SecretBuilderState {
    /**
     * The name of the secret.
     */
    urn?: string;
    /**
     * The name of the secret.
     */
    name?: string;

    /**
     * The value of the secret.
     */
    value?: string;

    /**
     * An optional description for the secret.
     */
    description?: string;
}
