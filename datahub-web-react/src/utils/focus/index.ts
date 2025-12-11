/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

// To focus out from modal popup so autofocus works
// getContainer prop value when rendering modals.
// The getContainer prop allows you to specify the container in which the modal should be rendered.
// By default, modals are appended to the end of the document body, but using getContainer, you can specify a different container.
export const getModalDomContainer = () => document.getElementById('root') as HTMLElement;
// this can we remove once we upgrade to new antd version i.e 5.11.2 because there we have autoFocus property for the modal.
