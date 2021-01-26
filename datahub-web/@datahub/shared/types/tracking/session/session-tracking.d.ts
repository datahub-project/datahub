/**
 * Represents a session information object that is stored on local storage representing the user's session and
 * information related that is needed to maintain the correct session identifier
 */
export interface ISessionInfo {
  // Current session identifier
  sessionId: string;
  // Time of the last event, as a timestamp
  timeOfLastEvent: number;
}
