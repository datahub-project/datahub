/**
 * Utilities for parsing AI message content including XML thinking blocks,
 * tool calls, and markdown.
 */

import { XMLParser } from 'fast-xml-parser';

export interface ReasoningStructure {
  action?: string;
  rationale?: string;
  confidence?: 'high' | 'medium' | 'low';
  plan_id?: string;
  step?: string;
  [key: string]: string | undefined;
}

export interface ThinkingBlock {
  content: string;
  raw: string;
  structured?: ReasoningStructure;
  tokens?: number;
  duration?: number;
  wallClockTime?: number;
  eventId?: string;
}

export interface ToolCall {
  name: string;
  input: Record<string, any>;
  timestamp?: number;
  tokens?: number;
  duration?: number;
  wallClockTime?: number;
  resultTokens?: number;
  eventId?: string;
}

export interface ToolResult {
  tool_name: string;
  result: any;
  error?: string;
  timestamp?: number;
  duration_ms?: number;
}

export type MessageEvent =
  | { type: 'thinking'; data: ThinkingBlock; position: number }
  | { type: 'tool_call'; data: ToolCall; position: number };

export interface ParsedMessage {
  events: MessageEvent[];  // Ordered events (thinking + tool calls)
  thinking?: ThinkingBlock[];  // Deprecated - kept for backward compat
  toolCalls?: ToolCall[];  // Deprecated - kept for backward compat
  toolResults?: ToolResult[];
  finalText?: string;
  rawText: string;
  totalDuration?: number;  // Total duration in seconds (from <metadata> for telemetry data)
}

// Create XML parser instance
const xmlParser = new XMLParser({
  ignoreAttributes: false,
  trimValues: true,
  parseTagValue: true,
  parseAttributeValue: false,
  cdataPropName: '__cdata',
});

/**
 * Parse reasoning XML structure into structured data using proper XML parser
 */
function parseReasoningStructure(content: string): ReasoningStructure | undefined {
  console.log('parseReasoningStructure called with:', {
    contentPreview: content.substring(0, 200),
    hasReasoningTag: content.includes('<reasoning>'),
    contentLength: content.length
  });

  if (!content.includes('<reasoning>')) {
    console.log('No <reasoning> tag found');
    return undefined;
  }

  try {
    // Parse the XML content
    const parsed = xmlParser.parse(content);
    console.log('XML parsed successfully:', parsed);

    // Extract reasoning object
    const reasoning = parsed.reasoning;
    if (!reasoning) {
      console.log('No reasoning object in parsed XML');
      return undefined;
    }

    // Build structured data from parsed XML
    const structured: ReasoningStructure = {};

    if (reasoning.action) {
      structured.action = String(reasoning.action).trim();
      console.log('Found action:', structured.action.substring(0, 50));
    }

    if (reasoning.rationale) {
      structured.rationale = String(reasoning.rationale).trim();
      console.log('Found rationale:', structured.rationale.substring(0, 50));
    }

    if (reasoning.confidence) {
      const conf = String(reasoning.confidence).toLowerCase().trim();
      structured.confidence = conf as 'high' | 'medium' | 'low';
      console.log('Found confidence:', structured.confidence);
    }

    if (reasoning.plan_id) {
      structured.plan_id = String(reasoning.plan_id).trim();
      console.log('Found plan_id:', structured.plan_id);
    }

    if (reasoning.step) {
      structured.step = String(reasoning.step).trim();
      console.log('Found step:', structured.step);
    }

    console.log('Parsed structured data:', structured);
    return Object.keys(structured).length > 0 ? structured : undefined;

  } catch (error) {
    console.error('XML parsing error:', error);
    return undefined;
  }
}

/**
 * Extract thinking blocks from XML tags with metadata
 */
export function extractThinkingBlocks(text: string): ThinkingBlock[] {
  const thinkingBlocks: ThinkingBlock[] = [];
  const thinkingRegex = /<thinking([^>]*)>([\s\S]*?)<\/thinking>/g;

  let match;
  while ((match = thinkingRegex.exec(text)) !== null) {
    const attributes = match[1];
    const content = match[2].trim();

    // Extract metadata from attributes
    const tokensMatch = /tokens="([^"]*)"/.exec(attributes);
    const durationMatch = /duration="([^"]*)"/.exec(attributes);
    const wallClockTimeMatch = /wallClockTime="([^"]*)"/.exec(attributes);
    const eventIdMatch = /eventId="([^"]*)"/.exec(attributes);

    thinkingBlocks.push({
      content,
      raw: match[0],
      structured: parseReasoningStructure(content),
      tokens: tokensMatch ? parseInt(tokensMatch[1], 10) : undefined,
      duration: durationMatch ? parseFloat(durationMatch[1]) : undefined,
      wallClockTime: wallClockTimeMatch ? parseFloat(wallClockTimeMatch[1]) : undefined,
      eventId: eventIdMatch ? eventIdMatch[1] : undefined,
    });
  }

  return thinkingBlocks;
}

/**
 * Extract tool use blocks from XML tags with metadata
 */
export function extractToolCalls(text: string): ToolCall[] {
  const toolCalls: ToolCall[] = [];
  const toolRegex = /<tool_use([^>]*)>([\s\S]*?)<\/tool_use>/g;

  let match;
  while ((match = toolRegex.exec(text)) !== null) {
    const attributes = match[1];
    const toolContent = match[2];

    // Extract metadata from attributes
    const tokensMatch = /tokens="([^"]*)"/.exec(attributes);
    const durationMatch = /duration="([^"]*)"/.exec(attributes);
    const wallClockTimeMatch = /wallClockTime="([^"]*)"/.exec(attributes);
    const resultTokensMatch = /resultTokens="([^"]*)"/.exec(attributes);
    const eventIdMatch = /eventId="([^"]*)"/.exec(attributes);

    // Extract tool name
    const nameMatch = /<tool_name>(.*?)<\/tool_name>/.exec(toolContent);
    const name = nameMatch ? nameMatch[1].trim() : 'unknown';

    // Extract parameters
    const paramsMatch = /<parameters>([\s\S]*?)<\/parameters>/.exec(toolContent);
    let input: Record<string, any> = {};

    if (paramsMatch) {
      try {
        input = JSON.parse(paramsMatch[1].trim());
      } catch (e) {
        // If not JSON, parse as XML parameters
        input = parseXmlParameters(paramsMatch[1]);
      }
    }

    toolCalls.push({
      name,
      input,
      timestamp: Date.now(),
      tokens: tokensMatch ? parseInt(tokensMatch[1], 10) : undefined,
      duration: durationMatch ? parseFloat(durationMatch[1]) : undefined,
      wallClockTime: wallClockTimeMatch ? parseFloat(wallClockTimeMatch[1]) : undefined,
      resultTokens: resultTokensMatch ? parseInt(resultTokensMatch[1], 10) : undefined,
      eventId: eventIdMatch ? eventIdMatch[1] : undefined,
    });
  }

  return toolCalls;
}

/**
 * Parse XML parameters into object
 */
function parseXmlParameters(xmlText: string): Record<string, any> {
  const params: Record<string, any> = {};
  const paramRegex = /<(\w+)>([\s\S]*?)<\/\1>/g;

  let match;
  while ((match = paramRegex.exec(xmlText)) !== null) {
    const key = match[1];
    const value = match[2].trim();

    // Try to parse as JSON if it looks like an array or object
    if ((value.startsWith('[') && value.endsWith(']')) ||
        (value.startsWith('{') && value.endsWith('}'))) {
      try {
        params[key] = JSON.parse(value);
      } catch {
        params[key] = value;
      }
    } else {
      params[key] = value;
    }
  }

  return params;
}

/**
 * Remove XML tags from text to get clean final message
 */
export function removeXmlTags(text: string): string {
  return text
    .replace(/<metadata[^>]*>[\s\S]*?<\/metadata>/g, '')
    .replace(/<thinking[^>]*>[\s\S]*?<\/thinking>/g, '')
    .replace(/<tool_use[^>]*>[\s\S]*?<\/tool_use>/g, '')
    .replace(/<tool_result[^>]*>[\s\S]*?<\/tool_result>/g, '')
    .trim();
}

/**
 * Extract all events (thinking + tool calls) in chronological order with metadata
 */
export function extractEventsInOrder(text: string): MessageEvent[] {
  const events: MessageEvent[] = [];

  // Find all thinking blocks with positions and metadata
  const thinkingRegex = /<thinking([^>]*)>([\s\S]*?)<\/thinking>/g;
  let match;
  while ((match = thinkingRegex.exec(text)) !== null) {
    const attributes = match[1];
    const content = match[2].trim();

    // Extract metadata from attributes
    const tokensMatch = /tokens="([^"]*)"/.exec(attributes);
    const durationMatch = /duration="([^"]*)"/.exec(attributes);
    const wallClockTimeMatch = /wallClockTime="([^"]*)"/.exec(attributes);
    const eventIdMatch = /eventId="([^"]*)"/.exec(attributes);

    events.push({
      type: 'thinking',
      data: {
        content,
        raw: match[0],
        structured: parseReasoningStructure(content),
        tokens: tokensMatch ? parseInt(tokensMatch[1], 10) : undefined,
        duration: durationMatch ? parseFloat(durationMatch[1]) : undefined,
        wallClockTime: wallClockTimeMatch ? parseFloat(wallClockTimeMatch[1]) : undefined,
        eventId: eventIdMatch ? eventIdMatch[1] : undefined,
      },
      position: match.index
    });
  }

  // Find all tool calls with positions and metadata
  const toolRegex = /<tool_use([^>]*)>([\s\S]*?)<\/tool_use>/g;
  while ((match = toolRegex.exec(text)) !== null) {
    const attributes = match[1];
    const toolContent = match[2];

    // Extract metadata from attributes
    const tokensMatch = /tokens="([^"]*)"/.exec(attributes);
    const durationMatch = /duration="([^"]*)"/.exec(attributes);
    const wallClockTimeMatch = /wallClockTime="([^"]*)"/.exec(attributes);
    const resultTokensMatch = /resultTokens="([^"]*)"/.exec(attributes);
    const eventIdMatch = /eventId="([^"]*)"/.exec(attributes);

    // Extract tool name
    const nameMatch = /<tool_name>(.*?)<\/tool_name>/.exec(toolContent);
    const name = nameMatch ? nameMatch[1].trim() : 'unknown';

    // Extract parameters
    const paramsMatch = /<parameters>([\s\S]*?)<\/parameters>/.exec(toolContent);
    let input: Record<string, any> = {};

    if (paramsMatch) {
      try {
        input = JSON.parse(paramsMatch[1].trim());
      } catch (e) {
        // If not JSON, parse as XML parameters
        input = parseXmlParameters(paramsMatch[1]);
      }
    }

    events.push({
      type: 'tool_call',
      data: {
        name,
        input,
        timestamp: Date.now(),
        tokens: tokensMatch ? parseInt(tokensMatch[1], 10) : undefined,
        duration: durationMatch ? parseFloat(durationMatch[1]) : undefined,
        wallClockTime: wallClockTimeMatch ? parseFloat(wallClockTimeMatch[1]) : undefined,
        resultTokens: resultTokensMatch ? parseInt(resultTokensMatch[1], 10) : undefined,
        eventId: eventIdMatch ? eventIdMatch[1] : undefined,
      },
      position: match.index
    });
  }

  // Sort by position to maintain chronological order
  events.sort((a, b) => a.position - b.position);

  return events;
}

/**
 * Extract total duration from metadata tag if present
 */
function extractTotalDuration(text: string): number | undefined {
  const metadataRegex = /<metadata[^>]*totalDuration="([^"]*)"[^>]*>/;
  const match = metadataRegex.exec(text);
  if (match) {
    const duration = parseFloat(match[1]);
    return isNaN(duration) ? undefined : duration;
  }
  return undefined;
}

/**
 * Parse a complete AI message with thinking, tool calls, and final text
 */
export function parseAiMessage(text: string): ParsedMessage {
  const events = extractEventsInOrder(text);
  const thinking = extractThinkingBlocks(text);
  const toolCalls = extractToolCalls(text);
  const finalText = removeXmlTags(text);
  const totalDuration = extractTotalDuration(text);

  return {
    events,
    thinking,  // Kept for backward compat
    toolCalls,  // Kept for backward compat
    finalText: finalText || undefined,
    rawText: text,
    totalDuration
  };
}

/**
 * Format duration in milliseconds to human-readable string
 */
export function formatDuration(ms: number): string {
  if (ms < 1000) {
    return `${ms}ms`;
  } else if (ms < 60000) {
    return `${(ms / 1000).toFixed(1)}s`;
  } else {
    const minutes = Math.floor(ms / 60000);
    const seconds = ((ms % 60000) / 1000).toFixed(0);
    return `${minutes}m ${seconds}s`;
  }
}

/**
 * Estimate token count (rough heuristic: ~4 chars per token)
 */
export function estimateTokenCount(text: string): number {
  return Math.ceil(text.length / 4);
}

/**
 * Abbreviate large numbers
 */
export function abbreviateNumber(num: number): string {
  if (num >= 1000000) {
    return `${(num / 1000000).toFixed(1)}M`;
  } else if (num >= 1000) {
    return `${(num / 1000).toFixed(1)}K`;
  }
  return num.toString();
}

/**
 * Format token count with abbreviation
 */
export function formatTokenCount(text: string): string {
  const count = estimateTokenCount(text);
  return `${abbreviateNumber(count)} tokens`;
}

/**
 * Try to parse string as JSON, return null if invalid
 */
export function tryParseJson(text: string): any {
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

/**
 * Format relative time from ISO timestamp to human-friendly string
 * @param isoTimestamp ISO 8601 timestamp string
 * @returns Human-friendly relative time (e.g., "2 mins ago", "just now")
 */
export function formatRelativeTime(isoTimestamp: string): string {
  const now = new Date();
  const then = new Date(isoTimestamp);
  const seconds = Math.floor((now.getTime() - then.getTime()) / 1000);

  if (seconds < 10) return 'just now';
  if (seconds < 60) return `${seconds}s ago`;

  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes} min${minutes === 1 ? '' : 's'} ago`;

  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours} hr${hours === 1 ? '' : 's'} ago`;

  const days = Math.floor(hours / 24);
  return `${days} day${days === 1 ? '' : 's'} ago`;
}
