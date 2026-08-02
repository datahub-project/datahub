import React from "react";
import { Dropdown, Button, message } from "antd";
import type { MenuProps } from "antd";
import { CopyOutlined, DownOutlined } from "@ant-design/icons";
import styles from "./styles.module.css";

const CHATGPT_URL = "https://chatgpt.com/";
const CLAUDE_URL = "https://claude.ai/new";

// The page at /docs/<slug> has a companion clean-markdown file emitted by
// generateDocsDir.ts at /docs/<slug>.md. Derive it from the current location.
function getMarkdownUrl(): string {
  const { origin, pathname } = window.location;
  const trimmed = pathname.replace(/\/$/, "");
  return `${origin}${trimmed}.md`;
}

function aiPrompt(mdUrl: string): string {
  return encodeURIComponent(
    `Using this DataHub docs page as reference, help me with: ${mdUrl}`
  );
}

function openInNewTab(url: string): void {
  window.open(url, "_blank", "noopener,noreferrer");
}

export default function CopyPageDropdown(): JSX.Element {
  const [messageApi, contextHolder] = message.useMessage();

  async function copyAsMarkdown(): Promise<void> {
    try {
      const response = await fetch(getMarkdownUrl());
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      const text = await response.text();
      await navigator.clipboard.writeText(text);
      void messageApi.success("Copied page as Markdown");
    } catch {
      void messageApi.error("Could not copy this page as Markdown");
    }
  }

  const items: MenuProps["items"] = [
    { key: "copy", label: "Copy page as Markdown", icon: <CopyOutlined /> },
    { key: "view-md", label: "View as Markdown" },
    { key: "llms", label: "View llms.txt" },
    { type: "divider" },
    { key: "chatgpt", label: "Open in ChatGPT" },
    { key: "claude", label: "Open in Claude" },
  ];

  const onMenuClick: MenuProps["onClick"] = ({ key }) => {
    const mdUrl = getMarkdownUrl();
    switch (key) {
      case "copy":
        void copyAsMarkdown();
        break;
      case "view-md":
        openInNewTab(mdUrl);
        break;
      case "llms":
        openInNewTab(`${window.location.origin}/llms.txt`);
        break;
      case "chatgpt":
        openInNewTab(`${CHATGPT_URL}?q=${aiPrompt(mdUrl)}`);
        break;
      case "claude":
        openInNewTab(`${CLAUDE_URL}?q=${aiPrompt(mdUrl)}`);
        break;
      default:
        break;
    }
  };

  return (
    <div className={styles.copyPageWrapper}>
      {contextHolder}
      <Dropdown
        menu={{ items, onClick: onMenuClick }}
        trigger={["click"]}
        placement="bottomRight"
      >
        <Button
          size="small"
          className={styles.copyPageButton}
          icon={<CopyOutlined />}
          aria-label="Copy page for AI tools"
        >
          <span className={styles.copyPageLabel}>Copy page</span>
          <DownOutlined className={styles.copyPageChevron} />
        </Button>
      </Dropdown>
    </div>
  );
}
