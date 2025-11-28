import React, { useState, useEffect } from "react";
import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import styles from "./styles.module.css";

const OSDetectionTabs = ({ children, defaultOS = null }) => {
  // Detect OS immediately during initialization
  const detectOS = () => {
    if (typeof window === "undefined") return "linux"; // SSR fallback

    const userAgent = window.navigator.userAgent;
    const platform = window.navigator.platform;

    console.log("Detecting OS - UserAgent:", userAgent, "Platform:", platform);

    // More specific macOS detection
    if (
      platform.indexOf("Mac") !== -1 ||
      userAgent.indexOf("Mac") !== -1 ||
      userAgent.indexOf("macOS") !== -1 ||
      platform === "MacIntel" ||
      platform === "MacPPC"
    ) {
      return "macos";
    } else if (
      userAgent.indexOf("Win") !== -1 ||
      platform.indexOf("Win") !== -1
    ) {
      return "windows";
    } else if (
      userAgent.indexOf("Linux") !== -1 ||
      platform.indexOf("Linux") !== -1
    ) {
      return "linux";
    } else {
      return "linux"; // Default fallback
    }
  };

  const [detectedOS, setDetectedOS] = useState(() => detectOS());
  const [defaultValue, setDefaultValue] = useState(
    () => defaultOS || detectOS(),
  );

  useEffect(() => {
    // Re-detect OS on client side to handle SSR
    const os = detectOS();
    console.log("Detected OS:", os);
    setDetectedOS(os);

    // Set default tab to detected OS if no explicit default provided
    if (!defaultOS) {
      setDefaultValue(os);
    }
  }, [defaultOS]);

  // Get OS icon
  const getOSIcon = (osValue) => {
    switch (osValue) {
      case "windows":
        return "🪟";
      case "macos":
        return "🍎";
      case "linux":
        return "🐧";
      default:
        return "";
    }
  };

  // Add OS detection info to child components
  const enhancedChildren = React.Children.map(children, (child) => {
    if (React.isValidElement(child) && child.type === TabItem) {
      const isDetected = child.props.value === detectedOS;
      const icon = getOSIcon(child.props.value);
      const label = isDetected
        ? `${icon} ${child.props.label} (Your OS)`
        : `${icon} ${child.props.label}`;

      return React.cloneElement(child, {
        ...child.props,
        label,
        className: isDetected ? styles.detectedTab : "",
      });
    }
    return child;
  });

  console.log(
    "Rendering OSDetectionTabs with defaultValue:",
    defaultValue,
    "detectedOS:",
    detectedOS,
  );

  return (
    <Tabs
      defaultValue={defaultValue}
      groupId="os-detection-tabs"
      key={defaultValue} // Force re-render when defaultValue changes
    >
      {enhancedChildren}
    </Tabs>
  );
};

export default OSDetectionTabs;
