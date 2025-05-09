import React, { useEffect } from "react";

export default function Home() {
  useEffect(() => {
    window.location.href = "https://datahub.com/slack";
  }, []);

  return null;
}
