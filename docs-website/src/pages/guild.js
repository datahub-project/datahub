import React, { useEffect } from "react";

export default function Home() {
  useEffect(() => {
    window.location.href = "https://www.datahub.com/guild";
  }, []);

  return null;
}
