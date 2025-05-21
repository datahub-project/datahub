import React, { useEffect } from "react";
import { Redirect } from "@docusaurus/router";

export default function Home() {
  useEffect(() => {
    window.location.href = "/docs/features";
  }, []);

  return <Redirect to="/docs/features" />;
}
