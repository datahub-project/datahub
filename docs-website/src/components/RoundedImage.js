import React from "react";
import clsx from "clsx";
import Image from "@theme/IdealImage";

const RoundedImage = (props) => (
  <Image
    className={clsx("shadow--tl")}
    style={{ borderRadius: "1rem", overflow: "hidden" }}
    {...props}
  />
);

export default RoundedImage;
