import React from "react";
import Image from "@theme/IdealImage";

const RoundedImage = (props) => <Image className="shadow--tl" style={{ borderRadius: "1rem", overflow: "hidden" }} {...props} />;

export default RoundedImage;
