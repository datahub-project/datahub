import React from "react";
import clsx from "clsx";
import useBaseUrl from "@docusaurus/useBaseUrl";
import Link from "@docusaurus/Link";
import styles from "./quicklinkcards.module.scss";
import FilterCard from "../FilterCard";


// var prevFilterState = ""
// if(componentDidUpdate(filterBar, prevFilterState)){
//   for (title in cards.keys){
//       cards[title].props.showInfo = title.includes(filterBar.searchQuery);
//   }
// }

// var cards = Object.assign({}, ...RTCPeerConnectionIceEvent.map((props, idx) => ({[props.title]:  <FilterCard key={idx} {...props} />})))


const FilterCards = ({content, filterBar}) =>
  content?.length > 0 ? (
    <div style={{ padding: "2vh 0" }}>
      <div className="container">
        <div className="row row--no-gutters">
          {content.map((props, idx) => (
            <FilterCard key={idx} {...props} />
          ))}
        </div>
      </div>
    </div>
  ) : null;

export default FilterCards;
