import React from "react";
import clsx from "clsx";
import styles from "./styles.module.scss";
import RoundedImage from "../../_components/RoundedImage";


const Features = () =>
    <div className="container">
    <div className="row row--padded row--centered">
      <div className="col col--6">
        <div className={clsx(styles.title)}>
          <img className={clsx(styles.icon)} src={"/img/assets/data-discovery.svg"}></img>
          <div className={clsx(styles.titleText)}>Data Discovery</div>
        </div>
        <RoundedImage
          img={require("/img/screenshots/lineage.png")}
          alt="DataHub Lineage Screenshot"
        />
      </div>
      <div className="col col--5 col--offset-1">
        <h2>
          <span>Discover Trusted Data</span>
        </h2>
        <p>
          Browse and search over a continuously updated catalog of
          datasets, dashboards, charts, ML models, and more.
        </p>
      </div>
    </div>

    <div className="row row--padded row--centered">
      <div className="col col--5">
        <h2>
          <span>Understand Data in Context</span>
        </h2>
        <p>
          DataHub is the one-stop shop for documentation, schemas,
          ownership, data lineage, pipelines, data quality, usage
          information, and more.
        </p>
      </div>
      <div className="col col--6 col--offset-1">
        <div className={clsx(styles.title)}>
          <img className={clsx(styles.icon)} src={"/img/assets/data-governance.svg"}></img>
          <div className={clsx(styles.titleText)}>Data Governance</div>
        </div>
        <RoundedImage
          img={require("/img/screenshots/metadata.png")}
          alt="DataHub Metadata Screenshot"
        />
      </div>
    </div>
    <div className="row row--padded row--centered">
      <div className="col col--6">
        <div className={clsx(styles.title)}>
          <img className={clsx(styles.icon)} src={"/img/assets/data-ob.svg"}></img>
          <div className={clsx(styles.titleText)}>Data Observability</div>
        </div>
        <RoundedImage
          img={require("/img/screenshots/lineage.png")}
          alt="DataHub Lineage Screenshot"
        />
      </div>
      <div className="col col--5 col--offset-1">
        <h2>
          <span>Discover Trusted Data</span>
        </h2>
        <p>
          Browse and search over a continuously updated catalog of
          datasets, dashboards, charts, ML models, and more.
        </p>
      </div>
    </div>
  </div>

export default Features;
