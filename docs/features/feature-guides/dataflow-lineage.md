# Data Pipeline Lineage

DataHub now supports visualizing your data pipelines' (e.g. Airflow DAG) task dependence! On a data pipeline's
entity page, go to the `Lineage` tab to open the visualization. At its center will be the data pipeline node,
represented as a box containing all of its composite tasks. You can click and drag to move each task within the box,
as well as click and drag the data pipeline box itself.

<p align="center">
<img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/dataflow-lineage-tab.png" />
</p>

Further, you can leverage DataHub's cross-platform lineage to view the upstreams and downstreams of each task.
The number on the expand lineage button represents how many data-dependence upstreams / downstreams the task has.
After expanding, you can keep expanding lineage further like the standard lineage explorer.

<p align="center">
<img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/dataflow-lineage-expand.png" />
</p>

Note that you can only expand one task's upstreams and one task's downstreams at a time, to keep the visualization simple.

<p align="center">
<img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/dataflow-lineage-expand-2.png" />
</p>
