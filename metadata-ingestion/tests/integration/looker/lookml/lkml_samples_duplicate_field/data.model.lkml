connection: "my_connection"

include: "dataset_lineages.view.lkml"

explore: explore_dataset_lineage {
    from: dataset_lineages
}
