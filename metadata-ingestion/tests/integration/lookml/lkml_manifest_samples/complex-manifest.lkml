project_name: "complex-manifest-project"

constant: CONNECTION_NAME {
  value: "choose-connection"
  export: override_required
}

constant: other_variable {
  value: "other-variable"
  export: override_required
}

local_dependency: {
  project: "looker-hub"
}

remote_dependency: remote-proj-1 {
  override_constant: schema_name {value: "mycorp_prod" }
  override_constant: choose-connection {value: "snowflake-conn-main"}
}

remote_dependency: remote-proj-2 {
}
