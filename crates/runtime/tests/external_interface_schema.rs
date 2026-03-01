use std::fs;
use std::path::{Path, PathBuf};

use jsonschema::JSONSchema;
use serde_json::Value;

#[test]
fn valid_interface_fixtures_match_external_interface_schema() {
    let compiled_schema = compiled_external_interface_schema();
    let fixture_dir = fixtures_dir().join("interfaces");

    let mut fixture_paths = list_json_files(&fixture_dir)
        .into_iter()
        .filter(|path| !path.to_string_lossy().contains("/invalid/"))
        .collect::<Vec<_>>();
    fixture_paths.sort();

    assert!(
        !fixture_paths.is_empty(),
        "expected at least one valid interface fixture"
    );

    for fixture_path in fixture_paths {
        let fixture_json = read_json(&fixture_path);
        let result = compiled_schema.validate(&fixture_json);
        assert!(
            result.is_ok(),
            "fixture should satisfy schema: {}",
            fixture_path.display()
        );
    }
}

#[test]
fn invalid_interface_fixtures_fail_external_interface_schema() {
    let compiled_schema = compiled_external_interface_schema();
    let invalid_dir = fixtures_dir().join("interfaces/invalid");

    let mut invalid_paths = list_json_files(&invalid_dir);
    invalid_paths.sort();

    assert!(
        !invalid_paths.is_empty(),
        "expected invalid interface fixtures under {}",
        invalid_dir.display()
    );

    for invalid_path in invalid_paths {
        let invalid_json = read_json(&invalid_path);
        let result = compiled_schema.validate(&invalid_json);
        assert!(
            result.is_err(),
            "invalid fixture unexpectedly passed schema: {}",
            invalid_path.display()
        );
    }
}

fn compiled_external_interface_schema() -> JSONSchema {
    let schema_path = repo_root().join("system/schemas/external_interface.schema.json");
    let schema_json = read_json(&schema_path);
    JSONSchema::compile(&schema_json).expect("external interface schema should compile")
}

fn read_json(path: &Path) -> Value {
    let content = fs::read_to_string(path)
        .unwrap_or_else(|_| panic!("failed to read json file: {}", path.display()));
    serde_json::from_str(&content)
        .unwrap_or_else(|_| panic!("failed to parse json file: {}", path.display()))
}

fn list_json_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();

    if !dir.exists() {
        return files;
    }

    let entries =
        fs::read_dir(dir).unwrap_or_else(|_| panic!("failed to read directory: {}", dir.display()));

    for entry in entries {
        let path = entry
            .unwrap_or_else(|_| panic!("failed to read entry under: {}", dir.display()))
            .path();
        if path.is_dir() {
            files.extend(list_json_files(&path));
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) == Some("json") {
            files.push(path);
        }
    }

    files
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("failed to resolve repository root")
}

fn fixtures_dir() -> PathBuf {
    repo_root().join("tests/fixtures")
}
