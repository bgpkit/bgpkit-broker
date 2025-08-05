# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

BGPKIT Broker is a Rust library and CLI tool that provides indexing and searching functionalities for public BGP data archive files. It consists of:
- A Rust SDK for querying BGP archive data
- A CLI tool for self-hosting a broker instance with RESTful API
- Backend services for crawling, database management, and notifications

## Key Commands

### Building
```bash
# Build the library only
cargo build --verbose

# Build with CLI features
cargo build --features cli --verbose

# Build everything
cargo build --all-features
```

### Testing
```bash
# Test the SDK only (no CLI features)
cargo test --no-default-features --verbose

# Run all tests
cargo test --all-features --verbose

# Run a specific test
cargo test test_name --verbose
```

### Linting
```bash
# Run clippy with all features
cargo clippy --all-features -- -D warnings
```

### Running Examples
```bash
cargo run --example query
cargo run --example latest
cargo run --example peers
```

## Architecture Overview

### Core Components

1. **Library Core** (`src/lib.rs`)
   - Main SDK interface through `BgpkitBroker` struct
   - Iterator-based API for streaming BGP archive items
   - Query builder pattern for search parameters

2. **CLI Application** (`src/cli/`)
   - `main.rs`: Entry point with subcommands (serve, update, bootstrap, backup, search, etc.)
   - `api.rs`: RESTful API implementation using Axum
   - `bootstrap.rs`: Database initialization from remote backups
   - `backup.rs`: Database backup functionality

3. **Data Models**
   - `item.rs`: Core `BrokerItem` struct representing BGP archive files
   - `collector.rs`: BGP collector information
   - `peer.rs`: BGP peer information
   - `query.rs`: Query builder implementation

4. **Backend Services** (feature-gated)
   - `crawler/`: Web crawlers for RouteViews and RIPE RIS
   - `db/`: SQLite database layer using SQLx
   - `notifier/`: NATS-based notification system

### Feature Flags

- Default: Core SDK only
- `cli`: Enables all CLI functionality including API server
- `backend`: Database operations with SQLx
- `nats`: NATS notification support

### Database

Uses SQLite with the following key tables:
- `broker_items`: Main table for BGP archive files
- `latest_files`: Tracks latest files per collector
- `meta`: Metadata including version and last update time

### API Endpoints

When running `bgpkit-broker serve`:
- Default port: 40064
- Health check: `/health`
- Main search: `/v3/search`
- Latest files: `/v3/latest`
- Peers info: `/v3/peers`

## Environment Variables

For NATS notifications:
- `BGPKIT_BROKER_NATS_URL`: NATS server URL
- `BGPKIT_BROKER_NATS_USER`: NATS username
- `BGPKIT_BROKER_NATS_PASSWORD`: NATS password
- `BGPKIT_BROKER_NATS_ROOT_SUBJECT`: Root subject for messages

## Development Tips

1. The project uses async Rust with Tokio for the CLI/backend features
2. API uses Axum web framework with Tower middleware
3. Database queries use SQLx with compile-time checked SQL
4. Crawlers parse HTML using scraper crate
5. All timestamps are handled as Unix timestamps (i64)

## Development Workflow Preferences

### Code Quality
- Always run `cargo fmt` after finishing each round of code editing
- Run clippy checks before committing changes
- **IMPORTANT**: Before committing any changes, run all relevant tests and checks from `.github/workflows/rust.yaml`:
  - `cargo fmt --check` - Check code formatting
  - `cargo build --no-default-features` - Build with no features
  - `cargo build` - Build with default features
  - `cargo test` - Run all tests
  - `cargo clippy --all-features -- -D warnings` - Run clippy on all features
  - `cargo clippy --no-default-features` - Run clippy with no features
  - Fix any issues before committing

### Documentation
- Update CHANGELOG.md when implementing fixes or features
- Add changes to the "Unreleased changes" section with appropriate subsections (Feature flags, Bug fixes, Code improvements, etc.)
- **IMPORTANT**: When changing lib.rs documentation, always run `cargo readme > README.md` and commit the README.md changes with a simple message "docs: update README.md from lib.rs documentation"

### Git Operations
- Do not prompt for git operations unless explicitly requested by the user
- Let the user initiate commits and other git actions when they're ready
- **IMPORTANT**: When pushing commits, always list all commits to be pushed first using `git log --oneline origin/[branch]..HEAD` and ask for user confirmation

### Commit Messages and Changelog Writing Guidelines
- **Keep language factual and professional**: Avoid subjective or exaggerated descriptive words
- **Avoid words like**: "comprehensive", "extensive", "amazing", "powerful", "robust", "excellent", etc.
- **Use objective language**: State what was added, changed, or fixed without editorial commentary
- **Good examples**: "Added RPKI documentation", "Fixed validation logic", "Updated error handling"
- **Poor examples**: "Added comprehensive RPKI documentation", "Significantly improved validation", "Enhanced robust error handling"
- **Exception**: Technical precision words are acceptable when factually accurate (e.g., "efficient lookup", "atomic operation")

### Release Process
When preparing a release, follow these steps in order:
1. **Update CHANGELOG.md**:
   - Move all "Unreleased changes" to a new version section with the release version number and date
   - Add any missing changes that were implemented but not documented
   - Follow the existing format: `## v[VERSION] - YYYY-MM-DD`
2. **Update Cargo.toml**:
   - Update the `version` field to the new version number
   - Follow semantic versioning (major.minor.patch)
3. **Review changes before committing**:
   - Run `git diff` to show all changes
   - Ask the user to confirm the diff is correct
   - Check for accidental version mismatches or unwanted changelog entries
4. **Commit the release preparation**:
   - After user confirmation, commit with message: `release: prepare v[VERSION]`
5. **Create and push git tag**:
   - Create a new git tag with the version number: `git tag v[VERSION]`
   - Push commits first: `git push origin [branch-name]`
   - Then push the tag: `git push origin v[VERSION]`
