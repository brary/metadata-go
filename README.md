# Metadata-Go

A lightweight metadata database built on top of TiKV, providing table constructs and transactional capabilities without the SQL parsing overhead.

## Prerequisites

- Go 1.21 or later
- TiKV cluster (can be local or remote)

## Installation

1. Install Go:
   ```bash
   # For macOS using Homebrew
   brew install go
   ```

2. Clone the repository:
   ```bash
   git clone https://github.com/avneetkang/metadata-go.git
   cd metadata-go
   ```

3. Install dependencies:
   ```bash
   go mod download
   ```

## Project Structure

- `pkg/table/` - Table management and schema definitions
- `pkg/transaction/` - Transaction management layer
- `pkg/storage/` - TiKV storage integration
- `pkg/metadata/` - Core metadata management

## Usage

[Documentation coming soon]

## License

MIT