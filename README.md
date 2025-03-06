# VarNoiseDB

VarNoiseDB is a tool for managing allele depth noise from sequencing of tumors. It supports SQLite, PostgreSQL, and MySQL databases.

## Overview

VarNoiseDB is designed to help bioinformaticians and researchers analyze allele frequency noise patterns in sequencing data. By collecting data from multiple samples, it builds a comprehensive database of noise profiles that can be used to distinguish true variants from sequencing artifacts.

Key features:
- Load allele frequency data from GVCF files into a SQL database
- Calculate and store statistics like mean, max, and min non-reference allele frequencies
- Track the samples where each variant has been observed
- Export data in VCF format with detailed annotations
- Support for multiple database backends (SQLite, PostgreSQL, MySQL)

## Installation

### Prerequisites

- Python 3.10 or higher
- pip or Poetry package manager

### Using Poetry (recommended)

```bash
git clone https://github.com/gmc-norr/VarNoiseDB.git
cd VarNoiseDB

# Install dependencies and the package
poetry install
```

### Using Pip
```bash
git clone https://github.com/gmc-norr/VarNoiseDB.git
cd VarNoiseDB

pip install .
```

## Configuration
VarNoiseDB uses a YAML configuration file to specify database settings. You can provide your own config file or use the built-in default.

### Example Configuration
```yaml
database:
  type: sqlite               # Options: sqlite, postgresql, mysql
  name: VarNoiseDB.db        # Database name
  # Additional fields for PostgreSQL/MySQL:
  # host: localhost
  # port: 5432
  # user: username
  # password: password
```

### Specifying configuration
```yaml
# Using a custom config file
VarNoiseDB --config path/to/your/config.yaml <command>

# Using environment variable
export VARNOISEDB_CONFIG=path/to/your/config.yaml
VarNoiseDB <command>
```

## Usage

### Initializing Tables
Before loading data, initialize the database structure:

```bash
VarNoiseDB init
```

### Loading Data from GVCF files

```bash
VarNoiseDB load --gvcf path/to/your/file.g.vcf
```

### Removing Data for a Sample

```bash
VarNoiseDB remove sample_name [--gvcf path/to/your/file.g.vcf]
```
If the --gvcf option is not provided, the GVCF path will be retrieved from the database to be the same used when loading the sample.

### Exporting Data as VCF

```bash
VarNoiseDB export --output path/to/output.vcf
```
## Database Structure 

VarNoiseDB maintains two tables: variants and samples

### Variants table

| Column                  | Type    | Description                                     |
|-------------------------|---------|-------------------------------------------------|
| chr                     | TEXT    | Chromosome                                      |
| pos                     | INTEGER | Position                                        |
| mean_non_ref_af         | REAL    | Mean non-reference allele frequency             |
| sd_non_ref_af           | REAL    | Standard deviation of non-reference allele frequency |
| max_non_ref_af          | REAL    | Maximum non-reference allele frequency observed |
| min_non_ref_af          | REAL    | Minimum non-reference allele frequency observed |
| total_depth             | REAL    | Cumulative sequencing depth                     |
| number_of_samples       | INTEGER | Number of samples with this variant             |
| max_non_ref_af_sample   | TEXT    | Sample name with maximum non-reference allele frequency |
| min_non_ref_af_sample   | TEXT    | Sample name with minimum non-reference allele frequency |

### Samples table

| Column     | Type      | Description                |
|------------|-----------|----------------------------|
| id         | INTEGER   | Primary key                |
| name       | TEXT      | Sample name                |
| gvcf_path  | TEXT      | Path to the GVCF file      |
| date_added | DATETIME  | Date the sample was added  |

## Examples

### Complete Workflow

```bash
# Initialize the database
VarNoiseDB init

# Load data from multiple GVCF files
VarNoiseDB load --gvcf sample1.g.vcf
VarNoiseDB load --gvcf sample2.g.vcf
VarNoiseDB load --gvcf sample3.g.vcf

# Export the database as VCF
VarNoiseDB export --output exported_variants.vcf

# Remove a sample and its variants
VarNoiseDB remove sample_name --gvcf sample1.g.vcf
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

