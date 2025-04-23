# Nimbus Data Generator

## Installation

```sh
pip install data-gen@git+https://github.com/aegion-dynamic/data-gen
```

## Data Generation

Since creating sample data for new projects is extremely time consuming, we are building this tool to accelerate the development workflow

How to run it:

```sh
python -m data_gen
```


## Data Migration

### Setting up the migration directory

In order to perform the data migration, we use this system to perform all the data transformation. Heres how you need to setup the files for the entire pipeline:

```
(target-dir)
-- schema
    -- nimbus_schema_1.hcl
    -- nimbus_schema_2.hcl
    -- nimbus_schema_3.hcl
-- table_data
   -- <schema_name>_<table_name>.csv
   -- <schema_name>_<table_name>.csv
   -- relationships.json
-- users
    -- users.csv
    -- cognito_config.json
Makefile (Default make file)
```

### Steps

#### Step 0: Install

**Manually** install the following dependencies:

[Atlas CLI](https://atlasgo.github.io/cli/)

```sh
curl -sSf https://atlasgo.sh | sh
```


[DataGen](https://github.com/aegion-dynamic/data-gen)

```sh
pip install data-gen@git+https://github.com/aegion-dynamic/data-gen
```

Download the Makefile from the repository and place it in the target directory.

#### Step 1: Setup the Migration Directory

```sh
make setup-dir
```

This will create the directory structure for the migration. If the target directory isn't passed, it will create the directory in the current working directory.

### Step 2: Create schema structure on local

**Manually** move the `.hcl` files to the `schema` directory.

**Run:**

```sh
make atlas-migrate
```

This will run the `atlas migrate` command with all the schema files in the schema directory.


### Step 3: Generate the users

**Manually** Fillout the `users.csv` and `cognito_config.json` file in the `users` directory. Ensure that the `users.csv` file has the required user infomation (email, first name, last name, etc.).

**Run:**

```sh
make generate-users
```

### Step 4: Generate the data templates

```sh
make generate-data-templates
```

This will generate the data templates for all the tables in the `table_data` directory.

### Step 5: Fill out the relationships

Fillout the `relationships.json` with the corresponding target lookup fields

### Step 6: Generate the data

**Manually:** Update all the data templates in the `table_data` directory. Fill them out with the info, use code if you have to, etc.


### Step 7: Populate the database

**Run:**

```sh
make populate-db
```


