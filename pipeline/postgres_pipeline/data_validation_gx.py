"""
Standard Great Expectations Workflow


Datasource
    ↓
Data Asset
    ↓
Batch Definition
    ↓
Expectation Suite
    ↓
Validation Definition
    ↓
Checkpoint
    ↓
Validation Result
    ↓
Data Docs


This follows the intended Great Expectations architecture for version **1.19.0**, where Expectation Suites define the validation rules, Validation Definitions bind those rules to data, Checkpoints execute the validations, and Data Docs provide human-readable validation reports.

"""

from __future__ import annotations

import logging
from pathlib import Path
import pandas as pd

import great_expectations as gx


logging.getLogger("great_expectations").setLevel(logging.WARNING) #it can be WARNING or ERROR or CRITICAL
logging.getLogger("great_expectations").propagate = False

logger = logging.getLogger(__name__)


class DataQualityValidator:

    PROJECT_DIR = Path(__file__).resolve().parent / "great_expectations"

    DATASOURCE_NAME = "shopsphere_datasource"

    def __init__(self) -> None:

        # Create or open the GX project
        self.context = gx.get_context(
            project_root_dir=self.PROJECT_DIR
        )

        logger.info(
            "Great Expectations project: %s",
            self.PROJECT_DIR,
        )

        self.datasource = self._get_or_create_datasource()

    # ---------------------------------------------------------
    # Datasource
    # ---------------------------------------------------------
    def _get_or_create_datasource(self):
        """
        Reuse the datasource if it already exists.
        """

        try:
            return self.context.data_sources.get(
                self.DATASOURCE_NAME
            )

        except Exception as e: # noqa: BLE001

            logger.info(
                "Creating GX datasource '%s'",
                self.DATASOURCE_NAME,
            )

            return self.context.data_sources.add_pandas(
                self.DATASOURCE_NAME
            )

    # ---------------------------------------------------------
    # Data Asset
    # ---------------------------------------------------------

    def _get_or_create_asset(self, dataset_name: str):
    
        try:
            return self.datasource.get_asset(dataset_name)

        except Exception: # noqa: BLE001
            return self.datasource.add_dataframe_asset(
                dataset_name
            )

    # ---------------------------------------------------------
    # Batch Definition
    # ---------------------------------------------------------

    def _get_or_create_batch_definition(self, dataset_name):
        asset = self._get_or_create_asset(dataset_name)
        batch_name = f"{dataset_name}_batch"

        try:
            return asset.get_batch_definition(batch_name)

        except Exception: # noqa: BLE001
            return asset.add_batch_definition_whole_dataframe(
                batch_name
            )
    

    # ---------------------------------------------------------
    # Batch
    # ---------------------------------------------------------

    def get_batch(
        self,
        dataset_name: str,
        dataframe: pd.DataFrame,
    ):

        batch_definition = self._get_or_create_batch_definition(
            dataset_name
        )

        return batch_definition.get_batch(
            batch_parameters={
                "dataframe": dataframe
            }
        )

    # ---------------------------------------------------------
    # Expectation Suite
    # ---------------------------------------------------------

    def _get_or_create_expectation_suite(
        self,
        dataset_name: str,
    ):
        try:
            return self.context.suites.get(dataset_name)
        except Exception as e: # noqa: BLE001
            logger.info(
                "Creating Expectation Suite '%s'",
                dataset_name,
            )
            return self.context.suites.add(
                gx.ExpectationSuite(name=dataset_name)
            )

    # ---------------------------------------------------------
    # Validation Definition
    # ---------------------------------------------------------

    def _get_or_create_validation_definition(
        self,
        dataset_name: str,
    ):
        validation_name = f"{dataset_name}_validation"
        batch_definition = self._get_or_create_batch_definition(
            dataset_name
        )

        suite = self._get_or_create_expectation_suite(
            dataset_name
        )

        try:
            return self.context.validation_definitions.get(
                validation_name
            )

        except Exception as e: # noqa: BLE001
            logger.info(
                "Creating Validation Definition '%s'",
                validation_name,
            )

            return self.context.validation_definitions.add(
                gx.ValidationDefinition(
                    name=validation_name,
                    data=batch_definition,
                    suite=suite,
                )
            )

    def _get_or_create_checkpoint(
        self,
        dataset_name: str,
    ):
        checkpoint_name = f"{dataset_name}_checkpoint"

        validation_definition = (
            self._get_or_create_validation_definition(
                dataset_name
            )
        )

        try:
            return self.context.checkpoints.get(
                checkpoint_name
            )

        except Exception as e: # noqa: BLE001
            logger.info(
                "Creating Checkpoint '%s'",
                checkpoint_name,
            )

            return self.context.checkpoints.add(
                gx.Checkpoint(
                    name=checkpoint_name,
                    validation_definitions=[
                        validation_definition,
                    ],
                )
            )
        
    # ---------------------------------------------------------
    # Validator
    # ---------------------------------------------------------

    def get_validator(
        self,
        dataset_name: str,
        dataframe: pd.DataFrame,
    ):

        batch = self.get_batch(
            dataset_name,
            dataframe,
        )

        suite = self._get_or_create_expectation_suite(
            dataset_name
        )

        return self.context.get_validator(
            batch=batch,
            expectation_suite=suite,
        )

    def validate_dataset(
        self,
        dataset_name: str,
        dataframe: pd.DataFrame,
    ):

        # validation_definition = (
        #     self._get_or_create_validation_definition(
        #         dataset_name
        #     )
        # )

        # results = validation_definition.run(
        #     batch_parameters={
        #         "dataframe": dataframe,
        #     }
        # )

        checkpoint = self._get_or_create_checkpoint(
            dataset_name
        )

        results = checkpoint.run(
            batch_parameters={
                "dataframe": dataframe,
            }
        )

        self.context.build_data_docs()

        validation_result = next(iter(results.run_results.values()))
        logger.info(validation_result.statistics)
        

        if results.success:
            logger.info(
                "✓ %s passed validation.",
                dataset_name,
            )
        else:
           
            logger.error(
                "%s validation failed.",
                dataset_name
            )
            
            for result in validation_result.results:
                if not result.success:
                    logger.error(
                        "FAILED: %s | %s",
                        result.expectation_config.type,
                        result.expectation_config.kwargs,
                    )
            
            
        return results.success
    



"""
Great Expectations 1.19.0 Standard Implementation Workflow

1. Create a Data Quality module
Create a reusable `data_quality.py` (or similar) that contains a `DataQualityValidator` class.
The class should automatically:
* Create or open the Great Expectations project.
* Create or get the datasource.
* Create or get the data asset.
* Create or get the batch definition.
* Create or get the Expectation Suite.

The class should expose a `get_validator()` method that automatically create those 5 things when ran.

---

2. Initialize Great Expectations in the pipeline
After transforming the dataframe, initialize the validator:
validator = DataQualityValidator() 
Then obtain a validator for the dataset:

gx_validator = validator.get_validator(
    dataset_name="customer_sessions",
    dataframe=df,
)

On the **first pipeline run**, Great Expectations will automatically create (if missing) once 'validator' runs
to initialize the class:
* GX project
* Datasource
* Asset
* Batch Definition
* Expectation Suite

On later runs, these resources are simply reused.
running gx_validator will only create:
great_expectations/
└── expectations/
    └── customer_sessions.json


3. Populate the Expectation Suite (one-time setup)

The file:
great_expectations/
└── expectations/
    └── customer_sessions.json

becomes the **source of truth** for the dataset.

Do **not** edit this JSON manually. Initially, the suite is empty.
Populate it by adding expectations programmatically:

gx_validator.expect_column_to_exist(...)
gx_validator.expect_column_values_to_not_be_null(...)

After all expectations have been added to the transform.py, save the suite:

validator.context.suites.add_or_update(
    gx_validator.expectation_suite
)

This writes all expectations into:
great_expectations/expectations/customer_sessions.json

---

4. Remove expectation-building code
Once the Expectation Suite has been successfully created and saved:

* Remove all `gx_validator.expect_*()` calls from `transform.py`.
* Do **not** recreate expectations during every pipeline run.

From this point onward:

* The Expectation Suite is maintained by Great Expectations.
* `transform.py` should only execute validations.


5. Create Validation Definitions
Once the suite exists, create or get a Validation Definition.
A Validation Definition connects:

* Batch Definition
* Expectation Suite

Example:

```text
customer_sessions_batch
        +
customer_sessions
        ↓
customer_sessions_validation
```

Validation Definitions become the standard way of validating datasets.

---

6. Create Checkpoints
Create or get a Checkpoint.
A Checkpoint executes one or more Validation Definitions.

Example:
```text
customer_sessions_validation
        ↓
customer_sessions_checkpoint
```

Instead of calling:

```python
validator.validate()
```

run the Checkpoint.

---

7. Generate Data Docs
After the Checkpoint completes successfully, build Data Docs:
self.context.build_data_docs()

This automatically generates HTML validation reports showing:

* Validation history
* Passed expectations
* Failed expectations
* Unexpected values
* Validation statistics

---

# Great Expectations Implementation Order

Implement Great Expectations in this order:

* ✅ Create or get Data Context (GX project)
* ✅ Create or get Datasource
* ✅ Create or get Data Asset
* ✅ Create or get Batch Definition
* ✅ Create or get Expectation Suite
* ✅ Populate and save the Expectation Suite
* ✅ Create or get Validation Definition
* ✅ Create or get Checkpoint
* ✅ Execute Checkpoint
* ✅ Build Data Docs

---

# Standard Great Expectations Workflow

```text
Datasource
    ↓
Data Asset
    ↓
Batch Definition
    ↓
Expectation Suite
    ↓
Validation Definition
    ↓
Checkpoint
    ↓
Validation Result
    ↓
Data Docs
```

This follows the intended Great Expectations architecture for version **1.19.0**, where Expectation Suites define the validation rules, Validation Definitions bind those rules to data, Checkpoints execute the validations, and Data Docs provide human-readable validation reports.

"""