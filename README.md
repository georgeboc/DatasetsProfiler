# Datasets Profiler
This project is used to profile a dataset. It has two use cases: the first one allows to get a profile of a dataset, 
and the second use case, prepares the dataset to serve as input for the [Testbed project](https://github.com/georgeboc/Testbed).

We have designed a set of integration tests to test against all modifications if there are functional changes in the 
project. 

Ensure there are `parameters` and `inputs` before proceeding.

The `parameters` are files which contain a list of configurations. Each configurations defines whe behaviour of an 
application. When the Datasets Profiler will be executed, it will run through a list of configurations and will execute 
an Application for each configuration.  
The `parameters` file looks as follows:
```json
[
  {
    "use_case": "get_description",
    "input_path": "input/Ad_click_on_taobao/Ad_click_on_taobao.csv",
    "output_path": "output/datasets/Ad_click_on_taobao_sample",
    "parser": "ad_click_on_taobao_log_parser_strategy"
  },
  {
    "use_case": "get_described_dataset",
    "input_path": "input/Ad_click_on_taobao/Ad_click_on_taobao.csv",
    "output_directory": "output/described_datasets/Ad_click_on_taobao_10000",
    "parser": "ad_click_on_taobao_log_parser_strategy",
    "limit": 10000
  },
  {
    "use_case": "get_description",
    "input_path": "input/Android/Android.log",
    "output_path": "output/samples_1000/Android_sample_1000",
    "parser": "android_log_parser_strategy",
    "specific_formatters": [
      "no_year_datetime_specific_formatter",
      "string_specific_formatter",
      "string_specific_formatter",
      "string_specific_formatter",
      "string_specific_formatter",
      "string_specific_formatter"
    ],
    "limit": 1000
  }
]
```

The `inputs` are files whose path is defined in the `parameters` file with the key `input_path` and refer to the raw Datasets.

To run the Dataset Profiler with the integration tests, execute the following command from within the root of 
the project `DatasetsProfiler/`:
```
$ scripts/check_tests.sh
```

You also have the option to run the Datasets Profiler either locally or in a cluster, using Yarn.
- If you want to run the Datasets Profiler locally, execute the following script:
  ```
  $ scripts/run-local.sh PATH_TO_PARAMETERS_FILE
  ```
  Where `PATH_TO_PARAMETERS_FILE` is the path which points to the parameters file.


- If you want to run the Datasets Profiler in a cluster, execute the following script:
  ```
  $ scripts/run-cluster.sh PATH_TO_PARAMETERS_FILE
  ```
  Where `PATH_TO_PARAMETERS_FILE` is the path which points to the parameters file.

An example of a local execution of the Datasets Profiler is shown as follows:
```
$ scripts/run-local.sh parameters/parameters_integration_test
```

[comment]: # (TODO: Add link)
For more information, check the thesis associated to this project.