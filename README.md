Dimensional modelling and ETL data processing

1. Software and tools
To complete the assignment, students will have to use
• MySQL and MySQL Workbench. 
• A Python (3.x) installation. You may use libraries and an IDE of your choice.

2. Data sources
The goal of this case is to implement a dimensional model to support the analysis of F1 results. The main data source is a Kaggle dataset, .csv files are already in the /data folder

3. Tasks
3.1 Data analysis and modelling
The objective is to model the performance of the drivers during the qualification sessions and  the races, i.e. the pit stops and the race final positions. Therefore, you should design and implement three star schemas. 
In the documentation, for each model, describe the analytical goals and justify each dimensional model accordingly. Your submission should include the diagrams of the models and the database scripts to create the schemas in the DBMS of your choice.

3.2 Data pipeline
Design and implement the data engineering pipeline that populate the data model. 

Deliverables from this task are:
- A high-level description of the pipelines, explaining the steps to read the source data,
clean and transform the data and load the final data into the corresponding schema. (in the /reports folder)
- (Optional) Any description of the source or intermediate data, statistics or any other
results that justify your design choices.
- The Python source code that implements the pipeline.
- A clear and concise description of how to run the whole pipeline. (in the /reports folder)


Keep in mind that the source code you provide must be execution-ready on the professor’s
machine. This means that:
- A small set of configuration parameters may be included (preferably in environment or
configuration files). This includes the database connection parameters, and possibly the
(relative path) location of the input files on the local machine. These configuration steps
must be clearly documented in your report.
- The source code must not contain any absolute local file paths or other elements that
prevent it from running on a different machine.
- In summary: make sure that your code can be executed on any other machine following
your submitted instructions.


