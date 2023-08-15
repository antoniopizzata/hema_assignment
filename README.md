# Overview
This file will hold all the information relative to the deployment of the here available code.

# File description
The repo is made up of the following files:
- etl_process.py: main python script with all the functionalities and requirements.
- helper.py: library file to support the etl_process.py script with library functions and logger.
- Dockerfile: file holding all the steps to create the docker image executing the script. 
- kaggle.json: file holding the api key for the kaggle api. Please insert your information in the file before building the docker image.
- requirements.txt: file containing all the python packages for the project.
- documentation.md: documentation of the code developed.
- databricks.json: example of a deployment file for the DataBricks job.

# Deployment guide
Follow these steps to deploy the module:
- Insert your information in the kaggle.json file. Guide to generate the API key can be found [here](https://github.com/Kaggle/kaggle-api#api-credentials)
- Run the command `docker build -t hema_assignment .`.
- Run the command `docker run -it hema_assignment /bin/bash` to access the docker image shell.
- Run the command `python3 etl_process.py` to execute the pipeline. 