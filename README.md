# CPS-IoT Surveillance system

## Team members
Abdirahman,
Andile,
Daniel,
Keanu,
Thibedi
and Ubayd

# Big Data Engineering Group Assignment
# Setting Up and Running an Application with Docker, Python, and Hadoop

This README file provides step-by-step instructions on how to set up and run the Python application
 using both Docker and a normal Python environment, with an added requirement for setting up 
 a Hadoop cluster for the Big Data Engineering Assignment 2. This guide assumes you have Docker installed on your system,
  a basic understanding of Python, and the capability to set up a Hadoop cluster.

## Table of Contents
- [Clone the Repository](#1-clone-the-repository)
- [Setting Up the Python Application](#2-setting-up-the-python-application)
- [Running the Python Application Locally](#3-running-the-python-application-locally)
- [Building the Docker Image](#4-building-the-docker-image)
- [Running the Dockerized Application](#5-running-the-dockerized-application)
- [Setting Up a Hadoop Cluster](#6-setting-up-a-hadoop-cluster)
- [API Documentation](#7-api-documentation)
- [Datasets](#8-using-the-dataset)

### 1. Clone the Repository
Clone the repository containing the Python application to your local machine using Git 
or by downloading the ZIP file from the source code on GitHub.
```bash
git clone git@github.com:ubayd-bapoo/bde_individual.git
cd bde_individual
```

### 2. Setting Up the Python Application
Lets make sure the Python application works correctly in a normal Python environment. 
This typically involves creating a virtual environment, installing dependencies, and 
testing the application.

```bash
# Create a virtual environment (optional but recommended)
python -m venv venv
source venv/bin/activate  # On Windows, use 'venv\Scripts\activate'

# Install application dependencies
pip install -r requirements.txt

# Test the application
python service.py  # In the service_app folder
```

### 3. Running the Python Application Locally
To run the application in your local Python environment, you can use the following command:
```bash
python service.py  # In the service_app folder
```

### 4. Building the Docker Image
The Dockerfile sets up a basic Python environment and copies the application files into 
the container. It also installs all necessary dependencies from requirements.txt and 
specifies the command to run when the container starts.
To build a Docker image of the Python application, navigate to the project directory 
(where the Dockerfile is located) and run the following command:
```
docker build -t bde-individual .
```

### 5. Running the Dockerized Application
Once the Docker image is built successfully, you can run the Python application within 
a Docker container:
```bash
docker run -p 8000:8000 bde-individual
```

### 6. Setting Up a Hadoop Cluster
Before using this application, you need to set up a Hadoop cluster. This involves configuring 
Hadoop nodes, creating HDFS directories, and ensuring proper network connectivity. 
Please refer to Hadoop documentation or resources specific to your environment to set up the 
cluster. The application may interact with Hadoop components, and a running Hadoop cluster is 
required for its functionality.


### 7. API Documentation
The API documentation is powered by FastAPI's built-in Swagger integration. This interactive documentation makes it 
easy to explore and understand the API endpoints.
- **Access API Documentation**: To explore our API documentation and test the endpoints 
interactively, simply visit [Swagger UI](http://localhost:8000/docs) (http://localhost:8000/docs) when the application
 is running locally.

- **Automatic Documentation**: We've designed our API using FastAPI, which automatically
 generates documentation based on the code, including request and response models and 
 descriptions.

FastAPI's Swagger UI provides a user-friendly way to understand and use our API. You can
 interact with the available endpoints, view request and response models, and even make
  test requests right from the documentation.

Feel free to dive into the documentation to get a better understanding of how to use our
 API effectively.

![FastAPI Swagger UI](https://fastapi.tiangolo.com/img/tutorial/tutorial-02-swagger-ui.png)

Note: In a production environment, replace `http://localhost:8000/docs` with the actual
 URL where your API is hosted.

### 8. Using the Dataset
GitHub Repository: Access the restaurant dataset at this [link](https://github.com/ozlerhakan/mongodb-json-files/blob/master/datasets/restaurant.json
).

**Additional Resources**

- **Aviation Information**: For aviation-related data, check out AviationStack.
- **Weather Data**: Retrieve weather data from the OpenWeatherMap API.
- **Mapping and Routing**: Utilize the TomTom routing API and map generator.
- **GPS Coordinates**: Collect GPS coordinates through web scraping.