README.md
## INTRODUCTION

The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:

trains the user to do a STEDI balance exercise
has sensors on the device that collect data to train a machine-learning algorithm to detect steps
has a companion mobile app that collects customer data and interacts with the device sensors
STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.

Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in deciding what data can be used.

Some of the early adopters have agreed to share their data for research purposes. Only these customers’ Step Trainer and accelerometer data should be used in the training data for the machine learning model.


## PROJECT DESCRIPTION
In this project, we are creating a data lakehouse solution that can store three sets of data from Stedi business:

Customer Data: Data coming from fulfillment and the STEDI website.
Accelerometer Data: Data that we receive through the mobile app.
Step Trainer Data: Data coming from the motion sensor of the device.

We are going to process data across three layers/zones: landing, trusted and curated. The solution is built using AWS S3, AWS Glue, Python and Spark. 


## LAYERS/ZONES

For the project we are dividing the data into 3 layers:

### Landing
Here we are basically storing the raw data from customer, accelerometer and step trainer.

### Trusted
Purpose of this layer is to filter out certain data. Here we are using following Glue jobs:

customer_landing_to_trusted.py [https://github.com/erknlr/stedi-human-balance-analytics/blob/main/Glue%20Jobs/customer_landing_to_trusted.py] - Here, we take the raw customer data and basically filter for only customers who agreed to share their data for research purposes. 

accelerometer_landing_to_trusted_zone.py [https://github.com/erknlr/stedi-human-balance-analytics/blob/main/Glue%20Jobs/accelerometer_landing_to_trusted_zone.py] - Here, we are filtering for accelerometer data of the customers who agreed to share their data for research purposes. For this purpose, we join raw accelerometer data to the putput we created above (customer_trusted)

step_trainer_landing_to_trusted.py [https://github.com/erknlr/stedi-human-balance-analytics/blob/main/Glue%20Jobs/step_trainer_landing_to_trusted.py] - This script takes the raw step trainer data and filters for customers who have accelerometer data and have agreed to share their data for research purposes. For this purpose, we use the output customer_curated that we are creating in the curated layer.  

### Curated
Here we are performing further transformations to make data suitable for different purposes (like machine learning). For this purpose, we are using following Glue jobs:

customer_trusted_to_curated.py [https://github.com/erknlr/stedi-human-balance-analytics/blob/main/Glue%20Jobs/customer_trusted_to_curated.py] - Here, we are taking the customer_trusted output and narrow it down to only to those who have some accelerometer readings. We do this by joining customer_trusted output to raw accelerometer data. 

machine_learning_curated.py [https://github.com/erknlr/stedi-human-balance-analytics/blob/main/Glue%20Jobs/machine_learning_curated.py] - Here, we are building an output that combines step trainer data with accelerometer data with the same timestamp for those customers who agreed to share their data for research purposes. 


