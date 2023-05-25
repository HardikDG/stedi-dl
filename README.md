# STEDI Human Balance Analytics

This analysis is done on the 3 types of data which are available in the JSON format under S3 buckets. 
- Customer
- Accelerometer
- Step_trainer

- As a general process for all 3 data is loaded in the landing zone first, based on the filter and updates it is moved to the trusted zone. 
For analysis based on the requirements data is stored in the curated zones


### **Glue table (DDL)**
The code for the creation of the Glue tables can be found in the following scripts:
- [customer_landing.sql](./DDL_Tables/landing/customer_landing.sql)
- [accelerometer_landing.sql](./DDL_Tables/landing/accelerometer_landing.sql)

### **Athena query results**

The customer and accelerometer landing tables were queried in Athena and the results are stored inside the DDL_Tables folder.
- Landing folder consists of queries fired on the landing tables, while trusted folder contains the queries fired on the trusted tables 

<br>

## **Trusted Zone**
---

In the Trusted Zone were stored the tables that contain the records from *customers who agreed to share their data* for research purposes.

### **Glue  scripts**
The scripts below were downloaded from the Glue studio. They were used to transform the landing tables into trusted tables.

- [customer_landing_to_trusted.py](./glue_scripts/customer_landing_to_trusted.py): script used to build the **customer_trusted** table, which contains customer records from customers who agreed to share their data for research purposes.

- [accelerometer_landing_to_trusted_zone.py](./glue_scripts/accelerometer_landing_to_trusted.py): script used to build the **accelerometer_trusted** table, which contains accelerometer records from customers who agreed to share their data for research purposes.

- [step_trainer_landing_to_trusted.py](./glue_scripts/step_trainer_landing_to_trusted.py): script used to build the **step_trainer_trusted** table,which contains the data of Step Trainer Records data for customers who have accelerometer data and have agreed to share their data for research
<br>


## **Curated Zone**
---
In the Curated Zone were stored the tables that contain the *correct serial numbers*.

### Glue job scripts
- [customer_trusted_to_curated.py](./glue_scripts/customer_trusted_to_curated.py): script used to build the **customer_curated** table, which contains customers who have accelerometer data and have agreed to share their data for research.

- [trainer_trusted_to_curated.py](./glue_scripts/trainer_trusted_to_curated.py): script used to build the **machine_learning_curated** table, which contains each of the step trainer readings, and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data.

