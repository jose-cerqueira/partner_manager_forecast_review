# partner_manager_forecast_review

**Descprition:** Allows for Partner Manager to review KPI Forecast, when they know of events.

**Challenge:** As a Partner Manager, I want to be able to submit a review of the value I get from the KPI forecast.

### KPI (Key Performance Indicators):
- NS - Percentage of No Stock - Percentage of ocurrences (in # items) on which an item ordered was cancelled by the company because there was No Stock. Timeframe is Date of Stock Evaluation.
- SOS 2D - Speed of Sending under 2 days - Percentage of ocurrences (in # orders) on which an order was sent in less than or equal to 2 days. Timeframe is Send Date.
- SOS 1D - Speed of Sending under 1 days - Percentage of ocurrences (in # orders) on which an order was sent in less than or equal to 1 days. Timeframe is Send Date.
- WI - Percentage of Wrong Item - Percentage of ocurrences (in # items) on which an item was returned because the wrong item was sent over the total number of items sent. Timeframe is Send Date.

### Inputs:
- KPI Forecast
- Partner Manager Review

### Auxiliar data (Data used in order to guide Partner Manager decision):
- Historic KPI perfomance (A.K.A. Actuals and Previous Year)
- Budgeted KPI targets (A.K.A. Budget)

Later on, after having registered a significant amount of KPI reviews, Hypothesis Testing was used to test the following Hypothesis.

**Null Hypothesis:** Forecast Error cannot be decreased by Reviewing it.

**Alternate Hypothesis:** Forecast Error can be decreased by Reviewing it.

### Project Overview:
1. Input and Auxiliar Data Pipeline - Design a data pipeline collecting all the data needed and presenting it in a manner which is easy for the Partner Manager to understand.
2. Interface - Build an interface which allows for the Partner Manager to visualize the collected data and submit feedback.
3. Review Collection Pipeline - Design a data pipeline that collects this input on a monthly basis.
4. Process Monitoring Data Pipeline and Looker Dashboard - Allowed to monitor the overall process.
5. Perform Hypothesis Testing on the collected data.

### Process Data Map Overview
![image](https://user-images.githubusercontent.com/101900632/208505152-6bf1b9c3-af52-479f-b9fb-201b960d86eb.png)

### To take a look at the queries used on steps 1-4 as well as Python code used for step 5, navigate through the files on the repository.

### Interface Overview
![image](https://user-images.githubusercontent.com/101900632/208507205-84c2fe58-64e0-4493-9656-c92af0eef1db.png)

### Looker Dashboard Overview
![Screenshot (711)](https://user-images.githubusercontent.com/101900632/208508135-f233758d-3c10-4cf4-81c6-612d4e92ce69.png)
