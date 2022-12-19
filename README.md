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
- Historic KPI perfomance (A.K.A. Actuals)
- Budgeted KPI targets (A.K.A. Budget)

Later on, after having registered a significant amount of KPI reviews, Hypothesis Testing was used to test the following Hypothesis.
Null Hypothesis: Forecast Error cannot be decreased by Reviewing it.
Alternate Hypothesis: Forecast Error can be decreased by Reviewing it.

### Project Overview:
1. Design a data pipeline collecting all the data needed and presenting it in a manner which is easy for the Partner Manager to understand.
2. Build an interface which allows for the Partner Manager to visualize the collected data and submit feedback.
3. Design a data pipeline that collects this input on a monthly basis.
4. Perform Hypothesis Testing on the collected data.

### 1. Input and Auxiliar Data Pipeline
