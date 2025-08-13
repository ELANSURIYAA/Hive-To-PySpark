# 1. Development and Testing Effort Estimation

## 1.1 Manual Code Refactoring Effort (in hours)
* Transformers / complex derivations: 12
* Joins / Aggregations / Window functions: 18
* Lateral views / explode / nested structs: 6
* UDF/UDAF (authoring & validation): 8
* Control/conditional flows (branching, sequencing): 6
* Helpers/utilities/metadata tracking: 4
* **Subtotal (Refactoring):** 54

## 1.2 Unit & Reconciliation Testing Effort (in hours)
* Test data design & generation: 6
* Row/column checks & rule assertions: 8
* Partition boundary & volume tests: 6
* Performance sanity tests: 4
* **Subtotal (Testing):** 24

**Total Estimated Effort (hours) = Refactoring Subtotal + Testing Subtotal = 78**

---

# 2. Compute Resource Cost 

## 2.1 Spark Runtime Cost (with calculation details and assumptions)
* **Assumptions**
  * Platform: Azure Databricks
  * Cluster: Standard job cluster, 1 driver + 4 workers (5 nodes total), Standard_DS3_v2 (4 cores, 14GB RAM per node)
  * Average runtime per execution: 2 hours
  * Runs per day / week / month: 1 / 7 / 30
  * Input data volume per run: 5.5 TB
  * Pricing reference: $0.60 per hour per node (compute), $18.40 per TB/month (storage)
* **Calculation**

  * Per-run compute cost: 5 nodes x 2 hours x $0.60 = $6.00
  * Daily cost: $6.00
  * Monthly cost: $6.00 x 30 = $180.00
  * Storage cost: 5.5 TB x $18.40 = $101.20/month
  * **Total monthly cost (compute + storage): $180.00 + $101.20 = $281.20**
* **Notes**

  * Tuning levers: Delta Lake, partitioning, bucketing, caching, indexing, AQE, broadcast joins
  * Cost risks: Data skew, large shuffles, spills, suboptimal partitioning, UDF performance

---

# 3. apiCost

```
apiCost: 0.0042 USD
```
