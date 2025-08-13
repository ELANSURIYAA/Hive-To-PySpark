# 1. Development and Testing Effort Estimation

## 1.1 Manual Code Refactoring Effort (in hours)
* Transformers / complex derivations: 10
* Joins / Aggregations / Window functions: 12
* Lateral views / explode / nested structs: 4
* UDF/UDAF (authoring & validation): 6
* Control/conditional flows (branching, sequencing): 4
* Helpers/utilities/metadata tracking: 3
* **Subtotal (Refactoring):** 39

## 1.2 Unit & Reconciliation Testing Effort (in hours)
* Test data design & generation: 5
* Row/column checks & rule assertions: 6
* Partition boundary & volume tests: 4
* Performance sanity tests: 3
* **Subtotal (Testing):** 18

**Total Estimated Effort (hours) = Refactoring Subtotal + Testing Subtotal = 57**

---

# 2. Compute Resource Cost 

## 2.1 Spark Runtime Cost (with calculation details and assumptions)
* **Assumptions**
  * Platform: Azure Databricks
  * Cluster: Standard job cluster, 1 driver + 4 workers, Standard_DS3_v2 nodes (4 cores, 14GB RAM each), on-demand
  * Average runtime per execution: 2 hours
  * Runs per day / week / month: 1 / 7 / 30
  * Input data volume per run: 5.5 TB
  * Pricing reference: $0.60 per node-hour (compute only), $18.40 per TB/month (storage)
* **Calculation**

  * Per-run cost: 5 nodes x 2 hours x $0.60 = $6.00
  * Daily cost: $6.00
  * Monthly cost: $6.00 x 30 = $180.00
  * Storage cost: 5.5 TB x $18.40 = $101.20/month
  * **Total monthly (compute + storage): $281.20**
* **Notes**

  * Tuning levers: Delta Lake, partitioning, bucketing, caching, indexing, AQE, broadcast joins
  * Cost risks: Data skew, shuffles, spills, UDF inefficiency, schema inference errors

---

# 3. apiCost

```
apiCost: 0.0025 USD
```
