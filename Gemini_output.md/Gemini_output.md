# Informatica Pipeline Metadata Documentation

## Component Metadata Table

| Component Name       | Component Type | Input Ports                               | Output Ports                              | Expressions/Logic            | Dependencies/Connections              | Additional Properties                          |
|--------------------- |---------------|-------------------------------------------|-------------------------------------------|------------------------------|----------------------------------------|------------------------------------------------|
| Source_Orders        | Source        | order_id, customer_id, order_date, amount | order_id, customer_id, order_date, amount | N/A                          | Reads from Orders table                | Source type: Relational, Table: Orders         |
| Expression_1         | Expression    | order_id, customer_id, order_date, amount | order_id, customer_id, order_year, amount | order_year = YEAR(order_date)| Input: Source_Orders, Output: Filter_RecentOrders | N/A                                    |
| Filter_RecentOrders  | Filter        | order_id, customer_id, order_year, amount | order_id, customer_id, order_year, amount | order_year >= 2022           | Input: Expression_1, Output: Target_RecentOrders | N/A                                    |
| Target_RecentOrders  | Target        | order_id, customer_id, order_year, amount | N/A                                      | N/A                          | Input: Filter_RecentOrders             | Target type: Relational, Table: RecentOrders   |

## Notes

- **Expression/Logic Details:**
    - **Expression_1:**
        - `order_year = YEAR(order_date)`
    - **Filter_RecentOrders:**
        - Filter condition: `order_year >= 2022`
- **Dependencies/Connections:**
    - **Source_Orders** feeds into **Expression_1**.
    - **Expression_1** feeds into **Filter_RecentOrders**.
    - **Filter_RecentOrders** feeds into **Target_RecentOrders**.
- **Additional Properties:**
    - **Source_Orders:** Source type is Relational, reading from the 'Orders' table.
    - **Target_RecentOrders:** Target type is Relational, writing to the 'RecentOrders' table.

All expressions are short and fully included in the table above. No truncation was necessary.

---

This documentation captures all Informatica mapping components, their ports, logic, dependencies, and relevant properties as extracted from '@3.json'.
